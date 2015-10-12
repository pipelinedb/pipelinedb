/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_target.h"
#include "pgstat.h"
#include "pipeline/stream.h"
#include "pipeline/streamReceiver.h"
#include "storage/shm_alloc.h"
#include "storage/ipc.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "pipeline/cont_adhoc_mgr.h"

#define SLEEP_MS 2

#define StreamBatchAllAcked(batch) ((batch)->num_wacks >= (batch)->num_wtups && (batch)->num_cacks >= (batch)->num_ctups)

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets = NULL;

static HTAB *prepared_stream_inserts = NULL;
static bool *cont_queries_active = NULL;

static InsertStmt *extended_stream_insert = NULL;

int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread) = NULL;
void *copy_iter_arg = NULL;

/*
 * get_desc
 *
 * Build a tuple descriptor from a query's target list
 */
static TupleDesc
get_desc(List *colnames, Query *q)
{
	ListCell *lc;
	TupleDesc desc = CreateTemplateTupleDesc(list_length(q->targetList), false);
	AttrNumber attno = 1;
	AttrNumber count = 0;

	foreach(lc, q->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (!te->resjunk)
			count++;
	}

	if (!AttributeNumberIsValid(count))
		elog(ERROR, "could not determine tuple descriptor from target list");

	desc = CreateTemplateTupleDesc(count, false);
	foreach(lc, q->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (te->resjunk)
			continue;

		TupleDescInitEntry(desc, attno,
						   strVal(list_nth(colnames, attno - 1)),
						   exprType((Node *) te->expr),
						   -1,
						   0);
		TupleDescInitEntryCollation(desc,
				attno, exprCollation((Node *) te->expr));
		attno++;
	}

	return desc;
}

/*
 * StorePreparedStreamInsert
 *
 * Create a PreparedStreamInsertStmt with the given column names
 */
PreparedStreamInsertStmt *
StorePreparedStreamInsert(const char *name, InsertStmt *insert)
{
	PreparedStreamInsertStmt *result;
	bool found;
	ListCell *lc;
	int i = 0;

	if (prepared_stream_inserts == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(PreparedStreamInsertStmt);

		prepared_stream_inserts = hash_create("Prepared Stream Inserts", 2, &ctl, HASH_ELEM);
	}

	result = (PreparedStreamInsertStmt *)
			hash_search(prepared_stream_inserts, (void *) name, HASH_ENTER, &found);

	result->inserts = NIL;
	result->relid = RangeVarGetRelid(insert->relation, AccessShareLock, false);
	result->desc = CreateTemplateTupleDesc(list_length(insert->cols), false);

	foreach(lc, insert->cols)
	{
		ResTarget *rt;

		Assert(IsA(lfirst(lc), ResTarget));
		rt = (ResTarget *) lfirst(lc);

		Assert(rt->name);
		namestrcpy(&(result->desc->attrs[i++]->attname), rt->name);
	}

	return result;
}

/*
 * SetExtendedStreamInsert
 *
 * Set a stream insert that was added via PARSE for later reading by BIND/EXECUTE
 */
void
SetExtendedStreamInsert(Node *ins)
{
	Assert(IsA(ins, InsertStmt));

	extended_stream_insert = (InsertStmt *) ins;
}

/*
 * GetExtendedStreamInsert
 *
 * Retrieve the stream insert that was added via PARSE
 */
InsertStmt *
GetExtendedStreamInsert(void)
{
	Assert(IsA(extended_stream_insert, InsertStmt));

	return (InsertStmt *) extended_stream_insert;
}

/*
 * HaveExtendedStreamInserts
 */
bool
HaveExtendedStreamInsert(void)
{
	return (extended_stream_insert != NULL);
}

/*
 * AddPreparedStreamInsert
 *
 * Add a Datum tuple to the PreparedStreamInsertStmt
 */
void
AddPreparedStreamInsert(PreparedStreamInsertStmt *stmt, ParamListInfoData *params)
{
	stmt->inserts = lappend(stmt->inserts, params);
}

/*
 * FetchPreparedStreamInsert
 *
 * Retrieve the given PreparedStreamInsertStmt, or NULL of it doesn't exist
 */
PreparedStreamInsertStmt *
FetchPreparedStreamInsert(const char *name)
{
	if (prepared_stream_inserts == NULL)
		return NULL;

	return (PreparedStreamInsertStmt *)
			hash_search(prepared_stream_inserts, (void *) name, HASH_FIND, NULL);
}

/*
 * DropPreparedStreamInsert
 *
 * Remove a PreparedStreamInsertStmt
 */
void
DropPreparedStreamInsert(const char *name)
{
	if (prepared_stream_inserts)
		hash_search(prepared_stream_inserts, (void *) name, HASH_REMOVE, NULL);
}

/*
 * InsertIntoStreamPrepared
 *
 * Send Datum-encoded events to the given stream
 */
int
InsertIntoStreamPrepared(PreparedStreamInsertStmt *pstmt)
{
	ListCell *lc;
	int count = 0;
	int adhoc_count = 0;

	Bitmapset *all_targets = GetLocalStreamReaders(pstmt->relid);
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();

	Bitmapset *targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = bms_difference(all_targets, targets);

	InsertBatchAck adhoc_acks[16];
	int* active_flags[16];
	int active_cqs[16];
	int adhoc_counts[16];

	int num_worker = bms_num_members(targets);
	int num_adhoc = bms_num_members(adhoc_targets);

	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;
	Size size = 0;

	/*
	 * If it's a typed stream we can get here because technically the relation does exist.
	 * However, we don't want to silently accept data that isn't being read by anything.
	 */
	if (targets == NULL && adhoc_targets == NULL)
	{
		char *name = get_rel_name(pstmt->relid);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", name),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.", name)));
	}

	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			batch = InsertBatchCreate();
			num_batches = 1;

			acks[0].batch_id = batch->id;
			acks[0].batch = batch;
			acks[0].count = 1;
		}

		if (num_adhoc)
		{
			InsertBatch* adhoc_batch = 0;

			int i = 0;
			int target = 0;

			Bitmapset *tmp_targets = bms_copy(adhoc_targets);

			while ((target = bms_first_member(tmp_targets)) >= 0)
			{
				adhoc_batch = InsertBatchCreate();

				adhoc_acks[i].batch_id = adhoc_batch->id;
				adhoc_acks[i].batch = adhoc_batch;
				adhoc_acks[i].count = 1;
				adhoc_counts[i] = 0;

				active_flags[i] = AdhocMgrGetActiveFlag(target);
				active_cqs[i] = target;

				i++;
			}
		}
	}

	Assert(pstmt->desc);

	foreach(lc, pstmt->inserts)
	{
		int i;
		ParamListInfoData *params = (ParamListInfoData *) lfirst(lc);
		Datum *values = palloc0(params->numParams * sizeof(Datum));
		bool *nulls = palloc0(params->numParams * sizeof(bool));
		StreamTuple *tuple;

		if (pstmt->desc->natts != params->numParams)
			elog(ERROR, "expected %d prepared parameters but received %d", pstmt->desc->natts, params->numParams);

		for (i=0; i<params->numParams; i++)
		{
			TypeCacheEntry *type = lookup_type_cache(params->params[i].ptype, 0);
			/*
			 * The incoming param may have a different but cast-compatible type with
			 * the target, so we change the TupleDesc before it gets physically packed
			 * in with the event. Eventually it will be casted to the correct type.
			 */
			pstmt->desc->attrs[i]->atttypid = params->params[i].ptype;
			pstmt->desc->attrs[i]->attbyval = type->typbyval;
			pstmt->desc->attrs[i]->attalign = type->typalign;
			pstmt->desc->attrs[i]->attlen = type->typlen;

			values[i] = params->params[i].value;
			nulls[i] = params->params[i].isnull;
		}

		if (num_worker)
		{
			tuple = MakeStreamTuple(heap_form_tuple(pstmt->desc, values, nulls), pstmt->desc, num_batches, acks);
			TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

			count++;
			size += tuple->heaptup->t_len + HEAPTUPLESIZE;
		}

		if (num_adhoc)
		{
			Bitmapset *tmp_targets = bms_copy(adhoc_targets);
			int target = 0;
			int ai = 0;

			while ((target = bms_first_member(tmp_targets)) >= 0)
			{
				int tmp_count = 0;
				Size tmp_bytes = 0;
				InsertBatchAck *ack = 0;
				StreamTuple *tuple = 0;
				Bitmapset *targets = 0;
				Bitmapset *single = bms_make_singleton(target);
				
				ack = &adhoc_acks[ai];
				tuple = MakeStreamTuple(heap_form_tuple(pstmt->desc, values, nulls), pstmt->desc, 1, ack);
				tuple->group_hash = target;

				if (TupleBufferInsert(AdhocTupleBuffer, tuple, single))
				{
					adhoc_counts[ai]++;
				}

				ai++;
			}
		}
	}

	pstmt->inserts = NIL;

	stream_stat_report(pstmt->relid, count, 1, size);

	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			InsertBatchWaitAndRemove(batch, count);
		}

		if (num_adhoc)
		{
			int i = 0;

			for (i = 0; i < num_adhoc; ++i)
			{
				InsertBatchWaitAndRemoveActive(adhoc_acks[i].batch, 
											   adhoc_counts[i],
											   active_flags[i],
											   active_cqs[i]);
			}
		}
	}

	return count;
}

/*
 * InsertIntoStream
 *
 * Send INSERT-encoded events to the given stream
 */
int
InsertIntoStream(InsertStmt *ins, List *params)
{
	int numcols = list_length(ins->cols);
	int i;
	int count = 0;
	List *colnames = NIL;
	TupleDesc desc = NULL;
	Oid relid = RangeVarGetRelid(ins->relation, NoLock, false);
	Bitmapset *all_targets = GetLocalStreamReaders(relid);
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();

	Bitmapset *targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = bms_difference(all_targets, targets);

	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;

	InsertBatchAck adhoc_acks[16];
	int* active_flags[16];
	int active_cqs[16];

	int num_worker = bms_num_members(targets);
	int num_adhoc = bms_num_members(adhoc_targets);

	// actually need the group id.
	// TupleBufferInsert

	Size size = 0;
	List *queries;
	List *plans;
	PlannedStmt *pstmt;
	DestReceiver *receiver;
	Portal portal;
	Query *query;
	SelectStmt *stmt;

	Assert(IsA(ins->selectStmt, SelectStmt));
	stmt = ((SelectStmt *) ins->selectStmt);

	/*
	 * If it's a typed stream we can get here because technically the relation does exist.
	 * However, we don't want to silently accept data that isn't being read by anything.
	 */
	if (targets == NULL && adhoc_targets == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", ins->relation->relname),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.", ins->relation->relname)));


	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			batch = InsertBatchCreate();
			num_batches = 1;

			acks[0].batch_id = batch->id;
			acks[0].batch = batch;
			acks[0].count = 1;
		}

		if (num_adhoc)
		{
			InsertBatch* adhoc_batch = 0;

			int i = 0;
			int target = 0;

			Bitmapset *tmp_targets = bms_copy(adhoc_targets);

			while ((target = bms_first_member(tmp_targets)) >= 0)
			{
				adhoc_batch = InsertBatchCreate();

				adhoc_acks[i].batch_id = adhoc_batch->id;
				adhoc_acks[i].batch = adhoc_batch;
				adhoc_acks[i].count = 1;

				active_flags[i] = AdhocMgrGetActiveFlag(target);
				active_cqs[i] = target;

				i++;
			}
		}
	}

	if (!numcols)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("stream inserts require a row descriptor"),
				 errhint("For example, INSERT INTO %s (x, y, z) VALUES (0, 1, 2)", ins->relation->relname)));

	/* build header of column names */
	for (i = 0; i < numcols; i++)
	{
		ListCell *rtc;
		ResTarget *res = (ResTarget *) list_nth(ins->cols, i);
		int count = 0;

		/* verify that each column only appears once */
		foreach(rtc, ins->cols)
		{
			ResTarget *r = (ResTarget *) lfirst(rtc);
			if (pg_strcasecmp(r->name, res->name) == 0)
				count++;
		}

		if (count > 1)
			elog(ERROR, "column \"%s\" appears more than once in columns list", res->name);

		colnames = lappend(colnames, makeString(res->name));
	}

	/*
	 * If we're doing a prepared insert on this path, then params is just an eval'd tuple of
	 * values and the simplest thing to do is to just use it as the actual values list.
	 * InsertIntoStreamPrepared handles more complex prepared inserts.
	 */
	if (params)
		stmt->valuesLists = params;

	/*
	 * Plan and execute the query being used to generate rows (usually this will be a VALUES list)
	 */
	queries = pg_analyze_and_rewrite(ins->selectStmt, "INSERT", NULL, 0);
	Assert(list_length(queries) == 1);
	query = linitial(queries);

	plans = pg_plan_queries(queries, 0, NULL);
	Assert(list_length(plans) == 1);

	pstmt = linitial(plans);

	receiver = CreateDestReceiver(DestStream);
	desc = get_desc(colnames, query);
	SetStreamDestReceiverParams(receiver, targets, adhoc_targets, desc, num_batches, acks, num_adhoc, adhoc_acks);

	portal = CreatePortal("__insert__", true, true);
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  ins->relation->relname,
					  "INSERT",
					  list_make1(pstmt),
					  NULL);

	PortalStart(portal, NULL, 0, InvalidSnapshot);

	(void) PortalRun(portal,
					 FETCH_ALL,
					 true,
					 receiver,
					 receiver,
					 NULL);

	count = ((StreamReceiver *) receiver)->count;
	size = ((StreamReceiver *) receiver)->bytes;
	(*receiver->rDestroy) (receiver);

	PortalDrop(portal, false);

	stream_stat_report(relid, count, 1, size);

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			InsertBatchWaitAndRemove(batch, count);
		}

		if (num_adhoc)
		{
			int i = 0;

			for (i = 0; i < num_adhoc; ++i)
			{
				InsertBatchWaitAndRemoveActive(adhoc_acks[i].batch, 
											   count,
											   active_flags[i],
											   active_cqs[i]);
			}
		}
	}

	return count;
}

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
uint64
CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	uint64 count = 0;
	int i;
	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;
	Size size = 0;
	bool snap = ActiveSnapshotSet();

	Bitmapset *all_targets = GetLocalStreamReaders(RelationGetRelid(stream));
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();

	Bitmapset *targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = bms_difference(all_targets, targets);

	InsertBatchAck adhoc_acks[16];
	int* active_flags[16];
	int active_cqs[16];
	int adhoc_counts[16];

	int num_worker = bms_num_members(targets);
	int num_adhoc = bms_num_members(adhoc_targets);

	/*
	 * If it's a typed stream we can get here because technically the relation does exist.
	 * However, we don't want to silently accept data that isn't being read by anything.
	 */
	if (!IsInferredStream(RelationGetRelid(stream)) && (num_worker + num_adhoc == 0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", RelationGetRelationName(stream)),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.",
						 RelationGetRelationName(stream))));

	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			batch = InsertBatchCreate();
			num_batches = 1;

			acks[0].batch_id = batch->id;
			acks[0].batch = batch;
			acks[0].count = 1;
		}

		if (num_adhoc)
		{
			InsertBatch* adhoc_batch = 0;

			int i = 0;
			int target = 0;

			Bitmapset *tmp_targets = bms_copy(adhoc_targets);

			while ((target = bms_first_member(tmp_targets)) >= 0)
			{
				adhoc_batch = InsertBatchCreate();

				adhoc_acks[i].batch_id = adhoc_batch->id;
				adhoc_acks[i].batch = adhoc_batch;
				adhoc_acks[i].count = 1;
				adhoc_counts[i] = 0;

				active_flags[i] = AdhocMgrGetActiveFlag(target);
				active_cqs[i] = target;

				i++;
			}
		}
	}

	if (snap)
		PopActiveSnapshot();

	for (i=0; i<ntuples; i++)
	{
		HeapTuple htup = tuples[i];
		StreamTuple *tuple;

		if (num_worker)
		{
			tuple = MakeStreamTuple(htup, desc, num_batches, acks);
			TupleBufferInsert(WorkerTupleBuffer, tuple, targets);
			count++;
			size += tuple->heaptup->t_len + HEAPTUPLESIZE;
		}

		if (num_adhoc)
		{
			Bitmapset *tmp_targets = bms_copy(adhoc_targets);
			int target = 0;
			int ai = 0;

			while ((target = bms_first_member(tmp_targets)) >= 0)
			{
				int tmp_count = 0;
				Size tmp_bytes = 0;
				InsertBatchAck *ack = 0;
				StreamTuple *tuple = 0;
				Bitmapset *single = bms_make_singleton(target);
				
				ack = &adhoc_acks[ai++];
				tuple = MakeStreamTuple(htup, desc, 1, ack);
				tuple->group_hash = target;

				if (TupleBufferInsert(AdhocTupleBuffer, tuple, single))
				{
					adhoc_counts[ai]++;
				}
			}
		}
	}

	stream_stat_report(RelationGetRelid(stream), count, 1, size);

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			InsertBatchWaitAndRemove(batch, count);
		}

		if (num_adhoc)
		{
			int i = 0;

			for (i = 0; i < num_adhoc; ++i)
			{
				InsertBatchWaitAndRemoveActive(adhoc_acks[i].batch, 
											   adhoc_counts[i],
											   active_flags[i],
											   active_cqs[i]);
			}
		}
	}

	if (snap)
		PushActiveSnapshot(GetTransactionSnapshot());

	return count;
}

InsertBatch *
InsertBatchCreate(void)
{
	char *ptr = ShmemDynAlloc0(sizeof(InsertBatch));
	InsertBatch *batch = (InsertBatch *) ptr;

	batch->id = rand() ^ (int) MyProcPid;
	SpinLockInit(&batch->mutex);

	return batch;
}

void
InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples)
{
	if (cont_queries_active ==  NULL)
		cont_queries_active = ContQueryGetActiveFlag();

	if (num_tuples)
	{
		batch->num_wtups = num_tuples;

		while (!StreamBatchAllAcked(batch) && *cont_queries_active)
		{
			pg_usleep(SLEEP_MS * 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	ShmemDynFree(batch);
}

void
InsertBatchWaitAndRemoveActive(InsertBatch *batch, int num_tuples, 
							   int *active, int cq_id)
{
	if (num_tuples && active)
	{
		batch->num_wtups = num_tuples;

		while (!StreamBatchAllAcked(batch) && (*active == cq_id))
		{
			pg_usleep(SLEEP_MS * 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	ShmemDynFree(batch);
}

void
InsertBatchIncrementNumCTuples(InsertBatch *batch)
{
	SpinLockAcquire(&batch->mutex);
	batch->num_ctups++;
	SpinLockRelease(&batch->mutex);
}

void
InsertBatchMarkAcked(InsertBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	SpinLockAcquire(&ack->batch->mutex);
	if (IsContQueryWorkerProcess() || IsContQueryAdhocProcess())
		ack->batch->num_wacks += ack->count;
	else if (IsContQueryCombinerProcess())
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
