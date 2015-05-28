/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_target.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"
#include "utils/guc.h"

#define SLEEP_MS 2

#define StreamBatchAllAcked(batch) ((batch)->num_wacks >= (batch)->num_wtups && (batch)->num_cacks >= (batch)->num_ctups)

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets = NULL;

static HTAB *prepared_stream_inserts = NULL;

/*
 * StorePreparedStreamInsert
 *
 * Create a PreparedStreamInsertStmt with the given column names
 */
PreparedStreamInsertStmt *
StorePreparedStreamInsert(const char *name, RangeVar *stream, List *cols)
{
	PreparedStreamInsertStmt *result;
	bool found;

	if (prepared_stream_inserts == NULL)
	{
		HASHCTL ctl;

		MemSet(&ctl, 0, sizeof(ctl));

		ctl.keysize = NAMEDATALEN;
		ctl.entrysize = sizeof(PreparedStreamInsertStmt);

		prepared_stream_inserts = hash_create("prepared_stream_inserts", 2, &ctl, HASH_ELEM);
	}

	result = (PreparedStreamInsertStmt *)
			hash_search(prepared_stream_inserts, (void *) name, HASH_ENTER, &found);

	result->inserts = NIL;
	result->stream = stream;
	result->cols = cols;
	result->desc = GetStreamTupleDesc(stream, cols);

	return result;
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
 * InsertTargetIsStream
 *
 * Is the given INSERT statements target relation a stream?
 * We assume it is if the relation doesn't exist in the catalog as a normal relation.
 */
bool
InsertTargetIsStream(InsertStmt *ins)
{
	Oid reloid = RangeVarGetRelid(ins->relation, NoLock, true);

	if (reloid != InvalidOid)
		return false;

	return RangeVarIsForStream(ins->relation);
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
	Bitmapset *targets = GetLocalStreamReaders(pstmt->stream);
	TupleDesc desc = GetStreamTupleDesc(pstmt->stream, pstmt->cols);
	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;

	if (synchronous_stream_insert)
	{
		batch = InsertBatchCreate(targets, list_length(pstmt->inserts));
		num_batches = 1;

		acks[0].batch_id = batch->id;
		acks[0].batch = batch;
		acks[0].count = 1;
	}

	foreach(lc, pstmt->inserts)
	{
		int i;
		ParamListInfoData *params = (ParamListInfoData *) lfirst(lc);
		Datum *values = palloc0(params->numParams * sizeof(Datum));
		bool *nulls = palloc0(params->numParams * sizeof(bool));
		StreamTuple *tuple;

		for (i=0; i<params->numParams; i++)
		{
			TypeCacheEntry *type = lookup_type_cache(params->params[i].ptype, 0);
			/*
			 * The incoming param may have a different but cast-compatible type with
			 * the target, so we change the TupleDesc before it gets physically packed
			 * in with the event. Eventually it will be casted to the correct type.
			 */
			desc->attrs[i]->atttypid = params->params[i].ptype;
			desc->attrs[i]->attbyval = type->typbyval;
			desc->attrs[i]->attalign = type->typalign;
			desc->attrs[i]->attlen = type->typlen;

			values[i] = params->params[i].value;
			nulls[i] = params->params[i].isnull;
		}

		tuple = MakeStreamTuple(heap_form_tuple(desc, values, nulls), desc, num_batches, acks);
		TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		count++;
	}

	if (synchronous_stream_insert)
		InsertBatchWaitAndRemove(batch);

	pstmt->inserts = NIL;

	return count;
}

/*
 * InsertIntoStream
 *
 * Send INSERT-encoded events to the given stream
 */
int
InsertIntoStream(InsertStmt *ins, List *values)
{
	ListCell *lc;
	int numcols = list_length(ins->cols);
	int i;
	int count = 0;
	ParseState *ps = make_parsestate(NULL);
	List *colnames = NIL;
	TupleDesc desc = NULL;
	ExprContext *econtext = CreateStandaloneExprContext();
	Bitmapset *targets = GetLocalStreamReaders(ins->relation);
	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;

	if (synchronous_stream_insert)
	{
		batch = InsertBatchCreate(targets, list_length(values));
		num_batches = 1;

		acks[0].batch_id = batch->id;
		acks[0].batch = batch;
		acks[0].count = 1;
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
			if (strcmp(r->name, res->name) == 0)
				count++;
		}

		if (count > 1)
			elog(ERROR, "column \"%s\" appears more than once in columns list", res->name);

		colnames = lappend(colnames, makeString(res->name));
	}

	desc = GetStreamTupleDesc(ins->relation, colnames);

	/* append each VALUES tuple to the stream buffer */
	foreach (lc, values)
	{
		List *raw = (List *) lfirst(lc);
		List *exprlist = transformExpressionList(ps, raw, EXPR_KIND_VALUES);
		List *exprstatelist;
		ListCell *l;
		Datum *values;
		bool *nulls;
		int col = 0;
		int attindex = 0;
		int evindex = 0;
		List *filteredvals = NIL;
		List *filteredcols = NIL;
		StreamTuple *tuple;

		assign_expr_collations(NULL, (Node *) exprlist);

		/*
		 * Extract all fields from the current tuple that are actually being
		 * read by something.
		 */
		while (attindex < desc->natts && evindex < list_length(exprlist))
		{
			Value *colname = (Value *) list_nth(colnames, evindex);
			Node *n = (Node *) list_nth(exprlist, evindex);

			if (pg_strcasecmp(strVal(colname), NameStr(desc->attrs[attindex]->attname)) == 0)
			{
				/* coerce column to common supertype */
				Node *c = coerce_to_specific_type(NULL, n, desc->attrs[attindex]->atttypid, "VALUES");

				filteredvals = lappend(filteredvals, c);
				filteredcols = lappend(filteredcols, strVal(colname));
				attindex++;
			}
			evindex++;
		}

		/*
		 * If we haven't read any fields then we need to add the last one
		 * available because a COUNT(*) might need it (annoying). We arbitrarily
		 * use the first value in the INSERT tuple in this case.
		 */
		if (!filteredvals)
		{
			filteredvals = lappend(filteredvals, linitial(exprlist));
			filteredcols = lappend(filteredcols, strVal(linitial(colnames)));
		}

		exprstatelist = (List *) ExecInitExpr((Expr *) filteredvals, NULL);

		values = palloc0(list_length(exprstatelist) * sizeof(Datum));
		nulls = palloc0(list_length(exprstatelist) * sizeof(bool));

		/*
		 * Eval expressions to create a tuple of constants
		 */
		col = 0;
		foreach(l, exprstatelist)
		{
			ExprState  *estate = (ExprState *) lfirst(l);

			values[col] = ExecEvalExpr(estate, econtext, &nulls[col], NULL);
			col++;
		}

		/*
		 * Now write the tuple of constants to the TupleBuffer
		 */
		tuple = MakeStreamTuple(heap_form_tuple(desc, values, nulls), desc, num_batches, acks);
		TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		count++;
	}

	FreeExprContext(econtext, false);

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (synchronous_stream_insert)
		InsertBatchWaitAndRemove(batch);

	return count;
}

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
uint64
CopyIntoStream(Oid namespace, char *stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	RangeVar *rv = makeNode(RangeVar);
	Bitmapset *targets;
	uint64 count = 0;
	int i;
	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;

	rv->relname = stream;
	rv->schemaname = get_namespace_name(namespace);

	targets = GetLocalStreamReaders(rv);

	if (synchronous_stream_insert)
	{
		batch = InsertBatchCreate(targets, ntuples);
		num_batches = 1;

		acks[0].batch_id = batch->id;
		acks[0].batch = batch;
		acks[0].count = 1;
	}

	for (i=0; i<ntuples; i++)
	{
		HeapTuple htup = tuples[i];
		StreamTuple *tuple;

		tuple = MakeStreamTuple(htup, desc, num_batches, acks);
		TupleBufferInsert(WorkerTupleBuffer, tuple, targets);

		count++;
	}

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (synchronous_stream_insert)
		InsertBatchWaitAndRemove(batch);

	return count;
}


InsertBatch *
InsertBatchCreate(Bitmapset *readers, int num_tuples)
{
	char *ptr = ShmemDynAlloc0(sizeof(InsertBatch) + BITMAPSET_SIZE(readers->nwords));
	InsertBatch *batch = (InsertBatch *) ptr;

	batch->id = rand() ^ (int) MyProcPid;
	batch->num_tups = num_tuples;
	batch->num_wtups = bms_num_members(readers) * num_tuples;
	SpinLockInit(&batch->mutex);

	ptr += sizeof(InsertBatch);
	batch->readers = (Bitmapset *) ptr;
	memcpy(batch->readers, readers, BITMAPSET_SIZE(readers->nwords));

	return batch;
}

void
InsertBatchWaitAndRemove(InsertBatch *batch)
{
	while (!StreamBatchAllAcked(batch))
	{
		pg_usleep(SLEEP_MS * 1000);
		CHECK_FOR_INTERRUPTS();
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
	if (IsContQueryWorkerProcess())
		ack->batch->num_wacks += ack->count;
	else if (IsContQueryCombinerProcess())
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
