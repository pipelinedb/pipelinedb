/*-------------------------------------------------------------------------
 *
 * cont_execute.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_execute.c
 *
 */

#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define SLEEP_MS 1

dsm_cqueue_peek_fn PartialTupleStatePeekFnHook = NULL;

void
PartialTupleStateCopyFn(void *dest, void *src, int len)
{
	PartialTupleState *pts = (PartialTupleState *) src;
	PartialTupleState *cpypts = (PartialTupleState *) dest;
	char *pos = (char *) dest;

	memcpy(pos, pts, sizeof(PartialTupleState));
	pos += sizeof(PartialTupleState);

	cpypts->tup = ptr_difference(dest, pos);
	memcpy(pos, pts->tup, HEAPTUPLESIZE);
	pos += HEAPTUPLESIZE;
	memcpy(pos, pts->tup->t_data, pts->tup->t_len);
	pos += pts->tup->t_len;

	if (synchronous_stream_insert)
	{
		cpypts->acks = ptr_difference(dest, pos);
		memcpy(pos, pts->acks, sizeof(InsertBatchAck) * pts->nacks);
		pos += sizeof(InsertBatchAck) * pts->nacks;
	}
	else
		cpypts->acks = NULL;

	Assert((uintptr_t) ptr_difference(dest, pos) == len);
}

void
PartialTupleStatePeekFn(void *ptr, int len)
{
	PartialTupleState *pts = (PartialTupleState *) ptr;
	pts->acks = ptr_offset(pts, pts->acks);
	pts->tup = ptr_offset(pts, pts->tup);
	pts->tup->t_data = (HeapTupleHeader) (((char *) pts->tup) + HEAPTUPLESIZE);

	if (pts->query_id == InvalidOid)
	{
		HeapTuple tup;
		Oid namespace;
		Form_pipeline_query row;

		Assert(strlen(NameStr(pts->cv)));
		Assert(strlen(NameStr(pts->namespace)));

		namespace = get_namespace_oid(NameStr(pts->namespace), false);

		if (!OidIsValid(namespace))
		{
			elog(WARNING, "couldn't find namespace %s, skipping partial tuple", NameStr(pts->namespace));
			return;
		}

		tup = SearchSysCache2(PIPELINEQUERYNAMESPACENAME, ObjectIdGetDatum(namespace),
				CStringGetDatum(NameStr(pts->cv)));

		if (!HeapTupleIsValid(tup))
		{
			elog(WARNING, "couldn't find continuous view %s.%s, skipping partial tuple",
					NameStr(pts->namespace), NameStr(pts->cv));
			return;
		}

		row = (Form_pipeline_query) GETSTRUCT(tup);
		pts->query_id = row->id;

		ReleaseSysCache(tup);
	}
}

void
PartialTupleStatePopFn(void *ptr, int len)
{
	PartialTupleState *pts = (PartialTupleState *) ptr;
	InsertBatchAck *acks = pts->acks;
	int i;

	for (i = 0; i < pts->nacks; i++)
		InsertBatchAckTuple(&acks[i]);
}

void
StreamTupleStateCopyFn(void *dest, void *src, int len)
{
	StreamTupleState *sts = (StreamTupleState *) src;
	StreamTupleState *cpysts = (StreamTupleState *) dest;
	char *pos = (char *) dest;

	memcpy(pos, sts, sizeof(StreamTupleState));
	pos += sizeof(StreamTupleState);

	cpysts->desc = ptr_difference(dest, pos);
	memcpy(pos, sts->desc, VARSIZE(sts->desc));
	pos += VARSIZE(sts->desc);

	cpysts->tup = ptr_difference(dest, pos);
	memcpy(pos, sts->tup, HEAPTUPLESIZE);
	pos += HEAPTUPLESIZE;
	memcpy(pos, sts->tup->t_data, sts->tup->t_len);
	pos += sts->tup->t_len;

	if (synchronous_stream_insert && sts->ack)
	{
		cpysts->ack = ptr_difference(dest, pos);
		memcpy(pos, sts->ack, sizeof(InsertBatchAck));
		pos += sizeof(InsertBatchAck);
	}
	else
		cpysts->ack = NULL;

	if (sts->num_record_descs)
	{
		int i;

		Assert(!IsContQueryProcess());

		cpysts->record_descs = ptr_difference(dest, pos);

		for (i = 0; i < sts->num_record_descs; i++)
		{
			RecordTupleDesc *rdesc = &sts->record_descs[i];
			memcpy(pos, rdesc, sizeof(RecordTupleDesc));
			pos += sizeof(RecordTupleDesc);
			memcpy(pos, rdesc->desc, VARSIZE(rdesc->desc));
			pos += VARSIZE(rdesc->desc);
		}
	}

	if (!bms_is_empty(sts->queries))
	{
		cpysts->queries = ptr_difference(dest, pos);
		memcpy(pos, sts->queries, BITMAPSET_SIZE(sts->queries->nwords));
		pos += BITMAPSET_SIZE(sts->queries->nwords);
	}
	else
		cpysts->queries = NULL;

	Assert((uintptr_t) ptr_difference(dest, pos) == len);
}

void
StreamTupleStatePeekFn(void *ptr, int len)
{
	StreamTupleState *sts = (StreamTupleState *) ptr;
	if (sts->ack)
		sts->ack = ptr_offset(sts, sts->ack);
	else
		sts->ack = NULL;

	if (sts->queries)
		sts->queries = ptr_offset(sts, sts->queries);
	else
		sts->queries = NULL;

	sts->desc =  ptr_offset(sts, sts->desc);
	sts->record_descs = ptr_offset(sts, sts->record_descs);
	sts->tup = ptr_offset(sts, sts->tup);
	sts->tup->t_data = (HeapTupleHeader) (((char *) sts->tup) + HEAPTUPLESIZE);
}

void
StreamTupleStatePopFn(void *ptr, int len)
{
	StreamTupleState *sts = (StreamTupleState *) ptr;
	if (sts->ack)
		InsertBatchAckTuple(sts->ack);
}

StreamTupleState *
StreamTupleStateCreate(HeapTuple tup, TupleDesc desc, bytea *packed_desc, Bitmapset *queries, InsertBatchAck *ack, int *len)
{
	StreamTupleState *tupstate = palloc0(sizeof(StreamTupleState));
	int i;
	int nrdescs = 0;
	RecordTupleDesc *rdescs = NULL;

	*len =  sizeof(StreamTupleState);
	*len += VARSIZE(packed_desc);
	*len += HEAPTUPLESIZE + tup->t_len;

	if (ack)
		*len += sizeof(InsertBatchAck);

	if (queries)
		*len += BITMAPSET_SIZE(queries->nwords);

	tupstate->ack = ack;
	tupstate->arrival_time = GetCurrentTimestamp();
	tupstate->desc = packed_desc;

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = desc->attrs[i];
		RecordTupleDesc *rdesc;
		Datum v;
		bool isnull;
		HeapTupleHeader rec;
		int32 tupTypmod;
		TupleDesc attdesc;

		if (attr->atttypid != RECORDOID)
			continue;

		v = heap_getattr(tup, i + 1, desc, &isnull);

		if (isnull)
			continue;

		if (rdescs == NULL)
			rdescs = palloc0(sizeof(RecordTupleDesc) * desc->natts);

		rec = DatumGetHeapTupleHeader(v);
		Assert(HeapTupleHeaderGetTypeId(rec) == RECORDOID);
		tupTypmod = HeapTupleHeaderGetTypMod(rec);

		rdesc = &rdescs[nrdescs++];
		rdesc->typmod = tupTypmod;
		attdesc = lookup_rowtype_tupdesc(RECORDOID, tupTypmod);
		rdesc->desc = PackTupleDesc(attdesc);
		ReleaseTupleDesc(attdesc);

		len += sizeof(RecordTupleDesc);
		len += VARSIZE(rdesc->desc);
	}

	tupstate->num_record_descs = nrdescs;
	tupstate->record_descs = rdescs;
	tupstate->queries = queries;
	tupstate->tup = tup;

	return tupstate;
}

InsertBatch *
InsertBatchCreate(void)
{
	InsertBatch *batch = (InsertBatch *) ShmemDynAlloc0(sizeof(InsertBatch));
	batch->id = rand() ^ (int) MyProcPid;
	atomic_init(&batch->num_cacks, 0);
	atomic_init(&batch->num_ctups, 0);
	atomic_init(&batch->num_wacks, 0);
	return batch;
}

static inline bool
InsertBatchAllAcked(InsertBatch *batch)
{
	return (atomic_load(&batch->num_wacks) >= batch->num_wtups &&
			atomic_load(&batch->num_cacks) >= atomic_load(&batch->num_ctups));
}

void
InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples)
{
	if (num_tuples)
	{
		batch->num_wtups = num_tuples;
		while (!InsertBatchAllAcked(batch))
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
	atomic_fetch_add(&batch->num_ctups, 1);
}

void
InsertBatchAckTuple(InsertBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	if (IsContQueryWorkerProcess())
		atomic_fetch_add(&ack->batch->num_wacks, 1);
	else if (IsContQueryCombinerProcess())
		atomic_fetch_add(&ack->batch->num_cacks, 1);
}

static dsm_segment *
attach_to_db_dsm_segment(void)
{
	static dsm_segment *db_dsm_segment = NULL;
	dsm_handle handle;
	Timestamp start_time;

	if (db_dsm_segment != NULL)
		return db_dsm_segment;

	start_time = GetCurrentTimestamp();

	/* Wait till all db workers have started up */
	while ((handle = GetDatabaseDSMHandle(NULL)) == 0)
	{
		pg_usleep(SLEEP_MS);

		if (TimestampDifferenceExceeds(start_time, GetCurrentTimestamp(), 1000))
			elog(ERROR, "could not connect to database dsm segment");
	}

	db_dsm_segment = dsm_find_or_attach(handle);
	dsm_pin_mapping(db_dsm_segment);
	return db_dsm_segment;
}

dsm_cqueue *
GetWorkerQueue(void)
{
	static long idx = -1;
	int ntries = 0;
	dsm_cqueue *cq = NULL;
	dsm_segment *segment;

	segment = attach_to_db_dsm_segment();

	if (idx == -1)
		idx = rand() % continuous_query_num_workers;

	idx = (idx + 1) % continuous_query_num_workers;

	for (;;)
	{
		cq = GetWorkerCQueue(segment, idx);

		/*
		 * Try to lock dsm_cqueue of any worker that is not already locked in
		 * a round robin fashion.
		 */
		if (ntries < continuous_query_num_workers)
		{
			if (dsm_cqueue_lock_nowait(cq))
				break;
		}
		else
		{
			/* If all workers are locked, then just wait for the lock. */
			dsm_cqueue_lock(cq);
			break;
		}

		ntries++;
		idx = (idx + 1) % continuous_query_num_workers;
	}

	Assert(cq);
	Assert(LWLockHeldByMe(&cq->lock));

	return cq;
}

dsm_cqueue *
GetCombinerQueue(PartialTupleState *pts)
{
	int idx = pts->hash % continuous_query_num_combiners;
	dsm_segment *segment = attach_to_db_dsm_segment();
	dsm_cqueue *cq = GetCombinerCQueue(segment, idx);
	dsm_cqueue_lock(cq);
	return cq;
}

ContExecutor *
ContExecutorNew(ContQueryProcType type, ContQueryStateInit initfn)
{
	ContExecutor *exec;
	MemoryContext cxt;
	MemoryContext old;

	/* Allocate continuous query execution state in a long-lived memory context. */
	cxt = AllocSetContextCreate(TopMemoryContext, "ContExecutor Context",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(cxt);

	exec = palloc0(sizeof(ContExecutor));
	exec->cxt = cxt;
	exec->exec_cxt = AllocSetContextCreate(cxt, "ContExecutor Exec Context",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	exec->ptype = type;
	exec->current_query_id = InvalidOid;
	exec->initfn = initfn;

	exec->cqueue = GetContProcCQueue(MyContQueryProc);
	dsm_cqueue_unpeek(exec->cqueue);

	MemoryContextSwitchTo(old);

	return exec;
}

void
ContExecutorDestroy(ContExecutor *exec)
{
	dsm_cqueue_pop_peeked(exec->cqueue);
	MemoryContextDelete(exec->cxt);
}

void
ContExecutorStartBatch(ContExecutor *exec)
{
	if (dsm_cqueue_is_empty(exec->cqueue))
	{
		char *proc_name = GetContQueryProcName(MyContQueryProc);
		cq_stat_report(true);

		pgstat_report_activity(STATE_IDLE, proc_name);
		dsm_cqueue_wait_non_empty(exec->cqueue, 0);
		pgstat_report_activity(STATE_RUNNING, proc_name);

		pfree(proc_name);
	}

	if (bms_is_empty(exec->queries))
	{
		MemoryContext old = CurrentMemoryContext;

		StartTransactionCommand();
		MemoryContextSwitchTo(exec->cxt);
		exec->queries = GetAllContinuousViewIds();
		CommitTransactionCommand();

		MemoryContextSwitchTo(old);
	}

	StartTransactionCommand();

	MemoryContextSwitchTo(exec->exec_cxt);
	ContQueryBatchContext = exec->exec_cxt;

	exec->exec_queries = bms_copy(exec->queries);
	exec->update_queries = true;
}

static ContQueryState *
init_query_state(ContExecutor *exec, ContQueryState *state)
{
	MemoryContext state_cxt;
	MemoryContext old_cxt;

	state_cxt = AllocSetContextCreate(exec->cxt, "QueryStateCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old_cxt = MemoryContextSwitchTo(state_cxt);

	state->view_id = exec->current_query_id;
	state->state_cxt = state_cxt;
	state->view = GetContinuousView(exec->current_query_id);
	state->tmp_cxt = AllocSetContextCreate(state_cxt, "QueryStateTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	if (state->view == NULL)
		return state;

	cq_stat_init(&state->stats, state->view->id, 0);
	state = exec->initfn(exec, state);

	MemoryContextSwitchTo(old_cxt);

	return state;
}

static ContQueryState *
get_query_state(ContExecutor *exec)
{
	ContQueryState *state = exec->states[exec->current_query_id];
	HeapTuple tup;

	MyCQStats = NULL;

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	tup = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(exec->current_query_id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tup))
	{
		PopActiveSnapshot();
		ContExecutorPurgeQuery(exec);
		return NULL;
	}

	if (state != NULL)
	{
		/* Was the continuous view modified? In our case this means remove the old view and add a new one. */
		if (HeapTupleGetOid(tup) != state->view->oid)
		{
			ContExecutorPurgeQuery(exec);
			state = NULL;
		}
	}

	ReleaseSysCache(tup);

	if (state == NULL)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(exec->cxt);
		state = palloc0(sizeof(ContQueryState));
		state = init_query_state(exec, state);
		exec->states[exec->current_query_id] = state;
		MemoryContextSwitchTo(old_cxt);

		if (state->view == NULL)
		{
			PopActiveSnapshot();
			ContExecutorPurgeQuery(exec);
			return NULL;
		}
	}

	PopActiveSnapshot();

	MyCQStats = &state->stats;

	return state;
}

Oid
ContExecutorStartNextQuery(ContExecutor *exec)
{
	MemoryContextSwitchTo(exec->exec_cxt);

	for (;;)
	{
		int id = bms_first_member(exec->exec_queries);

		if (id == -1)
		{
			exec->current_query_id = InvalidOid;
			break;
		}

		exec->current_query_id = id;

		if (!exec->timedout)
			break;

		if (bms_is_member(exec->current_query_id, exec->queries_seen))
			break;
	}

	if (exec->current_query_id == InvalidOid)
		exec->current_query = NULL;
	else
	{
		exec->current_query = get_query_state(exec);

		if (exec->current_query == NULL)
		{
			MemoryContext old = MemoryContextSwitchTo(exec->cxt);
			exec->queries = bms_del_member(exec->queries, exec->current_query_id);
			MemoryContextSwitchTo(old);
		}
		else
			debug_query_string = NameStr(exec->current_query->view->name);
	}

	return exec->current_query_id;
}

void
ContExecutorPurgeQuery(ContExecutor *exec)
{
	MemoryContext old = MemoryContextSwitchTo(exec->cxt);
	ContQueryState *state = exec->states[exec->current_query_id];

	exec->queries = bms_del_member(exec->queries, exec->current_query_id);

	if (state)
	{
		MemoryContextDelete(state->state_cxt);
		exec->states[exec->current_query_id] = NULL;
	}

	MemoryContextSwitchTo(old);

	IncrementCQErrors(1);
}

static inline bool
should_yield_item(ContExecutor *exec, void *ptr)
{
	if (exec->ptype == Worker)
	{
		StreamTupleState *sts = (StreamTupleState *) ptr;
		bool succ = bms_is_member(exec->current_query_id, sts->queries);

		if (synchronous_stream_insert && succ)
		{
			MemoryContext old = MemoryContextSwitchTo(exec->exec_cxt);
			exec->yielded = lappend(exec->yielded, sts);
			MemoryContextSwitchTo(old);
		}

		return succ;
	}
	else
	{
		PartialTupleState *pts = (PartialTupleState *) ptr;
		return exec->current_query_id == pts->query_id;
	}
}

void *
ContExecutorYieldItem(ContExecutor *exec, int *len)
{
	ContQueryRunParams *params;

	/* We've yielded all items belonging to the CQ in this batch? */
	if (exec->depleted)
		return NULL;

	if (exec->timedout)
	{
		if (exec->nitems == 0)
			return NULL;

		for (;;)
		{
			void *ptr;

			if (exec->depleted)
				return NULL;

			ptr = dsm_cqueue_peek_next(exec->cqueue, len);
			Assert(ptr);

			if (ptr == exec->cursor)
				exec->depleted = true;

			if (should_yield_item(exec, ptr))
				return ptr;
		}
	}

	if (!exec->started)
	{
		exec->started = true;
		exec->start_time = GetCurrentTimestamp();
	}

	params = GetContQueryRunParams();

	for (;;)
	{
		void *ptr;

		/* We've read a full batch or waited long enough? */
		if (exec->nitems == params->batch_size ||
				TimestampDifferenceExceeds(exec->start_time, GetCurrentTimestamp(), params->max_wait) ||
				MyContQueryProc->db_meta->terminate)
		{
			exec->timedout = true;
			exec->depleted = true;
			return NULL;
		}

		ptr = dsm_cqueue_peek_next(exec->cqueue, len);

		if (ptr)
		{
			MemoryContext old;
			exec->cursor = ptr;
			exec->nitems++;

			old = MemoryContextSwitchTo(exec->exec_cxt);

			if (exec->ptype == Worker)
			{
				StreamTupleState *sts = (StreamTupleState *) ptr;
				exec->queries_seen = bms_add_members(exec->queries_seen, sts->queries);
			}
			else
			{
				PartialTupleState *pts = (PartialTupleState *) ptr;
				exec->queries_seen = bms_add_member(exec->queries_seen, pts->query_id);
			}

			MemoryContextSwitchTo(old);

			if (should_yield_item(exec, ptr))
				return ptr;
		}
		else
			pg_usleep(SLEEP_MS * 1000);
	}
}

void
ContExecutorEndQuery(ContExecutor *exec)
{
	IncrementCQExecutions(1);

	if (!exec->started)
		return;

	dsm_cqueue_unpeek(exec->cqueue);

	if (!bms_is_empty(exec->queries_seen) && exec->update_queries)
	{
		MemoryContext old = MemoryContextSwitchTo(exec->cxt);
		exec->queries = bms_add_members(exec->queries, exec->queries_seen);

		MemoryContextSwitchTo(exec->exec_cxt);
		exec->exec_queries = bms_add_members(exec->exec_queries, exec->queries_seen);
		exec->exec_queries = bms_del_member(exec->exec_queries, exec->current_query_id);

		MemoryContextSwitchTo(old);

		exec->update_queries = false;
	}

	exec->current_query_id = InvalidOid;
	exec->depleted = false;
	list_free(exec->yielded);
	exec->yielded = NIL;

	if (exec->current_query)
		cq_stat_report(false);
	else
		cq_stat_send_purge(exec->current_query_id, 0, exec->ptype == Worker ? CQ_STAT_WORKER : CQ_STAT_COMBINER);

	debug_query_string = NULL;
}

void
ContExecutorEndBatch(ContExecutor *exec)
{
	void *ptr;

	CommitTransactionCommand();
	MemoryContextResetAndDeleteChildren(exec->exec_cxt);

	while (exec->nitems--)
	{
		int len;
		ptr = dsm_cqueue_peek_next(exec->cqueue, &len);
	}

	Assert(exec->cursor == NULL || exec->cursor == ptr);

	dsm_cqueue_pop_peeked(exec->cqueue);

	exec->cursor = NULL;
	exec->nitems = 0;
	exec->started = false;
	exec->timedout = false;
	exec->depleted = false;
	exec->queries_seen = NULL;
	exec->exec_queries = NULL;
}
