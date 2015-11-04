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
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"
#include "utils/memutils.h"

#define SLEEP_MS 1

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

	if (synchronous_stream_insert)
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

	cpysts->queries = ptr_difference(dest, pos);
	memcpy(pos, sts->queries, BITMAPSET_SIZE(sts->queries->nwords));
	pos += BITMAPSET_SIZE(sts->queries->nwords);

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
	sts->desc =  ptr_offset(sts, sts->desc);
	sts->queries = ptr_offset(sts, sts->queries);
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
	RecordTupleDesc *rdescs;

	*len =  sizeof(StreamTupleState);
	*len += VARSIZE(packed_desc);
	*len += HEAPTUPLESIZE + tup->t_len;
	*len += sizeof(InsertBatchAck);
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

typedef struct ContQueryProcIPCState
{
	bool connected;
	ContQueryProc *proc;
	dsm_cqueue_handle *cq_handle;
} ContQueryProcIPCState;

static ContQueryProcIPCState *WorkerIPCStates = NULL;
static ContQueryProcIPCState *CombinerIPCStates = NULL;

static void
attach_to_cont_proc(ContQueryProcIPCState *cpstate)
{
	ContQueryProc *proc;

	if (cpstate->connected)
		return;

	proc = cpstate->proc;
	if (proc->dsm_handle == 0)
		return;

	cpstate->cq_handle = dsm_cqueue_attach(proc->dsm_handle);
	dsm_pin_mapping(cpstate->cq_handle->segment);
	cpstate->connected = true;
}

static void
attach_to_cont_procs(ContQueryProcType type)
{
	int i;
	ContQueryProcIPCState **states;
	int nprocs;
	ContQueryProc *procs;

	if (type == Worker)
	{
		states = &WorkerIPCStates;
		procs = GetContQueryWorkerProcs();
		nprocs = continuous_query_num_workers;
	}
	else if (type == Combiner)
	{
		states = &CombinerIPCStates;
		procs = GetContQueryCombinerProcs();
		nprocs = continuous_query_num_combiners;
	}
	else
	{
		elog(ERROR, "can only attach to workers or combiners");
		return; /* keep compiler quiet */
	}

	if (*states == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
		*states = palloc0(sizeof(ContQueryProcIPCState) * nprocs);
		MemoryContextSwitchTo(old);
	}
	else
		return;

	for (i = 0; i < nprocs; i++)
	{
		ContQueryProc *proc = &procs[i];
		ContQueryProcIPCState *cpstate = &(*states)[i];

		cpstate->proc = proc;
		attach_to_cont_proc(cpstate);
	}
}

dsm_cqueue *
GetWorkerQueue(void)
{
	static long idx = -1;
	ContQueryProcIPCState *cpstate;
	int ntries = 0;
	dsm_cqueue *cq = NULL;

	attach_to_cont_procs(Worker);
	Assert(WorkerIPCStates);

	if (idx == -1)
		idx = rand() % continuous_query_num_workers;

	idx = (idx + 1) % continuous_query_num_workers;

	for (;;)
	{
		cpstate = &WorkerIPCStates[idx];

		if (!cpstate->connected)
			attach_to_cont_proc(cpstate);

		if (cpstate->connected)
		{
			cq = cpstate->cq_handle->cqueue;

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
	int idx;
	ContQueryProcIPCState *cpstate;

	attach_to_cont_procs(Combiner);
	Assert(CombinerIPCStates);

	idx = pts->hash % continuous_query_num_combiners;
	cpstate = &CombinerIPCStates[idx];


	for (;;)
	{
		if (!cpstate->connected)
			attach_to_cont_proc(cpstate);

		if (cpstate->connected)
			break;
	}

	dsm_cqueue_lock(cpstate->cq_handle->cqueue);
	return cpstate->cq_handle->cqueue;
}

ContExecutor *
ContExecutorNew(ContQueryProcType type)
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
	exec->cur_query_id = InvalidOid;

	StartTransactionCommand();
	exec->cq_handle = dsm_cqueue_attach(MyContQueryProc->dsm_handle);
	dsm_cqueue_pop_peeked(exec->cq_handle->cqueue);
	CommitTransactionCommand();

	MemoryContextSwitchTo(old);

	return exec;
}

void
ContExecutorDestroy(ContExecutor *exec)
{
	MemoryContextDelete(exec->cxt);

	dsm_cqueue_pop_peeked(exec->cq_handle->cqueue);
	dsm_cqueue_detach(exec->cq_handle);

	pfree(exec);
}

void
ContExecutorStartBatch(ContExecutor *exec)
{
	if (dsm_cqueue_is_empty(exec->cq_handle->cqueue))
	{
		char *proc_name = GetContQueryProcName(MyContQueryProc);
		cq_stat_report(true);

		pgstat_report_activity(STATE_IDLE, proc_name);
		dsm_cqueue_wait_non_empty(exec->cq_handle->cqueue, 0);
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

Oid
ContExecutorStartNextQuery(ContExecutor *exec)
{
	MemoryContextSwitchTo(exec->exec_cxt);

	exec->cur_query_id = InvalidOid;

	for (;;)
	{
		int id = bms_first_member(exec->exec_queries);

		if (id == -1)
			return InvalidOid;

		exec->cur_query_id = id;

		if (!exec->timedout)
			return exec->cur_query_id;

		if (bms_is_member(exec->cur_query_id, exec->queries_seen))
			return exec->cur_query_id;
	}

	return InvalidOid;
}

void
ContExecutorPurgeQuery(ContExecutor *exec)
{
	MemoryContext old = MemoryContextSwitchTo(exec->cxt);
	exec->queries = bms_del_member(exec->queries, exec->cur_query_id);
	MemoryContextSwitchTo(old);
}

static inline bool
should_yield_item(ContExecutor *exec, void *ptr)
{
	if (exec->ptype == Worker)
	{
		StreamTupleState *sts = (StreamTupleState *) ptr;
		bool succ = bms_is_member(exec->cur_query_id, sts->queries);

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
		return exec->cur_query_id == pts->query_id;
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

			ptr = dsm_cqueue_peek_next(exec->cq_handle->cqueue, len);
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

		ptr = dsm_cqueue_peek_next(exec->cq_handle->cqueue, len);

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

	dsm_cqueue_unpeek(exec->cq_handle->cqueue);

	if (!bms_is_empty(exec->queries_seen) && exec->update_queries)
	{
		MemoryContext old = MemoryContextSwitchTo(exec->cxt);
		exec->queries = bms_add_members(exec->queries, exec->queries_seen);

		MemoryContextSwitchTo(exec->exec_cxt);
		exec->exec_queries = bms_add_members(exec->exec_queries, exec->queries_seen);
		exec->exec_queries = bms_del_member(exec->exec_queries, exec->cur_query_id);

		MemoryContextSwitchTo(old);

		exec->update_queries = false;
	}

	exec->cur_query_id = InvalidOid;
	exec->depleted = false;
	list_free(exec->yielded);
	exec->yielded = NIL;
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
		ptr = dsm_cqueue_peek_next(exec->cq_handle->cqueue, &len);
	}

	Assert(exec->cursor == NULL || exec->cursor == ptr);

	dsm_cqueue_pop_peeked(exec->cq_handle->cqueue);

	exec->cursor = NULL;
	exec->nitems = 0;
	exec->started = false;
	exec->timedout = false;
	exec->depleted = false;
	exec->queries_seen = NULL;
	exec->exec_queries = NULL;
}
