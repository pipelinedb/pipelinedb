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

typedef struct BufferedStreamTupleState
{
	int len;
	StreamTupleState sts;
} BufferedStreamTupleState;

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

	if (synchronous_stream_insert && sts->acks)
	{
		cpysts->acks = ptr_difference(dest, pos);
		memcpy(pos, sts->acks, sizeof(InsertBatchAck) * sts->nacks);
		pos += sizeof(InsertBatchAck) * sts->nacks;
	}
	else
		cpysts->acks = NULL;

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
	if (sts->acks)
		sts->acks = ptr_offset(sts, sts->acks);
	else
		sts->acks = NULL;

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
	if (sts->acks)
	{
		int i;

		for (i = 0; i < sts->nacks; i++)
		{
			InsertBatchAck *ack = &sts->acks[i];
			InsertBatchAckTuple(ack);
		}
	}
}

StreamTupleState *
StreamTupleStateCreate(HeapTuple tup, TupleDesc desc, bytea *packed_desc, Bitmapset *queries,
		InsertBatchAck *acks, int nacks, int *len)
{
	StreamTupleState *tupstate = palloc0(sizeof(StreamTupleState));
	int i;
	int nrdescs = 0;
	RecordTupleDesc *rdescs = NULL;

	Assert(acks || nacks == 0);

	*len =  sizeof(StreamTupleState);
	*len += VARSIZE(packed_desc);
	*len += HEAPTUPLESIZE + tup->t_len;

	if (acks)
		*len += sizeof(InsertBatchAck) * nacks;

	if (queries)
		*len += BITMAPSET_SIZE(queries->nwords);

	tupstate->nacks = nacks;
	tupstate->acks = acks;
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
	pg_atomic_init_u32(&batch->num_cacks, 0);
	pg_atomic_init_u32(&batch->num_ctups, 0);
	pg_atomic_init_u32(&batch->num_wacks, 0);
	pg_atomic_init_u32(&batch->num_wtups, 0);
	return batch;
}

static inline bool
InsertBatchAllAcked(InsertBatch *batch)
{
	return (pg_atomic_read_u32(&batch->num_wacks) >= pg_atomic_read_u32(&batch->num_wtups) &&
			pg_atomic_read_u32(&batch->num_cacks) >= pg_atomic_read_u32(&batch->num_ctups));
}

void
InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples)
{
	if (num_tuples)
	{
		pg_atomic_fetch_add_u32(&batch->num_wtups, num_tuples);
		while (!InsertBatchAllAcked(batch))
		{
			pg_usleep(SLEEP_MS * 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	ShmemDynFree(batch);
}

void
InsertBatchIncrementNumCTuples(InsertBatch *batch, int n)
{
	pg_atomic_fetch_add_u32(&batch->num_ctups, n);
}

void
InsertBatchIncrementNumWTuples(InsertBatch *batch, int n)
{
	pg_atomic_fetch_add_u32(&batch->num_wtups, n);
}

void
InsertBatchAckTuple(InsertBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	if (IsContQueryWorkerProcess())
		pg_atomic_fetch_add_u32(&ack->batch->num_wacks, 1);
	else if (IsContQueryCombinerProcess())
		pg_atomic_fetch_add_u32(&ack->batch->num_cacks, 1);
}

/*
 * InsertBatchAckCreate
 *
 * Returns an array of InsertBatchAcks derived from the List of StreamTupleState structs passed to the function.
 * nacksptr is a pointer to an integer which is set to the size of the array returned.
 */
InsertBatchAck *
InsertBatchAckCreate(List *sts, int *nacksptr)
{
	List *acks_list = NIL;
	ListCell *lc;
	int i = 0;
	InsertBatchAck *acks;
	int nacks = 0;

	Assert(synchronous_stream_insert);

	foreach(lc, sts)
	{
		StreamTupleState *sts = lfirst(lc);
		InsertBatchAck *ack = sts->acks;
		ListCell *lc2;
		bool found = false;

		if (!ShmemDynAddrIsValid(ack->batch) || ack->batch_id != ack->batch->id)
			continue;

		foreach(lc2, acks_list)
		{
			InsertBatchAck *ack2 = lfirst(lc2);

			if (ack->batch_id == ack2->batch_id)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			InsertBatchAck *ack2 = (InsertBatchAck *) palloc(sizeof(InsertBatchAck));
			memcpy(ack2, ack, sizeof(InsertBatchAck));
			acks_list = lappend(acks_list, ack2);
			nacks++;
		}
	}

	acks = (InsertBatchAck *) palloc0(sizeof(InsertBatchAck) * nacks);

	foreach(lc, acks_list)
	{
		InsertBatchAck *ack = lfirst(lc);
		acks[i].batch_id = ack->batch_id;
		acks[i].batch = ack->batch;
		i++;
	}

	list_free_deep(acks_list);

	*nacksptr = nacks;
	return acks;
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

	if (exec->ptype == WORKER)
	{
		exec->ipcmq = ipc_multi_queue_init(acquire_my_broker_ipc_queue(), acquire_my_ipc_queue());
		ipc_multi_queue_set_priority_queue(exec->ipcmq, 0);
		ipc_multi_queue_unpeek_all(exec->ipcmq);
	}
	else
	{
		exec->ipcq = acquire_my_ipc_queue();
		ipc_queue_unpeek_all(exec->ipcq);
	}

	exec->peeked_msgs = palloc0(sizeof(ipc_message) * continuous_query_batch_size);
	exec->max_msgs = continuous_query_batch_size;
	exec->num_msgs = 0;

	MemoryContextSwitchTo(old);

	return exec;
}

void
ContExecutorDestroy(ContExecutor *exec)
{
	if (exec->ptype == WORKER)
		ipc_multi_queue_pop_peeked(exec->ipcmq);
	else
		ipc_queue_pop_peeked(exec->ipcq);

	MemoryContextDelete(exec->cxt);
}

void
ContExecutorStartBatch(ContExecutor *exec)
{
	bool is_empty;

	if (exec->ptype == WORKER)
		is_empty = ipc_multi_queue_is_empty(exec->ipcmq);
	else
		is_empty = ipc_queue_is_empty(exec->ipcq);

	if (is_empty)
	{
		if (!IsTransactionState())
		{
			char *proc_name = GetContQueryProcName(MyContQueryProc);

			pgstat_report_cqstat(true);

			pgstat_report_activity(STATE_IDLE, proc_name);

			if (exec->ptype == WORKER)
				ipc_multi_queue_wait_non_empty(exec->ipcmq, 0);
			else
				ipc_queue_wait_non_empty(exec->ipcq, 0);


			pgstat_report_activity(STATE_RUNNING, proc_name);

			pfree(proc_name);
		}
		else
			pg_usleep(500);
	}

	if (bms_is_empty(exec->queries) && !IsTransactionState())
	{
		MemoryContext old = CurrentMemoryContext;

		StartTransactionCommand();
		MemoryContextSwitchTo(exec->cxt);

		/* Combiners only execute queries for continuous views */
		if (IsContQueryCombinerProcess())
			exec->queries = GetContinuousViewIds();
		else
			exec->queries = GetContinuousQueryIds();

		exec->last_queries_load = GetCurrentTransactionStartTimestamp();

		CommitTransactionCommand();

		MemoryContextSwitchTo(old);
	}

	if (!IsTransactionState())
		StartTransactionCommand();

	MemoryContextSwitchTo(exec->exec_cxt);
	ContQueryBatchContext = exec->exec_cxt;

	exec->exec_queries = bms_copy(exec->queries);
	exec->update_queries = true;

	pgstat_start_cq(MyProcStatCQEntry);
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

	state->query_id = exec->current_query_id;
	state->state_cxt = state_cxt;
	state->query = GetContQueryForId(exec->current_query_id);
	state->tmp_cxt = AllocSetContextCreate(state_cxt, "QueryStateTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	if (state->query == NULL)
		return state;

	pgstat_init_cqstat((PgStat_StatCQEntry *) &state->stats, state->query->id, 0);
	state = exec->initfn(exec, state);

	MemoryContextSwitchTo(old_cxt);

	return state;
}

static ContQueryState *
get_query_state(ContExecutor *exec)
{
	ContQueryState *state = exec->states[exec->current_query_id];
	HeapTuple tup;

	MyStatCQEntry = NULL;

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
		if (HeapTupleGetOid(tup) != state->query->oid)
		{
			MemoryContextDelete(state->state_cxt);
			exec->states[exec->current_query_id] = NULL;
			state = NULL;
		}
		else
		{
			Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
			state->query->active = row->active;
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

		if (state->query == NULL)
		{
			PopActiveSnapshot();
			return NULL;
		}
	}

	PopActiveSnapshot();

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

		if (!exec->peek_timedout)
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
			debug_query_string = NameStr(exec->current_query->query->name);
	}

	if (exec->current_query)
	{
		MyStatCQEntry = (PgStat_StatCQEntry *) &exec->current_query->stats;
		pgstat_start_cq(MyStatCQEntry);
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

	exec->current_query = NULL;

	MemoryContextSwitchTo(old);
}

static inline bool
should_yield_item(ContExecutor *exec, void *ptr)
{
	if (exec->ptype == WORKER)
	{
		StreamTupleState *sts = (StreamTupleState *) ptr;
		bool succ = bms_is_member(exec->current_query_id, sts->queries);

		if (synchronous_stream_insert && succ)
		{
			MemoryContext old = MemoryContextSwitchTo(exec->exec_cxt);
			exec->yielded_msgs = lappend(exec->yielded_msgs, sts);
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
ContExecutorYieldNextMessage(ContExecutor *exec, int *len)
{
	static ContQueryRunParams *params = NULL;

	/* We've yielded all items belonging to the CQ in this batch? */
	if (exec->depleted)
		return NULL;

	if (exec->peek_timedout)
	{
		if (exec->num_msgs == 0)
		{
			exec->depleted = true;
			return NULL;
		}

		for (;;)
		{
			void *ptr;
			int mlen;

			if (exec->depleted)
				return NULL;

			ptr = exec->peeked_msgs[exec->curr_msg].msg;
			mlen = exec->peeked_msgs[exec->curr_msg].len;
			exec->curr_msg++;

			if (exec->curr_msg == exec->num_msgs)
				exec->depleted = true;

			if (should_yield_item(exec, ptr))
			{
				*len = mlen;
				return ptr;
			}
		}
	}

	if (!exec->peeked_any)
	{
		exec->peeked_any = true;
		exec->peek_start = GetCurrentTimestamp();
	}

	if (!params)
		params = GetContQueryRunParams();

	for (;;)
	{
		void *ptr;
		int mlen;

		/* We've read a full batch or waited long enough? */
		if (exec->num_msgs == params->batch_size ||
				TimestampDifferenceExceeds(exec->peek_start, GetCurrentTimestamp(), params->max_wait) ||
				MyContQueryProc->db_meta->terminate)
		{
			exec->peek_timedout = true;
			exec->depleted = true;
			return NULL;
		}

		if (exec->ptype == WORKER)
			ptr = ipc_multi_queue_peek_next(exec->ipcmq, &mlen);
		else
			ptr = ipc_queue_peek_next(exec->ipcq, &mlen);

		if (ptr)
		{
			MemoryContext old;

			exec->peeked_msgs[exec->curr_msg].msg = ptr;
			exec->peeked_msgs[exec->curr_msg].len = mlen;

			exec->curr_msg++;
			exec->num_msgs++;

			/* This can happen is continuous_query_batch_size was increased at runtime */
			if (exec->num_msgs == exec->max_msgs)
			{
				exec->max_msgs *= 2;
				exec->peeked_msgs = repalloc(exec->peeked_msgs, sizeof(ipc_message) * exec->max_msgs);
			}

			exec->nbytes += mlen;

			old = MemoryContextSwitchTo(exec->exec_cxt);

			if (exec->ptype == WORKER)
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
			{
				*len = mlen;
				return ptr;
			}
		}
		else
			pg_usleep(SLEEP_MS * 1000);
	}
}

void
ContExecutorEndQuery(ContExecutor *exec)
{
	pgstat_increment_cq_exec(1);

	if (!exec->peeked_any)
		return;

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
	exec->curr_msg = 0;
	exec->depleted = false;

	list_free(exec->yielded_msgs);
	exec->yielded_msgs = NIL;

	if (exec->current_query)
	{
		pgstat_end_cq(MyStatCQEntry);
		pgstat_report_cqstat(false);
	}
	else
		pgstat_send_cqpurge(exec->current_query_id, 0, exec->ptype);

	debug_query_string = NULL;
}

void
ContExecutorEndBatch(ContExecutor *exec, bool commit)
{
	Assert(IsTransactionState());

	if (commit)
		CommitTransactionCommand();

	pgstat_end_cq_batch(MyProcStatCQEntry, exec->num_msgs, exec->nbytes);

	MemoryContextResetAndDeleteChildren(exec->exec_cxt);

	if (exec->peeked_any)
	{
		if (exec->ptype == WORKER)
			ipc_multi_queue_pop_peeked(exec->ipcmq);
		else
			ipc_queue_pop_peeked(exec->ipcq);
	}
	else
	{
		Assert(bms_num_members(exec->queries) == 0);

		if (exec->ptype == WORKER)
			ipc_multi_queue_pop_inserted_before(exec->ipcmq, exec->last_queries_load);
		else
			ipc_queue_pop_inserted_before(exec->ipcq, exec->last_queries_load);
	}

	exec->curr_msg = 0;
	exec->num_msgs = 0;
	exec->nbytes = 0;
	exec->peeked_any = false;
	exec->peek_timedout = false;
	exec->depleted = false;
	exec->queries_seen = NULL;
	exec->exec_queries = NULL;
}
