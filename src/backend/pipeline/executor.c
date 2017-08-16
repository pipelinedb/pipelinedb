/*-------------------------------------------------------------------------
 *
 * executor.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/executor.c
 *
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "pipeline/executor.h"
#include "pipeline/scheduler.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/ipc/reader.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/syscache.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define MAX_IN_XACT_TIMEOUT 5 /* 5ms */
#define MAX_NOT_IN_XACT_TIMEOUT 3000 /* 3s */

/*
 * We use this relation as the basis for mutual exclusion between worker/combiner
 * execution and DROP CONTINUOUS execution.
 */
Oid PipelineExecLockRelationOid = InvalidOid;

/*
 * exec_begin
 */
static void
exec_begin(ContExecutor *exec)
{
	StartTransactionCommand();
	exec->lock = AcquireContExecutionLock(AccessShareLock);
}

/*
 * exec_commit
 */
static void
exec_commit(ContExecutor *exec)
{
	/*
	 * We can end up here without having acquired the lock when it's the first execution of
	 * the executor's CQs, and no CQ's state has been retrieved yet.
	 */
	if (exec->lock)
		ReleaseContExecutionLock(exec->lock);

	CommitTransactionCommand();
}

ContExecutor *
ContExecutorNew(ContQueryStateInit initfn)
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

	ContQueryBatchContext = AllocSetContextCreate(cxt, "ContQueryBatchContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	ContQueryTransactionContext = AllocSetContextCreate(cxt, "ContQueryTransactionContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	exec->initfn = initfn;
	exec->ptype = MyContQueryProc->type;
	exec->pname = GetContQueryProcName(MyContQueryProc);

	MemoryContextSwitchTo(old);

	ipc_tuple_reader_init();
	pgstat_report_activity(STATE_RUNNING, exec->pname);

	debug_query_string = NULL;
	MyStatCQEntry = NULL;

	return exec;
}

void
ContExecutorDestroy(ContExecutor *exec)
{
	ipc_tuple_reader_destroy();
	MemoryContextDelete(exec->cxt);
}

void
ContExecutorStartBatch(ContExecutor *exec, int timeout)
{
	bool success;

	exec->batch = NULL;

	/*
	 * We should never sleep forever, since there is a race in setting got_SIGTERM and
	 * zmq_poll(). If we set got_SIGTERM right before calling zmq_poll(), we will
	 * sleep forever since the postmaster doens't resend SIGTERM signals to unresponsive
	 * child processes.
	 */
	if (get_sigterm_flag())
		timeout = 0;
	else if (IsTransactionState())
		timeout = timeout ? Min(MAX_IN_XACT_TIMEOUT, timeout) : MAX_IN_XACT_TIMEOUT;
	else
		timeout = timeout ? Min(MAX_NOT_IN_XACT_TIMEOUT, timeout) : MAX_NOT_IN_XACT_TIMEOUT;

	/* TODO(usmanm): report activity */
	success = ipc_tuple_reader_poll(timeout);

	if (!IsTransactionState())
		exec_begin(exec);

	if (success)
	{
		exec->batch = ipc_tuple_reader_pull();

		if (bms_is_empty(exec->all_queries))
		{
			MemoryContext old;

			old = MemoryContextSwitchTo(exec->cxt);

			/* Combiners only need to execute view queries */
			if (exec->ptype == Combiner)
				exec->all_queries = GetContinuousViewIds();
			else
				exec->all_queries = GetContinuousQueryIds();

			MemoryContextSwitchTo(old);
		}

		if (!bms_is_subset(exec->batch->queries, exec->all_queries))
		{
			Bitmapset *queries;
			MemoryContext old;

			old = MemoryContextSwitchTo(exec->cxt);

			queries = exec->all_queries;
			exec->all_queries = bms_union(queries, exec->batch->queries);
			bms_free(queries);

			MemoryContextSwitchTo(old);
		}
	}

	MemoryContextSwitchTo(ContQueryBatchContext);

	exec->exec_queries = bms_copy(exec->all_queries);

	pgstat_start_cq_batch();
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

	state->query_id = exec->curr_query_id;
	state->state_cxt = state_cxt;
	state->query = GetContQueryForId(exec->curr_query_id);
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
	ContQueryState *state;
	HeapTuple tup;
	bool commit = false;

	MyStatCQEntry = NULL;
	state = exec->states[exec->curr_query_id];

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		exec_commit(exec);
		exec_begin(exec);
		commit = true;
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	tup = SearchPipelineSysCache1(PIPELINEQUERYID, Int32GetDatum(exec->curr_query_id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tup))
	{
		PopActiveSnapshot();
		ContExecutorPurgeQuery(exec);
		return NULL;
	}

	if (state != NULL)
	{
		/*
		 * Was the continuous view modified? In our case this means remove the old view and add a new one.
		 * Don't purge the query since the ID is still valid. We just have a stale state.
		 */
		if (HeapTupleGetOid(tup) != state->query->oid)
		{
			MemoryContextDelete(state->state_cxt);
			exec->states[exec->curr_query_id] = NULL;
			state = NULL;
			commit = true;
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

		MemoryContextSwitchTo(old_cxt);

		exec->states[exec->curr_query_id] = state;

		if (state->query == NULL)
		{
			PopActiveSnapshot();
			ContExecutorPurgeQuery(exec);
			return NULL;
		}
	}

	PopActiveSnapshot();

	if (commit)
	{
		exec_commit(exec);
		exec_begin(exec);
	}

	Assert(exec->states[exec->curr_query_id] == state);
	Assert(state->query);

	return state;
}

Oid
ContExecutorStartNextQuery(ContExecutor *exec, int timeout)
{
	MemoryContextSwitchTo(ContQueryBatchContext);

	for (;;)
	{
		int id = bms_first_member(exec->exec_queries);

		if (id == -1)
		{
			exec->curr_query_id = InvalidOid;
			break;
		}

		exec->curr_query_id = id;

		/*
		 * If we have a timeout, we want to force an execution of this
		 * query regardless of whether or not it actually has data. This
		 * is primarily used for ticking SW queries that are writing to output streams.
		 */
		if (timeout)
			break;

		if (!exec->batch)
		{
			exec->curr_query_id = InvalidOid;
			break;
		}

		if (bms_is_member(exec->curr_query_id, exec->batch->queries))
			break;
	}

	if (!OidIsValid(exec->curr_query_id))
		exec->curr_query = NULL;
	else
	{
		ContQueryState *state = get_query_state(exec);
		if (state && state->query->active)
		{
			exec->curr_query = state;
			debug_query_string = state->query->name->relname;
			MyStatCQEntry = (PgStat_StatCQEntry *) &exec->curr_query->stats;
			pgstat_start_cq(MyStatCQEntry);
		}
		else
			exec->curr_query = NULL;
	}

	return exec->curr_query_id;
}

void
ContExecutorPurgeQuery(ContExecutor *exec)
{
	MemoryContext old;
	ContQueryState *state = exec->states[exec->curr_query_id];

	old = MemoryContextSwitchTo(exec->cxt);
	exec->all_queries = bms_del_member(exec->all_queries, exec->curr_query_id);
	MemoryContextSwitchTo(old);

	if (state)
	{
		MemoryContextDelete(state->state_cxt);
		exec->states[exec->curr_query_id] = NULL;
	}

	/*
	 * We can end up here if we're purging a query whose state was never loaded,
	 * e.g. if it was deleted before ever being executed.
	 */
	if (exec->lock)
		ReleaseContExecutionLock(exec->lock);

	exec->curr_query = NULL;
	exec->lock = NULL;
}

void
ContExecutorEndQuery(ContExecutor *exec)
{
	if (!exec->batch)
		return;

	pgstat_increment_cq_exec(1);

	exec->curr_query_id = InvalidOid;
	if (exec->curr_query)
	{
		pgstat_end_cq(MyStatCQEntry);
		if (exec->ptype == Worker)
			pgstat_report_cqstat(false);
	}
	else
		pgstat_send_cqpurge(exec->curr_query_id, 0, exec->ptype);

	ipc_tuple_reader_rewind();
	debug_query_string = NULL;
	MyStatCQEntry = NULL;
}

void
ContExecutorAbortQuery(ContExecutor *exec)
{
	if (exec->lock)
		ReleaseContExecutionLock(exec->lock);
	AbortCurrentTransaction();
	StartTransactionCommand();
	exec->lock = AcquireContExecutionLock(AccessShareLock);
}

void
ContExecutorEndBatch(ContExecutor *exec, bool commit)
{
	Assert(IsTransactionState());

	if (commit)
	{
		exec_commit(exec);
		MemoryContextReset(ContQueryTransactionContext);
		pgstat_report_stat(false);
		exec->lock = NULL;
	}

	if (exec->batch)
	{
		pgstat_end_cq_batch(exec->batch->ntups, exec->batch->nbytes);

		if (exec->ptype == Worker)
		{
			ListCell *lc;

			foreach(lc, exec->batch->flush_acks)
			{
				tagged_ref_t *ref = lfirst(lc);
				microbatch_ack_t *ack = (microbatch_ack_t *) ref->ptr;
				microbatch_t *mb;
				int i;

				if (!microbatch_ack_ref_is_valid(ref))
					continue;

				mb = microbatch_new(FlushTuple, NULL, NULL);
				microbatch_add_ack(mb, ack);

				for (i = 0; i < continuous_query_num_combiners; i++)
					microbatch_send_to_combiner(mb, i);

				microbatch_destroy(mb);
			}

			microbatch_acks_check_and_exec(exec->batch->flush_acks, microbatch_ack_increment_ctups,
					continuous_query_num_combiners);
		}
	}

	ipc_tuple_reader_ack();
	ipc_tuple_reader_reset();

	MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
	MemoryContextResetAndDeleteChildren(ErrorContext);
	exec->exec_queries = NULL;
	exec->batch = NULL;

	debug_query_string = NULL;
	MyStatCQEntry = NULL;
}

/*
 * AcquireContExecutionLock
 */
ContExecutionLock
AcquireContExecutionLock(LOCKMODE mode)
{
	Assert(OidIsValid(PipelineExecLockRelationOid));

	return heap_open(PipelineExecLockRelationOid, mode);
}

/*
 * ReleaseContExecutionLock
 */
void
ReleaseContExecutionLock(ContExecutionLock lock)
{
	heap_close(lock, NoLock);
}
