/*-------------------------------------------------------------------------
 *
 * executor.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "executor.h"
#include "executor/executor.h"
#include "microbatch.h"
#include "miscutils.h"
#include "pgstat.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define MAX_IN_XACT_TIMEOUT 5 /* 5ms */
#define MAX_NOT_IN_XACT_TIMEOUT 3000 /* 3s */

Oid PipelineExecLockRelationOid;

MemoryContext ContQueryTransactionContext = NULL;
MemoryContext ContQueryBatchContext = NULL;

/*
 * AcquireContExecutionLock
 */
ContExecutionLock
AcquireContExecutionLock(LOCKMODE mode)
{
	if (IsBinaryUpgrade)
		return NULL;
	return heap_open(GetPipelineExecLockOid(), mode);
}

/*
 * ReleaseContExecutionLock
 */
void
ReleaseContExecutionLock(ContExecutionLock lock)
{
	heap_close(lock, NoLock);
}

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

/*
 * ContExecutorNew
 */
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
	MyProcStatCQEntry = NULL;

	return exec;
}

/*
 * ContExecutorDestroy
 */
void
ContExecutorDestroy(ContExecutor *exec)
{
	ipc_tuple_reader_destroy();
	MemoryContextDelete(exec->cxt);
}

/*
 * ContExecutorStartBatch
 */
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
				exec->all_queries = GetContViewIds();
			else
				exec->all_queries = GetContQueryIds();

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
}

/*
 * init_query_state
 */
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

	state = exec->initfn(exec, state);

	MemoryContextSwitchTo(old_cxt);

	return state;
}

/*
 * get_query_state
 */
static ContQueryState *
get_query_state(ContExecutor *exec)
{
	ContQueryState *state;
	HeapTuple tup;
	bool commit = false;

	MyProcStatCQEntry = NULL;
	state = exec->states[exec->curr_query_id];

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		exec_commit(exec);
		exec_begin(exec);
		commit = true;
	}

	PushActiveSnapshot(GetTransactionSnapshot());

	tup = PipelineCatalogLookup(PIPELINEQUERYID, 1, Int32GetDatum(exec->curr_query_id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tup))
	{
		PopActiveSnapshot();
		ContExecutorPurgeQuery(exec);
		return NULL;
	}

	if (state != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		/*
		 * Check if the catalog row still has the same relid as when we cached it with this query ID.
		 * Since query IDs are kept as small as possible, they'll be reused when dropping and creating
		 * new CQs. But the relid is just a regular OID so if it changed, the cache is stale.
		 */
		if (row->relid != state->query->relid)
		{
			MemoryContextDelete(state->state_cxt);
			exec->states[exec->curr_query_id] = NULL;
			state = NULL;
			commit = true;
		}
		else
		{
			state->query->active = row->active;
		}
	}

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

/*
 * ContExecutorStartNextQuery
 */
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
			MyProcStatCQEntry = exec->curr_query->stats;
		}
		else
			exec->curr_query = NULL;
	}

	return exec->curr_query_id;
}

/*
 * ContExecutorPurgeQuery
 */
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

/*
 * ContExecutorEndQuery
 */
void
ContExecutorEndQuery(ContExecutor *exec)
{
	if (!exec->batch)
		return;

	StatsIncrementCQExec(1);

	exec->curr_query_id = InvalidOid;
	ipc_tuple_reader_rewind();
	debug_query_string = NULL;
	MyProcStatCQEntry = NULL;
}

/*
 * ContExecutorAbortQuery
 */
void
ContExecutorAbortQuery(ContExecutor *exec)
{
	if (exec->lock)
		ReleaseContExecutionLock(exec->lock);
	AbortCurrentTransaction();
	StartTransactionCommand();
	exec->lock = AcquireContExecutionLock(AccessShareLock);
}

/*
 * ContExecutorEndBatch
 */
void
ContExecutorEndBatch(ContExecutor *exec, bool commit)
{
	Assert(IsTransactionState());

	if (commit)
	{
		exec_commit(exec);
		MemoryContextReset(ContQueryTransactionContext);
		exec->lock = NULL;
	}

	if (exec->batch)
	{
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

				for (i = 0; i < num_combiners; i++)
					microbatch_send_to_combiner(mb, i);

				microbatch_destroy(mb);
			}

			microbatch_acks_check_and_exec(exec->batch->flush_acks, microbatch_ack_increment_ctups, num_combiners);
		}
	}

	ipc_tuple_reader_ack();
	ipc_tuple_reader_reset();

	MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
	MemoryContextResetAndDeleteChildren(ErrorContext);
	exec->exec_queries = NULL;
	exec->batch = NULL;
	debug_query_string = NULL;
	MyProcStatCQEntry = NULL;
}

/*
 * ExecuteContPlan
 *
 * Slightly modified copy of PG's ExecutePlan, used for executing worker plans
 */
void
ExecuteContPlan(EState *estate,
			PlanState *planstate,
			bool use_parallel_mode,
			CmdType operation,
			bool sendTuples,
			uint64 numberTuples,
			ScanDirection direction,
			DestReceiver *dest,
			bool execute_once)
{
	TupleTableSlot *slot;
	uint64		current_tuple_count;

	/*
	 * initialize local variables
	 */
	current_tuple_count = 0;

	/*
	 * Set the direction.
	 */
	estate->es_direction = direction;

	/*
	 * If the plan might potentially be executed multiple times, we must force
	 * it to run without parallelism, because we might exit early.  Also
	 * disable parallelism when writing into a relation, because no database
	 * changes are allowed in parallel mode.
	 */
	if (!execute_once || dest->mydest == DestIntoRel)
		use_parallel_mode = false;

	estate->es_use_parallel_mode = use_parallel_mode;
	if (use_parallel_mode)
		EnterParallelMode();

	/*
	 * Loop until we've processed the proper number of tuples from the plan.
	 */
	for (;;)
	{
		/* Reset the per-output-tuple exprcontext */
		ResetPerTupleExprContext(estate);

		/*
		 * Execute the plan and obtain a tuple
		 */
		slot = ExecProcNode(planstate);

		/*
		 * if the tuple is null, then we assume there is nothing more to
		 * process so we just end the loop...
		 */
		if (TupIsNull(slot))
		{
			/* Allow nodes to release or shut down resources. */
			(void) ExecShutdownNode(planstate);
			break;
		}

		/*
		 * If we have a junk filter, then project a new tuple with the junk
		 * removed.
		 *
		 * Store this new "clean" tuple in the junkfilter's resultSlot.
		 * (Formerly, we stored it back over the "dirty" tuple, which is WRONG
		 * because that tuple slot has the wrong descriptor.)
		 */
		if (estate->es_junkFilter != NULL)
			slot = ExecFilterJunk(estate->es_junkFilter, slot);

		/*
		 * If we are supposed to send the tuple somewhere, do so. (In
		 * practice, this is probably always the case at this point.)
		 */
		if (sendTuples)
		{
			/*
			 * If we are not able to send the tuple, we assume the destination
			 * has closed and no more tuples can be sent. If that's the case,
			 * end the loop.
			 */
			if (!((*dest->receiveSlot) (slot, dest)))
				break;
		}

		/*
		 * Count tuples processed, if this is a SELECT.  (For other operation
		 * types, the ModifyTable plan node must count the appropriate
		 * events.)
		 */
		if (operation == CMD_SELECT)
			(estate->es_processed)++;

		/*
		 * check our tuple count.. if we've processed the proper number then
		 * quit, else loop again and process more tuples.  Zero numberTuples
		 * means no limit.
		 */
		current_tuple_count++;
		if (numberTuples && numberTuples == current_tuple_count)
		{
			/* Allow nodes to release or shut down resources. */
			(void) ExecShutdownNode(planstate);
			break;
		}
	}

	if (use_parallel_mode)
		ExitParallelMode();
}
