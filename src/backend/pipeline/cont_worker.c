/*-------------------------------------------------------------------------
 *
 * cont_worker.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_plan.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/stream_fdw.h"
#include "pipeline/tuplebuf.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"

typedef struct {
	Oid view_id;
	ContinuousView *view;
	DestReceiver *dest;
	QueryDesc *query_desc;
	MemoryContext state_cxt;
	MemoryContext tmp_cxt;
	CQStatEntry stats;
	AttrNumber *groupatts;
	FuncExpr *hashfunc;
} ContQueryWorkerState;

static void
set_cont_executor(PlanState *planstate, ContExecutor *exec)
{
	if (planstate == NULL)
		return;

	if (IsA(planstate, ForeignScanState))
	{
		ForeignScanState *fss = (ForeignScanState *) planstate;
		if (IsStream(RelationGetRelid(fss->ss.ss_currentRelation)))
		{
			StreamScanState *scan = (StreamScanState *) fss->fdw_state;
			scan->cont_executor = exec;
		}

		return;
	}
	else if (IsA(planstate, SubqueryScanState))
	{
		set_cont_executor(((SubqueryScanState *) planstate)->subplan, exec);
		return;
	}

	set_cont_executor(planstate->lefttree, exec);
	set_cont_executor(planstate->righttree, exec);
}

static void
init_query_state(ContQueryWorkerState *state, ContExecutor *exec, ResourceOwner owner)
{
	PlannedStmt *pstmt;
	MemoryContext state_cxt;
	MemoryContext old_cxt;

	MemSet(state, 0, sizeof(ContQueryWorkerState));

	state_cxt = AllocSetContextCreate(exec->cxt, "WorkerQueryStateCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old_cxt = MemoryContextSwitchTo(state_cxt);

	state->view_id = exec->cur_query_id;
	state->state_cxt = state_cxt;
	state->view = GetContinuousView(exec->cur_query_id);
	state->tmp_cxt = AllocSetContextCreate(state_cxt, "WorkerQueryTmpCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	if (state->view == NULL)
		return;

	state->dest = CreateDestReceiver(DestCombiner);

	SetCombinerDestReceiverParams(state->dest, exec);

	pstmt = GetContPlan(state->view, Worker);
	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot, InvalidSnapshot, state->dest, NULL, 0);
	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;

	RegisterSnapshotOnOwner(state->query_desc->snapshot, owner);

	ExecutorStart(state->query_desc, EXEC_NO_STREAM_LOCKING);

	state->query_desc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(state->query_desc->snapshot, owner);
	UnregisterSnapshotFromOwner(state->query_desc->estate->es_snapshot, owner);
	state->query_desc->snapshot = NULL;

	if (IsA(state->query_desc->plannedstmt->planTree, Agg))
	{
		Agg *agg = (Agg *) state->query_desc->plannedstmt->planTree;
		Relation matrel;
		ResultRelInfo *ri;

		state->groupatts = agg->grpColIdx;

		matrel = heap_openrv_extended(state->view->matrel, NoLock, true);

		if (matrel == NULL)
		{
			state->view = NULL;
			return;
		}

		ri = CQMatRelOpen(matrel);

		if (agg->numCols)
		{
			FuncExpr *hash = GetGroupHashIndexExpr(agg->numCols, ri);
			if (hash == NULL)
			{
				/* matrel has been dropped */
				state->view = NULL;
				CQMatRelClose(ri);
				heap_close(matrel, NoLock);
				return;
			}

			SetCombinerDestReceiverHashFunc(state->dest, hash);
		}

		CQMatRelClose(ri);
		heap_close(matrel, NoLock);
	}

	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, state->query_desc->operation, state->query_desc->tupDesc);

	cq_stat_init(&state->stats, state->view->id, 0);

	/*
	 * The main loop initializes and ends plans across plan executions, so it expects
	 * the plan to be uninitialized
	 */
	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	MemoryContextSwitchTo(old_cxt);
}

static void
cleanup_query_state(ContQueryWorkerState **states, Oid id)
{
	ContQueryWorkerState *state = states[id];

	if (state == NULL)
		return;

	MemoryContextDelete(state->state_cxt);
	pfree(state);
	states[id] = NULL;
}

static ContQueryWorkerState **
init_query_states_array(MemoryContext context)
{
	MemoryContext old_cxt = MemoryContextSwitchTo(context);
	ContQueryWorkerState **states = palloc0(MAXALIGN(mul_size(sizeof(ContQueryWorkerState *), MAX_CQS)));
	MemoryContextSwitchTo(old_cxt);

	return states;
}

static ContQueryWorkerState *
get_query_state(ContQueryWorkerState **states, ContExecutor *exec, ResourceOwner owner)
{
	ContQueryWorkerState *state = states[exec->cur_query_id];
	HeapTuple tuple;
	ResourceOwner old_owner;

	MyCQStats = NULL;

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	old_owner = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	PushActiveSnapshot(GetTransactionSnapshot());

	tuple = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(exec->cur_query_id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tuple))
	{
		PopActiveSnapshot();
		cleanup_query_state(states, exec->cur_query_id);
		return NULL;
	}

	if (state != NULL)
	{
		/* Was the continuous view modified? In our case this means remove the old view and add a new one */
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		if (row->hash != state->view->hash)
		{
			cleanup_query_state(states, exec->cur_query_id);
			state = NULL;
		}
	}

	ReleaseSysCache(tuple);

	if (state == NULL)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(exec->cxt);
		state = palloc0(sizeof(ContQueryWorkerState));
		init_query_state(state, exec, owner);
		states[exec->cur_query_id] = state;
		MemoryContextSwitchTo(old_cxt);

		if (state->view == NULL)
		{
			PopActiveSnapshot();
			cleanup_query_state(states, exec->cur_query_id);
			return NULL;
		}
	}

	PopActiveSnapshot();

	CurrentResourceOwner = old_owner;

	MyCQStats = &state->stats;

	return state;
}

void
ContinuousQueryWorkerMain(void)
{
	ResourceOwner owner = ResourceOwnerCreate(NULL, "WorkerResourceOwner");
	ContQueryWorkerState **states;
	ContQueryWorkerState *state;
	ContExecutor *cont_exec = ContExecutorNew(Worker);
	Oid query_id;

	/* Workers never perform any writes, so only need read only transactions. */
	XactReadOnly = true;

	states = init_query_states_array(cont_exec->cxt);

	for (;;)
	{
		ContExecutorStartBatch(cont_exec);

		if (ShouldTerminateContQueryProcess())
			break;

		while ((query_id = ContExecutorStartNextQuery(cont_exec)) != InvalidOid)
		{
			Plan *plan = NULL;
			EState *estate = NULL;

			PG_TRY();
			{
				state = get_query_state(states, cont_exec, owner);

				if (state == NULL)
				{
					ContExecutorPurgeQuery(cont_exec);
					goto next;
				}

				CHECK_FOR_INTERRUPTS();

				debug_query_string = NameStr(state->view->name);
				MemoryContextSwitchTo(state->tmp_cxt);
				state->query_desc->estate = estate = CreateEState(state->query_desc);

				SetEStateSnapshot(estate);
				CurrentResourceOwner = owner;

				/* initialize the plan for execution within this xact */
				plan = state->query_desc->plannedstmt->planTree;
				state->query_desc->planstate = ExecInitNode(plan, state->query_desc->estate, EXEC_NO_STREAM_LOCKING);
				set_cont_executor(state->query_desc->planstate, cont_exec);

				ExecutePlan(estate, state->query_desc->planstate, state->query_desc->operation,
						true, 0, 0, ForwardScanDirection, state->dest);

				/* free up any resources used by this plan before committing */
				ExecEndNode(state->query_desc->planstate);
				state->query_desc->planstate = NULL;

				MemoryContextResetAndDeleteChildren(state->tmp_cxt);
				MemoryContextSwitchTo(state->state_cxt);

				UnsetEStateSnapshot(estate);
				state->query_desc->estate = estate = NULL;
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (estate && ActiveSnapshotSet())
					UnsetEStateSnapshot(estate);

				if (state)
					cleanup_query_state(states, query_id);

				IncrementCQErrors(1);

				if (!continuous_query_crash_recovery)
					exit(1);

				AbortCurrentTransaction();
				StartTransactionCommand();

				MemoryContextSwitchTo(cont_exec->exec_cxt);
			}
			PG_END_TRY();

next:
			ContExecutorEndQuery(cont_exec);

			if (state)
				cq_stat_report(false);
			else
				cq_stat_send_purge(query_id, 0, CQ_STAT_WORKER);

			debug_query_string = NULL;
		}

		ContExecutorEndBatch(cont_exec);
	}

	StartTransactionCommand();

	for (query_id = 0; query_id < MAX_CQS; query_id++)
	{
		ContQueryWorkerState *state = states[query_id];
		QueryDesc *query_desc;
		EState *estate;

		if (state == NULL)
			continue;

		/*
		 * We wrap this in a separate try/catch block because ExecInitNode call can potentially throw
		 * an error if the state was for a stream-table join and the table has been dropped.
		 */
		PG_TRY();
		{
			query_desc = state->query_desc;
			estate = query_desc->estate;

			(*state->dest->rShutdown) (state->dest);

			if (estate == NULL)
				query_desc->estate = estate = CreateEState(state->query_desc);

			/* The cleanup functions below expect these things to be registered. */
			RegisterSnapshotOnOwner(estate->es_snapshot, owner);
			RegisterSnapshotOnOwner(query_desc->snapshot, owner);

			CurrentResourceOwner = owner;

			if (query_desc->totaltime)
				InstrStopNode(query_desc->totaltime, estate->es_processed);

			if (query_desc->planstate == NULL)
				query_desc->planstate = ExecInitNode(query_desc->plannedstmt->planTree, state->query_desc->estate, EXEC_NO_STREAM_LOCKING);

			/* Clean up. */
			ExecutorFinish(query_desc);
			ExecutorEnd(query_desc);
			FreeQueryDesc(query_desc);

			MyCQStats = &state->stats;
			cq_stat_report(true);
		}
		PG_CATCH();
		{
			/*
			 * If this happens, it almost certainly means that a stream or table has been dropped
			 * and no longer exists, even though the ended plan may have references to them. We're
			 * not doing anything particularly critical in the above TRY block, so just consume these
			 * harmless errors.
			 */
			FlushErrorState();
		}
		PG_END_TRY();
	}

	CommitTransactionCommand();

	MemoryContextSwitchTo(TopMemoryContext);
	ContExecutorDestroy(cont_exec);
}
