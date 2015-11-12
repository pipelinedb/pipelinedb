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
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

static ResourceOwner WorkerResOwner = NULL;

typedef struct {
	ContQueryState base;
	DestReceiver *dest;
	QueryDesc *query_desc;
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

static ContQueryState *
init_query_state(ContExecutor *exec, ContQueryState *base)
{
	PlannedStmt *pstmt;
	ContQueryWorkerState *state;
	ResourceOwner res;

	res = CurrentResourceOwner;
	CurrentResourceOwner = WorkerResOwner;

	state = (ContQueryWorkerState *) palloc0(sizeof(ContQueryWorkerState));
	memcpy(&state->base, base, sizeof(ContQueryState));
	pfree(base);
	base = (ContQueryState *) state;

	state->dest = CreateDestReceiver(DestCombiner);

	SetCombinerDestReceiverParams(state->dest, exec);

	pstmt = GetContPlan(base->view, Worker);
	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot, InvalidSnapshot, state->dest, NULL, 0);
	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;

	RegisterSnapshotOnOwner(state->query_desc->snapshot, WorkerResOwner);

	ExecutorStart(state->query_desc, EXEC_NO_STREAM_LOCKING);

	state->query_desc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(state->query_desc->snapshot, WorkerResOwner);
	UnregisterSnapshotFromOwner(state->query_desc->estate->es_snapshot, WorkerResOwner);
	state->query_desc->snapshot = NULL;

	if (IsA(state->query_desc->plannedstmt->planTree, Agg))
	{
		Agg *agg = (Agg *) state->query_desc->plannedstmt->planTree;
		Relation matrel;
		ResultRelInfo *ri;

		state->groupatts = agg->grpColIdx;

		matrel = heap_openrv_extended(base->view->matrel, NoLock, true);

		if (matrel == NULL)
		{
			base->view = NULL;
			return base;
		}

		ri = CQMatRelOpen(matrel);

		if (agg->numCols)
		{
			FuncExpr *hash = GetGroupHashIndexExpr(agg->numCols, ri);
			if (hash == NULL)
			{
				/* matrel has been dropped */
				base->view = NULL;
				CQMatRelClose(ri);
				heap_close(matrel, NoLock);
				return base;
			}

			SetCombinerDestReceiverHashFunc(state->dest, hash);
		}

		CQMatRelClose(ri);
		heap_close(matrel, NoLock);
	}

	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, state->query_desc->operation, state->query_desc->tupDesc);

	/*
	 * The main loop initializes and ends plans across plan executions, so it expects
	 * the plan to be uninitialized
	 */
	ExecEndNode(state->query_desc->planstate);
	FreeExecutorState(state->query_desc->estate);

	CurrentResourceOwner = res;

	return base;
}

void
ContinuousQueryWorkerMain(void)
{
	ContExecutor *cont_exec = ContExecutorNew(Worker, &init_query_state);
	Oid query_id;

	WorkerResOwner = ResourceOwnerCreate(NULL, "WorkerResOwner");

	/* Workers never perform any writes, so only need read only transactions. */
	XactReadOnly = true;

	for (;;)
	{
		ContExecutorStartBatch(cont_exec);

		if (ShouldTerminateContQueryProcess())
			break;

		while ((query_id = ContExecutorStartNextQuery(cont_exec)) != InvalidOid)
		{
			Plan *plan = NULL;
			EState *estate = NULL;
			ContQueryWorkerState *state = (ContQueryWorkerState *) cont_exec->current_query;

			PG_TRY();
			{
				if (state == NULL)
					goto next;

				CHECK_FOR_INTERRUPTS();

				MemoryContextSwitchTo(state->base.tmp_cxt);
				state->query_desc->estate = estate = CreateEState(state->query_desc);

				SetEStateSnapshot(estate);
				CurrentResourceOwner = WorkerResOwner;

				/* initialize the plan for execution within this xact */
				plan = state->query_desc->plannedstmt->planTree;
				state->query_desc->planstate = ExecInitNode(plan, state->query_desc->estate, EXEC_NO_STREAM_LOCKING);
				set_cont_executor(state->query_desc->planstate, cont_exec);

				ExecutePlan(estate, state->query_desc->planstate, state->query_desc->operation,
						true, 0, 0, ForwardScanDirection, state->dest);

				/* free up any resources used by this plan before committing */
				ExecEndNode(state->query_desc->planstate);
				state->query_desc->planstate = NULL;

				MemoryContextResetAndDeleteChildren(state->base.tmp_cxt);
				MemoryContextSwitchTo(state->base.state_cxt);

				UnsetEStateSnapshot(estate);
				state->query_desc->estate = estate = NULL;
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (estate && ActiveSnapshotSet())
					UnsetEStateSnapshot(estate);

				ContExecutorPurgeQuery(cont_exec);

				if (!continuous_query_crash_recovery)
					exit(1);

				AbortCurrentTransaction();
				StartTransactionCommand();

				MemoryContextSwitchTo(cont_exec->exec_cxt);
			}
			PG_END_TRY();

next:
			ContExecutorEndQuery(cont_exec);
		}

		ContExecutorEndBatch(cont_exec);
	}

	StartTransactionCommand();

	for (query_id = 0; query_id < MAX_CQS; query_id++)
	{
		ContQueryWorkerState *state = (ContQueryWorkerState *) cont_exec->states[query_id];
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
			RegisterSnapshotOnOwner(estate->es_snapshot, WorkerResOwner);
			RegisterSnapshotOnOwner(query_desc->snapshot, WorkerResOwner);

			CurrentResourceOwner = WorkerResOwner;

			if (query_desc->totaltime)
				InstrStopNode(query_desc->totaltime, estate->es_processed);

			if (query_desc->planstate == NULL)
				query_desc->planstate = ExecInitNode(query_desc->plannedstmt->planTree, state->query_desc->estate, EXEC_NO_STREAM_LOCKING);

			/* Clean up. */
			ExecutorFinish(query_desc);
			ExecutorEnd(query_desc);
			FreeQueryDesc(query_desc);

			MyCQStats = &state->base.stats;
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
