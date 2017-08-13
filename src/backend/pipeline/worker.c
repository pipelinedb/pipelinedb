/*-------------------------------------------------------------------------
 *
 * worker.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/worker.c
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
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pipeline/combiner_receiver.h"
#include "pipeline/executor.h"
#include "pipeline/planner.h"
#include "pipeline/scheduler.h"
#include "pipeline/matrel.h"
#include "pipeline/stream_fdw.h"
#include "pipeline/transform_receiver.h"
#include "storage/ipc.h"
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
	BatchReceiver *receiver;
	QueryDesc *query_desc;
	AttrNumber *groupatts;
	FuncExpr *hashfunc;
	Tuplestorestate *plan_output;
	TupleTableSlot *result_slot;
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

	/* Each plan execution's output on a microbatch is buffered in a tuplestore */
	state->dest = CreateDestReceiver(DestTuplestore);
	state->plan_output = tuplestore_begin_heap(false, false, continuous_query_batch_mem);
	SetTuplestoreDestReceiverParams(state->dest, state->plan_output, CurrentMemoryContext, false);

	/*
	 * Now create the receivers, which send the buffered plan output down the processing pipeline
	 */
	if (base->query->type == CONT_VIEW)
		state->receiver = CreateCombinerReceiver(exec, base->query, state->plan_output);
	else
		state->receiver = CreateTransformReceiver(exec, base->query, state->plan_output);

	pstmt = GetContPlan(base->query, Worker);
	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot, InvalidSnapshot, state->dest, NULL, 0);
	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;

	RegisterSnapshotOnOwner(state->query_desc->snapshot, WorkerResOwner);

	ExecutorStart(state->query_desc, EXEC_NO_STREAM_LOCKING | PIPELINE_EXEC_CONTINUOUS);

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

		matrel = heap_openrv_extended(base->query->matrel, NoLock, true);

		if (matrel == NULL)
		{
			base->query = NULL;
			return base;
		}

		ri = CQMatRelOpen(matrel);

		if (agg->numCols)
		{
			FuncExpr *hash = GetGroupHashIndexExpr(ri);
			if (hash == NULL)
			{
				/* matrel has been dropped */
				base->query = NULL;
				CQMatRelClose(ri);
				heap_close(matrel, NoLock);
				return base;
			}

			Assert(base->query->type == CONT_VIEW);
			SetCombinerDestReceiverHashFunc(state->receiver, hash);
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

static void
flush_tuples(ContQueryWorkerState *state)
{
	state->receiver->flush(state->receiver, state->result_slot);
}

/*
 * init_plan
 */
static void
init_plan(ContQueryWorkerState *state)
{
	QueryDesc *query_desc = state->query_desc;
	Plan *plan = query_desc->plannedstmt->planTree;
	ListCell *lc;

	foreach(lc, query_desc->plannedstmt->subplans)
	{
		Plan *subplan = (Plan *) lfirst(lc);
		PlanState  *subplanstate;

		subplanstate = ExecInitNode(subplan, query_desc->estate, EXEC_NO_STREAM_LOCKING | PIPELINE_EXEC_CONTINUOUS);
		query_desc->estate->es_subplanstates = lappend(query_desc->estate->es_subplanstates, subplanstate);
	}

	query_desc->planstate = ExecInitNode(plan, query_desc->estate, EXEC_NO_STREAM_LOCKING | PIPELINE_EXEC_CONTINUOUS);
	query_desc->tupDesc = ExecGetResultType(query_desc->planstate);

	state->result_slot = MakeSingleTupleTableSlot(query_desc->tupDesc);
	tuplestore_clear(state->plan_output);
}

/*
 * end_plan
 */
static void
end_plan(QueryDesc *query_desc)
{
	ListCell *lc;

	ExecEndNode(query_desc->planstate);
	foreach(lc, query_desc->estate->es_subplanstates)
	{
		PlanState *ps = (PlanState *) lfirst(lc);
		ExecEndNode(ps);
	}

	query_desc->planstate = NULL;
}

static void
cleanup_worker_state(ContQueryWorkerState *state)
{
	QueryDesc *query_desc;
	volatile EState *estate = NULL;

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
		{
			estate = CreateEState(state->query_desc);
			query_desc->estate = (EState *) estate;
			SetEStateSnapshot((EState *) estate);
		}

		/* The cleanup functions below expect these things to be registered. */
		RegisterSnapshotOnOwner(estate->es_snapshot, WorkerResOwner);
		RegisterSnapshotOnOwner(query_desc->snapshot, WorkerResOwner);

		CurrentResourceOwner = WorkerResOwner;

		if (query_desc->totaltime)
			InstrStopNode(query_desc->totaltime, estate->es_processed);

		if (query_desc->planstate == NULL)
			init_plan(state);

		/* Clean up. */
		ExecutorFinish(query_desc);
		ExecutorEnd(query_desc);
		UnsetEStateSnapshot((EState *) estate);

		FreeQueryDesc(query_desc);
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

		if (estate && ActiveSnapshotSet())
			UnsetEStateSnapshot((EState *) estate);
	}
	PG_END_TRY();
}

static bool
should_exec_query(ContQuery *query)
{
	Bitmapset *readers;

	if (query->type != CONT_TRANSFORM)
		return true;

	/*
	 * If it's a transform with no trigger function and no output stream readers,
	 * it's a noop
	 */
	readers = GetAllStreamReaders(query->osrelid);

	if (!OidIsValid(query->tgfn) && bms_is_empty(readers))
		return false;

	return true;
}

void
ContinuousQueryWorkerMain(void)
{
	ContExecutor *cont_exec = ContExecutorNew(&init_query_state);
	Oid query_id;

	WorkerResOwner = ResourceOwnerCreate(NULL, "WorkerResOwner");

	/* Workers never perform any writes, so only need read only transactions. */
	XactReadOnly = true;

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		if (get_sigterm_flag())
			break;

		ContExecutorStartBatch(cont_exec, 0);

		while ((query_id = ContExecutorStartNextQuery(cont_exec, 0)) != InvalidOid)
		{
			volatile EState *estate = NULL;
			ContQueryWorkerState *state = (ContQueryWorkerState *) cont_exec->curr_query;
			volatile bool error = false;

			PG_TRY();
			{
				if (state == NULL)
					goto next;

				MemoryContextSwitchTo(state->base.tmp_cxt);

				estate = CreateEState(state->query_desc);
				state->query_desc->estate = (EState *) estate;
				SetEStateSnapshot((EState *) estate);

				CurrentResourceOwner = WorkerResOwner;

				if (should_exec_query(state->base.query))
				{
					TimestampTz start_time = GetCurrentTimestamp();
					long secs;
					int usecs;

					/* initialize the plan for execution within this xact */
					init_plan(state);
					set_cont_executor(state->query_desc->planstate, cont_exec);

					ExecutePlan((EState *) estate, state->query_desc->planstate, state->query_desc->operation,
							true, 0, ForwardScanDirection, state->dest);

					/* free up any resources used by this plan before committing */
					end_plan(state->query_desc);

					/* flush tuples to combiners or transform out functions */
					flush_tuples(state);

					/* record execution time */
					TimestampDifference(start_time, GetCurrentTimestamp(), &secs, &usecs);
					pgstat_increment_cq_exec_time(secs * 1000 + (usecs / 1000));
				}

				UnsetEStateSnapshot((EState *) estate);
				state->query_desc->estate = NULL;
				estate = NULL;

				MemoryContextResetAndDeleteChildren(state->base.tmp_cxt);
				MemoryContextSwitchTo(state->base.state_cxt);
				ResourceOwnerRelease(WorkerResOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, true);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (estate && ActiveSnapshotSet())
					UnsetEStateSnapshot((EState *) estate);

				AbortCurrentTransaction();
				StartTransactionCommand();

				/*
				 * Modifying anything within a PG_CATCH block can have unpredictable behavior
				 * when optimization is enabled, so we do the remaining error handling later.
				 */
				error = true;
			}
			PG_END_TRY();

			if (error)
				pgstat_increment_cq_error(1);

next:
			ContExecutorEndQuery(cont_exec);

			/*
			 * We wait to purge until we're done incrementing all stats, because this will
			 * free the stats object
			 */
			if (error)
				ContExecutorPurgeQuery(cont_exec);
		}

		ContExecutorEndBatch(cont_exec, true);
	}

	StartTransactionCommand();

	for (query_id = 0; query_id < MAX_CQS; query_id++)
	{
		ContQueryWorkerState *state = (ContQueryWorkerState *) cont_exec->states[query_id];

		if (state == NULL)
			continue;

		MyStatCQEntry = (PgStat_StatCQEntry *) &state->base.stats;

		cleanup_worker_state(state);

		pgstat_report_cqstat(true);
		MemoryContextDelete(state->base.state_cxt);
	}

	CommitTransactionCommand();

	MemoryContextSwitchTo(TopMemoryContext);
	ContExecutorDestroy(cont_exec);
}
