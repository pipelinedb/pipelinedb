/*-------------------------------------------------------------------------
 *
 * cont_worker.c
 *
 * IDENTIFICATION
 *    src/include/pipeline/cont_worker.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/cqplan.h"
#include "pipeline/tuplebuf.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "tcop/dest.h"

static bool
should_read_fn(TupleBufferReader *reader, TupleBufferSlot *slot)
{
	return slot->id % continuous_query_num_workers == reader->proc->group_id;
}

typedef struct {
	Oid view_id;
	ContinuousView *view;
	DestReceiver *dest;
	QueryDesc *query_desc;
	MemoryContext exec_cxt;
	CQStatEntry stats;
} ContQueryWorkerState;

/*
 * set_reader
 *
 * Sets the TupleBufferBatchReader for any StreamScanState nodes in the PlanState.
 */
static void
set_reader(PlanState *planstate, TupleBufferBatchReader *reader)
{
	if (planstate == NULL)
		return;

	if (IsA(planstate, StreamScanState))
	{
		StreamScanState *scan = (StreamScanState *) planstate;
		scan->reader = reader;
		return;
	}

	set_reader(planstate->lefttree, reader);
	set_reader(planstate->righttree, reader);
}

static void
init_query_state(ContQueryWorkerState *state, Oid id, MemoryContext context, ResourceOwner owner, TupleBufferBatchReader *reader)
{
	PlannedStmt *pstmt;
	MemoryContext exec_cxt;
	MemoryContext old_cxt;
	ResourceOwner old_owner;

	MemSet(state, 0, sizeof(ContQueryWorkerState));

	exec_cxt = AllocSetContextCreate(context, "WorkerQueryExecCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old_cxt = MemoryContextSwitchTo(exec_cxt);
	old_owner = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	state->view_id = id;

	state->exec_cxt = exec_cxt;
	state->view = GetContinuousView(id);

	state->dest = CreateDestReceiver(DestCombiner);
	SetCombinerDestReceiverParams(state->dest, reader, id);

	pstmt = GetCQPlan(id, state->view->query, NameStr(state->view->matrelname));
	state->query_desc = CreateQueryDesc(pstmt, NULL, InvalidSnapshot, InvalidSnapshot, state->dest, NULL, 0);
	state->query_desc->snapshot = GetTransactionSnapshot();
	state->query_desc->snapshot->copied = true;

	RegisterSnapshotOnOwner(state->query_desc->snapshot, owner);

	ExecutorStart(state->query_desc, 0);

	set_reader(state->query_desc->planstate, reader);

	state->query_desc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(state->query_desc->snapshot, owner);
	UnregisterSnapshotFromOwner(state->query_desc->estate->es_snapshot, owner);

	state->query_desc->estate->es_lastoid = InvalidOid;

	(*state->dest->rStartup) (state->dest, state->query_desc->operation, state->query_desc->tupDesc);

	CurrentResourceOwner = old_owner;

	cq_stat_init(&state->stats, state->view->id, 0);

	MemoryContextSwitchTo(old_cxt);
}

static void
cleanup_query_state(ContQueryWorkerState **states, Oid id)
{
	ContQueryWorkerState *state = states[id];

	if (state == NULL)
		return;

	MemoryContextDelete(state->exec_cxt);
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
get_query_state(ContQueryWorkerState **states, Oid id, MemoryContext context, ResourceOwner owner, TupleBufferBatchReader *reader)
{
	ContQueryWorkerState *state = states[id];
	HeapTuple tuple;
	Datum tmp;
	bool isnull;

	/* Entry missing? Start a new transaction so we read the latest pipeline_query catalog. */
	if (state == NULL)
	{
		CommitTransactionCommand();
		StartTransactionCommand();
	}

	tuple = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(id));

	/* Was the continuous view removed? */
	if (!HeapTupleIsValid(tuple))
	{
		cleanup_query_state(states, id);
		return NULL;
	}

	if (state != NULL)
	{
		/* Was the continuous view modified? In our case this means remove the old view and add a new one */
		tmp = SysCacheGetAttr(PIPELINEQUERYNAME, tuple, Anum_pipeline_query_query, &isnull);
		if (strcmp(state->view->query, TextDatumGetCString(tmp)) != 0)
		{
			cleanup_query_state(states, id);
			state = NULL;
		}
	}

	ReleaseSysCache(tuple);

	if (state == NULL)
	{
		MemoryContext old_cxt = MemoryContextSwitchTo(context);
		state = palloc0(sizeof(ContQueryWorkerState));
		init_query_state(state, id, context, owner, reader);
		states[id] = state;
		MemoryContextSwitchTo(old_cxt);
	}


	MyCQStats = &state->stats;

	return state;
}

static void
set_snapshot(EState *estate, ResourceOwner owner)
{
	estate->es_snapshot = GetTransactionSnapshot();
	estate->es_snapshot->active_count++;
	estate->es_snapshot->copied = true;
	RegisterSnapshotOnOwner(estate->es_snapshot, owner);
	PushActiveSnapshot(estate->es_snapshot);
}

static void
unset_snapshot(EState *estate, ResourceOwner owner)
{
	PopActiveSnapshot();
	UnregisterSnapshotFromOwner(estate->es_snapshot, owner);
}

static bool
has_queries_to_process(Bitmapset *queries)
{
	return !bms_is_empty(queries);
}

void
ContinuousQueryWorkerMain(void)
{
	TupleBufferBatchReader *reader = TupleBufferOpenBatchReader(WorkerTupleBuffer, &should_read_fn);
	ResourceOwner owner = ResourceOwnerCreate(NULL, "WorkerResourceOwner");
	MemoryContext run_cxt = AllocSetContextCreate(TopMemoryContext, "WorkerRunCxt",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	ContQueryWorkerState **states = init_query_states_array(run_cxt);
	ContQueryWorkerState *state;
	Bitmapset *queries;
	TimestampTz last_processed = GetCurrentTimestamp();
	bool has_queries;
	int id;

	/* Workers never perform any writes, so only need read only transactions. */
	XactReadOnly = true;

	ContQueryBatchContext = AllocSetContextCreate(run_cxt, "ContQueryBatchContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	/* Bootstrap the query ids we should process. */
	StartTransactionCommand();
	MemoryContextSwitchTo(run_cxt);
	queries = GetAllContinuousViewIds();
	CommitTransactionCommand();

	has_queries = has_queries_to_process(queries);

	MemoryContextSwitchTo(run_cxt);

	for (;;)
	{
		uint32 num_processed = 0;
		Bitmapset *tmp;
		bool updated_queries = false;

		sleep_if_cqs_deactivated();

		TupleBufferBatchReaderTrySleep(reader, last_processed);

		if (MyContQueryProc->group->terminate)
			break;

		/* If we had no queries, then rescan the catalog. */
		if (!has_queries)
		{
			Bitmapset *new, *removed;

			StartTransactionCommand();
			MemoryContextSwitchTo(run_cxt);
			new = GetAllContinuousViewIds();
			CommitTransactionCommand();

			removed = bms_difference(queries, new);
			bms_free(queries);
			queries = new;

			while ((id = bms_first_member(removed)) >= 0)
				cleanup_query_state(states, id);

			bms_free(removed);

			has_queries = has_queries_to_process(queries);
		}

		StartTransactionCommand();

		MemoryContextSwitchTo(ContQueryBatchContext);

		tmp = bms_copy(queries);
		while ((id = bms_first_member(tmp)) >= 0)
		{
			QueryDesc *query_desc;
			EState *estate;

			PG_TRY();
			{
				state = get_query_state(states, id, run_cxt, owner, reader);

				if (state == NULL)
				{
					queries = bms_del_member(queries, id);
					has_queries = has_queries_to_process(queries);
					goto next;
				}

				query_desc = state->query_desc;
				estate = query_desc->estate;

				/* No need to process queries which we don't have tuples for. */
				if (!TupleBufferBatchReaderHasTuplesForCQId(reader, id))
					goto next;

				MemoryContextSwitchTo(state->exec_cxt);

				TupleBufferBatchReaderSetCQId(reader, id);

				set_snapshot(estate, owner);
				CurrentResourceOwner = owner;

				estate->es_processed = 0;
				estate->es_filtered = 0;

				/*
				 * We pass a timeout of 0 because the underlying TupleBufferBatchReader takes care of
				 * waiting for enough to read tuples from the TupleBuffer.
				 */
				ExecutePlan(estate, state->query_desc->planstate, state->query_desc->operation,
						true, 0, 0, ForwardScanDirection, state->dest);

				num_processed += estate->es_processed;
				num_processed += estate->es_filtered;

				MemoryContextSwitchTo(state->exec_cxt);
				CurrentResourceOwner = owner;
				unset_snapshot(estate, owner);

				TupleBufferBatchReaderRewind(reader);

				IncrementCQExecutions(1);
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();

				if (ActiveSnapshotSet())
					unset_snapshot(estate, owner);

				cleanup_query_state(states, state->view_id);

				TupleBufferBatchReaderRewind(reader);

				IncrementCQErrors(1);

				if (!continuous_query_crash_recovery)
					exit(1);
			}
			PG_END_TRY();

next:
			/* after reading a full batch, update query bitset with any new queries seen */
			if (reader->batch_done && !updated_queries)
			{
				Bitmapset *new;

				updated_queries = true;

				new = bms_difference(reader->queries_seen, queries);

				if (!bms_is_empty(new))
				{
					MemoryContextSwitchTo(ContQueryBatchContext);
					tmp = bms_add_members(tmp, new);

					MemoryContextSwitchTo(run_cxt);
					queries = bms_add_members(queries, new);

					has_queries = has_queries_to_process(queries);
				}

				bms_free(new);
			}

			if (state)
				cq_stat_report(false);
			else
				cq_stat_send_purge(id, 0, CQ_STAT_WORKER);
		}

		CommitTransactionCommand();

		if (num_processed)
			last_processed = GetCurrentTimestamp();

		TupleBufferBatchReaderReset(reader);
		MemoryContextResetAndDeleteChildren(ContQueryBatchContext);
	}

	for (id = 0; id < MAX_CQS; id++)
	{
		ContQueryWorkerState *state = states[id];
		QueryDesc *query_desc;
		EState *estate;

		if (state == NULL)
			continue;

		query_desc = state->query_desc;
		estate = query_desc->estate;

		(*state->dest->rShutdown) (state->dest);

		/* The cleanup functions below expect these things to be registered. */
		RegisterSnapshotOnOwner(estate->es_snapshot, owner);
		RegisterSnapshotOnOwner(query_desc->snapshot, owner);
		RegisterSnapshotOnOwner(query_desc->crosscheck_snapshot, owner);

		CurrentResourceOwner = owner;

		/* Clean up. */
		ExecutorFinish(query_desc);
		ExecutorEnd(query_desc);
		FreeQueryDesc(query_desc);

		if (query_desc->totaltime)
			InstrStopNode(query_desc->totaltime, estate->es_processed);

		MyCQStats = &state->stats;
		cq_stat_report(true);
	}

	TupleBufferCloseBatchReader(reader);
	pfree(states);
	MemoryContextDelete(run_cxt);
	MemoryContextSwitchTo(TopMemoryContext);
}
