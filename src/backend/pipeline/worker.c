/*-------------------------------------------------------------------------
 *
 * worker.c
 *
 *	  Worker process functionality
 *
 * src/backend/pipeline/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <time.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_query.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pipeline/combiner.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/cqproc.h"
#include "pipeline/worker.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "storage/proc.h"
#include "pgstat.h"
#include "utils/timestamp.h"

#define COMBINER_WAIT_TIMEOUT (100 * 1000)

extern StreamBuffer *GlobalStreamBuffer;
extern int EmptyStreamBufferWaitTime;


/*
 * We keep some resources across transactions, so we attach everything to a
 * long-lived ResourceOwner, which prevents the below commit from thinking that
 * there are reference leaks
 */
static void
start_executor(QueryDesc *queryDesc, MemoryContext context, ResourceOwner owner)
{
	MemoryContext old;
	ResourceOwner save;

	StartTransactionCommand();

	old = MemoryContextSwitchTo(context);

	save = CurrentResourceOwner;
	CurrentResourceOwner = owner;

	queryDesc->snapshot = GetTransactionSnapshot();
	queryDesc->snapshot->copied = true;

	RegisterSnapshotOnOwner(queryDesc->snapshot, owner);

	ExecutorStart(queryDesc, 0);

	queryDesc->snapshot->active_count++;
	UnregisterSnapshotFromOwner(queryDesc->snapshot, owner);
	UnregisterSnapshotFromOwner(queryDesc->estate->es_snapshot, owner);

	CurrentResourceOwner = TopTransactionResourceOwner;

	MemoryContextSwitchTo(old);

	CommitTransactionCommand();

	CurrentResourceOwner = save;
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

/*
 * ContinuousQueryWorkerStartup
 *
 * Launches a CQ worker, which continuously generates partial query results to send
 * back to the combiner process.
 */
void
ContinuousQueryWorkerRun(Portal portal, ContinuousViewState *state, QueryDesc *queryDesc, ResourceOwner owner)
{
	EState	   *estate;
	DestReceiver *dest;
	CmdType		operation;
	MemoryContext oldcontext;
	MemoryContext es_query_cxt;
	MemoryContext es_query_child_cxt;
	MemoryContext runcontext_child;
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	char *cvname = rv->relname;
	int timeoutms = state->maxwaitms;
	MemoryContext runcontext;
	bool *activeFlagPtr = GetActiveFlagPtr(MyCQId);
	TimestampTz curtime = GetCurrentTimestamp();
	TimestampTz last_process_time = GetCurrentTimestamp();
	ResourceOwner cqowner = ResourceOwnerCreate(NULL, "CQResourceOwner");
	bool savereadonly = XactReadOnly;

	runcontext = AllocSetContextCreate(TopMemoryContext, "CQRunContext",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	start_executor(queryDesc, runcontext, cqowner);

	/* workers only need read-only transactions */
	XactReadOnly = true;

	CurrentResourceOwner = cqowner;

	estate = queryDesc->estate;
	operation = queryDesc->operation;

	/*
	 * startup tuple receiver, if we will be emitting tuples
	 */
	estate->es_processed = 0;
	estate->es_lastoid = InvalidOid;

	dest = CreateDestReceiver(DestCombiner);
	SetCombinerDestReceiverParams(dest, GetSocketName(MyCQId));

	(*dest->rStartup) (dest, operation, queryDesc->tupDesc);
	elog(LOG, "\"%s\" worker %d connected to combiner", cvname, MyProcPid);

	MarkWorkerAsRunning(MyCQId, MyWorkerId);

	while (!IsCombinerRunning(MyCQId))
		pg_usleep(COMBINER_WAIT_TIMEOUT);

	/* XXX (jay)Should be able to copy pointers and maintain an array of pointers instead
	   of an array of latches. This somehow does not work as expected and autovacuum
	   seems to be obliterating the new shared array. Make this better.
	 */
	memcpy(&GlobalStreamBuffer->procLatch[MyCQId], &MyProc->procLatch, sizeof(Latch));

	/*
	 * Create children contexts for all memory contexts that are used in the for loop,
	 * so we can reset them in case of errors while still preserving data in memory that
	 * is expected to persist across iterations of the loop.
	 */
	es_query_child_cxt = AllocSetContextCreate(estate->es_query_cxt, "es_query_ctx child",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	es_query_cxt = estate->es_query_cxt;
	estate->es_query_cxt = es_query_child_cxt;

	runcontext_child = AllocSetContextCreate(runcontext, "runcontext child",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	for (;;)
	{
		PG_TRY();
		{
			ResetStreamBufferLatch(MyCQId);
			if (GlobalStreamBuffer->empty)

			{

				curtime = GetCurrentTimestamp();
				if (TimestampDifferenceExceeds(last_process_time, curtime, EmptyStreamBufferWaitTime * 1000))
				{
					pgstat_report_activity(STATE_WORKER_WAIT, queryDesc->sourceText);
					WaitOnStreamBufferLatch(MyCQId);
					pgstat_report_activity(STATE_WORKER_RUNNING, queryDesc->sourceText);
				}
				else
				{
					pg_usleep(CQ_DEFAULT_SLEEP_MS * 1000);
				}
			}

			TopTransactionContext = runcontext_child;

			StartTransactionCommand();
			set_snapshot(estate, cqowner);

			CurrentResourceOwner = cqowner;
			oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

			/*
			 * Run plan on a microbatch
			 */
			ExecutePlan(estate, queryDesc->planstate, operation,
					true, 0, timeoutms, ForwardScanDirection, dest);

			MemoryContextSwitchTo(oldcontext);
			CurrentResourceOwner = cqowner;

			unset_snapshot(estate, cqowner);
			CommitTransactionCommand();

			if (estate->es_processed != 0)
			{
				/*
				 * If the CV query is such that the select does not return any tuples
				 * ex: select id where id=99; and id=99 does not exist, then this reset
				 * will fail. What will happen is that the worker will block at the latch for every
				 * allocated slot, TILL a cv returns a non-zero tuple, at which point
				 * the worker will resume a simple sleep for the threshold time.
				 */
				last_process_time = GetCurrentTimestamp();
			}
			estate->es_processed = 0;
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			MemoryContextResetAndDeleteChildren(estate->es_query_cxt);
			MemoryContextResetAndDeleteChildren(runcontext_child);
		}
		PG_END_TRY();

		/* Has the CQ been deactivated? */
		if (!*activeFlagPtr)
			break;
	}

	estate->es_query_cxt = es_query_cxt;
	CurrentResourceOwner = cqowner;

	(*dest->rShutdown) (dest);

	/*
	 * The cleanup functions below expect these things to be registered
	 */
	RegisterSnapshotOnOwner(estate->es_snapshot, cqowner);
	RegisterSnapshotOnOwner(queryDesc->snapshot, cqowner);
	RegisterSnapshotOnOwner(queryDesc->crosscheck_snapshot, cqowner);

	/* cleanup */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);

	MemoryContextDelete(runcontext);

	XactReadOnly = savereadonly;

	if (queryDesc->totaltime)
		InstrStopNode(queryDesc->totaltime, estate->es_processed);

	CurrentResourceOwner = owner;
}
