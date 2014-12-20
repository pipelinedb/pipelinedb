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
}

static void
unset_snapshot(EState *estate, ResourceOwner owner)
{
	UnregisterSnapshotFromOwner(estate->es_snapshot, owner);
}

/*
 * ContinuousQueryWorkerStartup
 *
 * Launches a CQ worker, which continuously generates partial query results to send
 * back to the combiner process.
 */
void
ContinuousQueryWorkerRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	EState	   *estate;
	DestReceiver *dest;
	CmdType		operation;
	MemoryContext oldcontext;
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	char *cvname = rv->relname;
	int batchsize = queryDesc->plannedstmt->cq_state->batchsize;
	int timeoutms = queryDesc->plannedstmt->cq_state->maxwaitms;
	MemoryContext runcontext;
	int32 cq_id = queryDesc->plannedstmt->cq_state->id;
	bool *activeFlagPtr = GetActiveFlagPtr(cq_id);
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
	SetCombinerDestReceiverParams(dest, combiner);

	(*dest->rStartup) (dest, operation, queryDesc->tupDesc);
	elog(LOG, "\"%s\" worker %d connected to combiner", cvname, MyProcPid);

	IncrementProcessGroupCount(cq_id);
	/* XXX (jay)Should be able to copy pointers and maintain an array of pointers instead
	   of an array of latches. This somehow does not work as expected and autovacuum
	   seems to be obliterating the new shared array. Make this better.
	 */
	memcpy(&GlobalStreamBuffer->procLatch[cq_id], &MyProc->procLatch, sizeof(Latch));

	for (;;)
	{
		ResetStreamBufferLatch(cq_id);
		if (GlobalStreamBuffer->empty)
		{
			curtime = GetCurrentTimestamp();
			if (TimestampDifferenceExceeds(last_process_time, curtime, EmptyStreamBufferWaitTime * 1000))
			{
				pgstat_report_activity(STATE_WORKER_WAIT_ON_LATCH, queryDesc->sourceText);
				WaitOnStreamBufferLatch(cq_id);
				pgstat_report_activity(STATE_WORKER_CONTINUE, queryDesc->sourceText);
			}
			else
			{
				pg_usleep(CQ_DEFAULT_SLEEP_MS * 1000);
			}
		}

		TopTransactionContext = runcontext;

		StartTransactionCommand();
		set_snapshot(estate, cqowner);

		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

		/*
		 * Run plan on a microbatch
		 */
		ExecutePlan(estate, queryDesc->planstate, operation,
					true, batchsize, timeoutms, ForwardScanDirection, dest);

		unset_snapshot(estate, cqowner);
		CommitTransactionCommand();

		CurrentResourceOwner = cqowner;

		MemoryContextSwitchTo(oldcontext);

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

		/* Check the shared metadata to see if the CV has been deactivated */
		if (!*activeFlagPtr)
		{
			int32 i = -1;
			ssize_t res = write(combiner->sock, &i, sizeof(int32));
			if (res < 0)
				elog(ERROR, "failed to send about-to-die message to the combiner");
			SetCQWorkerDoneFlag(cq_id);
			break;
		}
	}

	(*dest->rShutdown) (dest);

	DecrementProcessGroupCount(cq_id);

	/*
	 * The cleanup functions below expected this thing to be registered */
	RegisterSnapshotOnOwner(estate->es_snapshot, cqowner);

	/* cleanup */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	MemoryContextDelete(runcontext);

	XactReadOnly = savereadonly;

	if (queryDesc->totaltime)
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
}
