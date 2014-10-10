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

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pipeline_queries.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "pipeline/combiner.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/worker.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "pipeline/cvmetadata.h"


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
	ResourceOwner save = CurrentResourceOwner;
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	char *cvname = rv->relname;
	int batchsize = queryDesc->plannedstmt->cq_state->batchsize;
	int timeoutms = queryDesc->plannedstmt->cq_state->maxwaitms;
	NameData name;
	bool hasBeenDeactivated = false;
	MemoryContext runcontext;
	MemoryContext execcontext;
	TimestampTz lastDeactivateCheckTime = GetCurrentTimestamp();
	int32 cq_id = queryDesc->plannedstmt->cq_state->id;

	runcontext = AllocSetContextCreate(TopMemoryContext, "CQRunContext",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	execcontext = AllocSetContextCreate(runcontext, "ExecProcNodeContext",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	namestrcpy(&name, cvname);
	CurrentResourceOwner = owner;

	/* prepare the plan for execution */
	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(runcontext);
	ExecutorStart(queryDesc, 0);
	MemoryContextSwitchTo(oldcontext);

	CommitTransactionCommand();

	estate = queryDesc->estate;
	operation = queryDesc->operation;
	estate->es_exec_node_cxt = execcontext;

	/*
	 * startup tuple receiver, if we will be emitting tuples
	 */
	estate->es_processed = 0;
	estate->es_lastoid = InvalidOid;

	dest = CreateDestReceiver(DestCombiner);
	SetCombinerDestReceiverParams(dest, combiner);

	(*dest->rStartup) (dest, operation, queryDesc->tupDesc);
	elog(LOG, "\"%s\" worker %d connected to combiner", cvname, MyProcPid);

	DecrementProcessGroupCount(cq_id);

	/*
	 * We wait until we're up and running before telling the stream buffer that
	 * there is a new reader in order to avoid having any events being assigned
	 * to a process that fails to launch properly.
	 */
	NotifyUpdateGlobalStreamBuffer();

	CurrentResourceOwner = save;

	for (;;)
	{
		oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
		CurrentResourceOwner = owner;

		/*
		 * Run plan on a microbatch
		 */
		ExecutePlan(estate, queryDesc->planstate, operation,
					true, batchsize, timeoutms, ForwardScanDirection, dest);

		MemoryContextSwitchTo(oldcontext);

		CurrentResourceOwner = save;

		/*
		 * If we didn't see any new tuples, sleep briefly to save cycles
		 */
		if (estate->es_processed == 0)
			pg_usleep(CQ_DEFAULT_SLEEP_MS * 1000);

		estate->es_processed = 0;
		
		/* Check the shared metadata to see if the CV has been deactivated */
		if (GetActiveFlag(cq_id) == false)
		{
			break;
		}
	}

	(*dest->rShutdown) (dest);

	IncrementProcessGroupCount(cq_id);
	/* cleanup */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);

	MemoryContextDelete(runcontext);

	if (queryDesc->totaltime)
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
}
