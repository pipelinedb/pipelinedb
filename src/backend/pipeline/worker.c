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

#include "access/xact.h"
#include "catalog/pipeline_queries.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pipeline/combiner.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/worker.h"
#include "tcop/dest.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "miscadmin.h"


/*
 * ContinuousQueryWorkerStartup
 *
 * Launches a CQ worker, which continuously generates partial query results to send
 * back to the combiner process.
 */
extern void
ContinuousQueryWorkerRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	EState	   *estate;
	DestReceiver *dest;
	CmdType		operation;
	MemoryContext oldcontext;
	ResourceOwner save = CurrentResourceOwner;
	char *cvname = queryDesc->plannedstmt->cq_target->relname;
	int batchsize = queryDesc->plannedstmt->cq_batch_size;
	int timeoutms = queryDesc->plannedstmt->cq_batch_timeout_ms;

	CurrentResourceOwner = owner;

	/* prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* Allow instrumentation of Executor overall runtime */
	if (queryDesc->totaltime)
		InstrStartNode(queryDesc->totaltime);

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
			pg_usleep(PIPELINE_SLEEP_MS * 1000);

		estate->es_processed = 0;

		MemoryContextReset(ContinuousQueryContext);
	}

	(*dest->rShutdown) (dest);

	/* cleanup */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);

	if (queryDesc->totaltime)
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
}
