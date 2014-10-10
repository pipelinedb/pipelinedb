/*-------------------------------------------------------------------------
 *
 * gc.c
 *
 *	  GC process functionality
 *
 * src/backend/pipeline/gc.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/pipeline_queries.h"
#include "commands/pipelinecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "pipeline/gc.h"
#include "pipeline/cvmetadata.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/*
 * ContinuousQueryWorkerStartup
 *
 * Launches a CQ worker, which continuously generates partial query results to send
 * back to the combiner process.
 */
void
ContinuousQueryGarbageCollectorRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	EState	   *estate;
	DestReceiver *dest;
	MemoryContext oldcontext;
	MemoryContext exec_ctx;
	ResourceOwner save = CurrentResourceOwner;
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	char *cvname = rv->relname;
	int32 cq_id = queryDesc->plannedstmt->cq_state->id;
	bool *activeFlagPtr = GetActiveFlagPtr(cq_id);

	exec_ctx = AllocSetContextCreate(TopMemoryContext, "GCContext",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	dest = CreateDestReceiver(DestNone);

	elog(LOG, "\"%s\" gc %d running", cvname, MyProcPid);


	for (;;)
	{
		CurrentResourceOwner = owner;
		oldcontext = MemoryContextSwitchTo(exec_ctx);

		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		queryDesc->snapshot = GetActiveSnapshot();

		ExecutorStart(queryDesc, 0);

		estate = queryDesc->estate;
		estate->es_exec_node_cxt = exec_ctx;
		estate->es_lastoid = InvalidOid;
		estate->es_processed = 0;

		ExecutePlan(estate, queryDesc->planstate, queryDesc->operation,
					true, 0, 0, ForwardScanDirection, dest);

		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		queryDesc->snapshot = NULL;
		queryDesc->estate = NULL;

		PopActiveSnapshot();
		CommitTransactionCommand();

		MemoryContextReset(exec_ctx);
		MemoryContextSwitchTo(oldcontext);

		CurrentResourceOwner = save;

		/* Check the shared metadata to see if the CV has been deactivated */
		if (!*activeFlagPtr)
			break;

		pg_usleep(CQ_GC_SLEEP_MS * 1000);
	}

	FreeQueryDesc(queryDesc);
	MemoryContextDelete(exec_ctx);
}
