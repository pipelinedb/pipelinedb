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
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "pipeline/gc.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"

/*
 * ExecutePlannedStmt
 *
 * Execute plannedstmt, and discard the results.
 */
void
ExecutePlannedStmt(PlannedStmt *plannedstmt)
{
	Portal portal;
	DestReceiver *receiver;
	char completionTag[COMPLETION_TAG_BUFSIZE];

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	portal = CreatePortal("__gc__", true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
			NULL,
			NULL,
			"DELETE",
			list_make1(plannedstmt),
			NULL);

	receiver = CreateDestReceiver(DestNone);
	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			completionTag);


	(*receiver->rDestroy) (receiver);
	PortalDrop(portal, false);

	PopActiveSnapshot();
	CommitTransactionCommand();
}

/*
 * ContinuousQueryGarbageCollectorRun
 */
void
ContinuousQueryGarbageCollectorRun(Portal portal, CombinerDesc *combiner, QueryDesc *queryDesc, ResourceOwner owner)
{
	RangeVar *rv = queryDesc->plannedstmt->cq_target;
	char *cvname = rv->relname;
	MemoryContext gc_ctx;
	ResourceOwner save = CurrentResourceOwner;
	bool hasBeenDeactivated = false;
	TimestampTz lastDeactivateCheckTime = GetCurrentTimestamp();

	gc_ctx = AllocSetContextCreate(TopMemoryContext,
			"GarbageCollectContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	elog(LOG, "\"%s\" gc %d running", cvname, MyProcPid);

	CurrentResourceOwner = owner;

	/* Mark the PlannedStmt as not continuous now */
	queryDesc->plannedstmt->is_continuous = false;

	for(;;)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(gc_ctx);

		ExecutePlannedStmt(queryDesc->plannedstmt);

		MemoryContextReset(gc_ctx);
		MemoryContextSwitchTo(oldcontext);

		if (TimestampDifferenceExceeds(lastDeactivateCheckTime, GetCurrentTimestamp(), CQ_INACTIVE_CHECK_MS))
		{
			/* Check is we have been deactivated, and break out
			 * if we have. */
			StartTransactionCommand();

			hasBeenDeactivated = !IsContinuousViewActive(rv);

			CommitTransactionCommand();

			if (hasBeenDeactivated)
				break;

			lastDeactivateCheckTime = GetCurrentTimestamp();
		}

		pg_usleep(1 * 1000 * 1000);
	}

	CurrentResourceOwner = save;
}
