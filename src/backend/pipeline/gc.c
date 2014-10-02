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
 * GarbageCollectDisqualifiedTuples
 *
 * Garbage collect any tuples that no longer belong to the CQ result set.
 * This is only applicable to sliding windows of the form:
 *   SELECT * from test_stream WHERE time::timestamptz > clock_timestamp() - interval '5' minute;
 */
void
GarbageCollectDisqualifiedTuples(PlannedStmt *plannedstmt)
{
	MemoryContext oldcontext;
	Portal portal;
	DestReceiver *receiver;
	char completionTag[COMPLETION_TAG_BUFSIZE];

	Assert(plannedstmt->cq_gc_plan != NULL);

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	portal = CreatePortal("__gc__", true, true);
	portal->visible = false;
	PortalDefineQuery(portal,
			NULL,
			NULL,
			"DELETE",
			list_make1(plannedstmt->cq_gc_plan),
			NULL);

	receiver = CreateDestReceiver(DestNone);
	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	oldcontext = MemoryContextSwitchTo(TopTransactionContext);

	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			completionTag);

	MemoryContextSwitchTo(oldcontext);

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

	/* if there is no clean up needed, exit immediately. */
	if (!queryDesc->plannedstmt->cq_gc_plan)
		return;

	gc_ctx = AllocSetContextCreate(TopMemoryContext,
			"GarbageCollectContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	elog(LOG, "\"%s\" gc %d running", cvname, MyProcPid);

	CurrentResourceOwner = owner;

	for(;;)
	{
		MemoryContext oldcontext = MemoryContextSwitchTo(gc_ctx);

		GarbageCollectDisqualifiedTuples(queryDesc->plannedstmt);

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
