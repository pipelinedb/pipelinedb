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
	int batchsize = queryDesc->plannedstmt->cq_state->batchsize;
	int timeoutms = queryDesc->plannedstmt->cq_state->maxwaitms;
	Relation pipeline_queries;
	HeapTuple tuple;
	HeapTuple newtuple;
	Form_pipeline_queries row;
	NameData name;
	bool nulls[Natts_pipeline_queries];
	bool replaces[Natts_pipeline_queries];
	Datum values[Natts_pipeline_queries];
	bool alreadyRunning = false;
	bool hasBeenDeactivated = false;
	clock_t lastCheckTime = clock();

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

	namestrcpy(&name, cvname);

//	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
//	tuple = SearchSysCache1(PIPELINEQUERIESNAME, NameGetDatum(&name));
//
//	if (!HeapTupleIsValid(tuple))
//		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist", cvname);
//
//	row = (Form_pipeline_queries) GETSTRUCT(tuple);
//	alreadyRunning = row->state == PIPELINE_QUERY_STATE_ACTIVE;
//
//	if (!alreadyRunning)
//	{
//		/* Mark the CV as active. */
//		MemSet(values, 0, sizeof(values));
//		MemSet(nulls, false, sizeof(nulls));
//		MemSet(replaces, false, sizeof(replaces));
//
//		replaces[Anum_pipeline_queries_state - 1] = true;
//		values[Anum_pipeline_queries_state - 1] = PIPELINE_QUERY_STATE_ACTIVE;
//
//		newtuple = heap_modify_tuple(tuple, RelationGetDescr(pipeline_queries), values,
//				nulls, replaces);
//		simple_heap_update(pipeline_queries, &newtuple->t_self, newtuple);
//		CommandCounterIncrement();
//	}

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

		MemoryContextReset(ContinuousQueryContext);

//		if ((double) (clock() - lastCheckTime) >= (2 * CLOCKS_PER_SEC))
//		{
//			/* Check is we have been deactivated, and break out
//			 * if we have. */
//			StartTransactionCommand();
//			tuple = SearchSysCache1(PIPELINEQUERIESNAME,
//					NameGetDatum(&name));
//			hasBeenDeactivated = !HeapTupleIsValid(tuple);
//			if (!hasBeenDeactivated)
//			{
//				row = (Form_pipeline_queries) GETSTRUCT(tuple);
//				hasBeenDeactivated = (row->state ==
//						PIPELINE_QUERY_STATE_INACTIVE);
//			}
//			ReleaseSysCache(tuple);
//			CommitTransactionCommand();
//
//			if (hasBeenDeactivated)
//				break;
//
//			lastCheckTime = clock();
//		}
	}

	(*dest->rShutdown) (dest);

	/* cleanup */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);
	FreeQueryDesc(queryDesc);

	if (queryDesc->totaltime)
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
}
