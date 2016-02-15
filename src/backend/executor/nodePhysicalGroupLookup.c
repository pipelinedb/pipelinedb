/*-------------------------------------------------------------------------
 *
 * nodePhysicalGroupLookup.c
 *
 * 	Node for efficiently retrieving updatable physical groups from
 * 	aggregate materialization tables.
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodePhysicalGroupLookup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodePhysicalGroupLookup.h"
#include "storage/bufmgr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

/*
 * ExecInitPhysicalGroupLookup
 */
PhysicalGroupLookupState *
ExecInitPhysicalGroupLookup(PhysicalGroupLookup *node, EState *estate, int eflags)
{
	Plan *outer = outerPlan(node);
	NestLoop *nl;
	PhysicalGroupLookupState *state;
	TupleDesc resultdesc;

	if (!IsA(outer, NestLoop))
		elog(ERROR, "unexpected join node found in physical group lookup: %d", nodeTag(outer));

	nl = (NestLoop *) outer;
	nl->join.jointype = JOIN_INNER;

	state = makeNode(PhysicalGroupLookupState);
	state->ps.plan = (Plan *) node;

	outerPlanState(state) = ExecInitNode(outer, estate, eflags);

	ExecInitResultTupleSlot(estate, &state->ps);
	resultdesc = ExecTypeFromTL(innerPlan(outer)->targetlist, false);
	ExecAssignResultType(&state->ps, resultdesc);

	return state;
}

/*
 * ExecPhysicalGroupLookup
 */
TupleTableSlot *
ExecPhysicalGroupLookup(PhysicalGroupLookupState *node)
{
	NestLoopState *outer = (NestLoopState *) outerPlanState(node);
	TupleTableSlot *result;
	TupleTableSlot *inner;
	bool res;
	EState *estate = outer->js.ps.state;
	Buffer buffer;
	HeapUpdateFailureData hufd;
	HeapTuple tup;
	Relation rel;
	TupleTableSlot *slot = NULL;
	ScanState *scan;

	/* Get next tuple from subplan, if any. */
lnext:
	result = ExecProcNode((PlanState *) outer);
	if (TupIsNull(result))
		return NULL;

	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	/* this slot contains the physical matrel tuple that was joined on */
	inner = outer->js.ps.ps_ExprContext->ecxt_innertuple;

	tup = inner->tts_tuple;
	Assert(inner->tts_tuple);

	scan = (ScanState *) outer->js.ps.righttree;
	rel = scan->ss_currentRelation;

	/* lock the physical tuple for update */
	res = heap_lock_tuple(rel, tup, estate->es_output_cid,
			LockTupleExclusive, false, true, &buffer, &hufd);
	ReleaseBuffer(buffer);

	switch (res)
	{
		case HeapTupleSelfUpdated:
			/*
			 * The target tuple was already updated or deleted by the
			 * current command, or by a later command in the current
			 * transaction.
			 *
			 * This should NEVER happen.
			 */
			elog(ERROR, "tuple updated again in the same transaction");
			break;

		case HeapTupleMayBeUpdated:
			/* Got the lock successfully! */
			slot = inner;
			break;

		case HeapTupleUpdated:
			/* Was tuple deleted? */
			if (ItemPointerEquals(&hufd.ctid, &tup->t_self))
				goto lnext;

			/* Tuple was updated, so fetch and lock the updated version */
			tup = EvalPlanQualFetch(estate, rel, LockTupleExclusive, false, &hufd.ctid, hufd.xmax);
			if (tup == NULL)
				goto lnext;

			slot = ExecStoreTuple(tup, node->ps.ps_ResultTupleSlot, InvalidBuffer, true);
			break;

		default:
			elog(ERROR, "unrecognized heap_lock_tuple status: %u", res);
	}

	return slot;
}

/*
 * ExecEndPhysicalGroupLookup
 */
void
ExecEndPhysicalGroupLookup(PhysicalGroupLookupState *node)
{
	ExecEndNode(outerPlanState(node));
}
