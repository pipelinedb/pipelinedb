/*-------------------------------------------------------------------------
 *
 * nodePhysicalGroupLookup.c
 *
 * 	Node for efficiently retrieving updatable physical groups from
 * 	aggregate materialization tables.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodePhysicalGroupLookup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/nodePhysicalGroupLookup.h"

/*
 * ExecInitExtractPhysical
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
	nl->join.jointype = JOIN_SEMI;

	state = makeNode(PhysicalGroupLookupState);
	state->ps.plan = (Plan *) node;

	outerPlanState(state) = ExecInitNode(outer, estate, eflags);

	ExecInitResultTupleSlot(estate, &state->ps);
	resultdesc = ExecTypeFromTL(innerPlan(outer)->targetlist, false);
	ExecAssignResultType(&state->ps, resultdesc);

	return state;
}

/*
 * ExecExtractPhysical
 */
TupleTableSlot *
ExecPhysicalGroupLookup(PhysicalGroupLookupState *node)
{
	NestLoopState *outer = (NestLoopState *) outerPlanState(node);
	TupleTableSlot *result = ExecProcNode((PlanState *) outer);
	TupleTableSlot *inner = outer->js.ps.ps_ExprContext->ecxt_innertuple;

	if (TupIsNull(result))
		return NULL;

	/* this slot contains the physical matrel tuple that was joined on */
	return inner;
}

/*
 * ExecEndExtractPhysical
 */
void
ExecEndPhysicalGroupLookup(PhysicalGroupLookupState *node)
{
	ExecEndNode(outerPlanState(node));
}
