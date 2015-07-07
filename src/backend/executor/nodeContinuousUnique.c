/*-------------------------------------------------------------------------
 *
 * nodeUnique.c
 *	  Routines to handle unique'ing of continuous queries using HLL.
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeContinuousUnique.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pipeline_tstate_fn.h"
#include "executor/executor.h"
#include "executor/nodeContinuousUnique.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/* ----------------------------------------------------------------
 *		ExecContinuousUnique
 * ----------------------------------------------------------------
 */
TupleTableSlot *				/* return: a tuple or NULL */
ExecContinuousUnique(ContinuousUniqueState *node)
{
	Unique *plannode = (Unique *) node->ps.plan;
	TupleTableSlot *resultTupleSlot;
	TupleTableSlot *slot;
	PlanState  *outerPlan;
	int unique;

	/*
	 * get information from the node
	 */
	outerPlan = outerPlanState(node);
	resultTupleSlot = node->ps.ps_ResultTupleSlot;

	/*
	 * now loop, returning only non-duplicate tuples. We assume that the
	 * tuples arrive in sorted order so we can detect duplicates easily. The
	 * first tuple of each group is returned.
	 */
	for (;;)
	{
		/*
		 * fetch a tuple from the outer subplan
		 */
		slot = ExecProcNode(outerPlan);

		if (TupIsNull(slot))
		{
			/* end of subplan, so we're done */
			ExecClearTuple(resultTupleSlot);
			return NULL;
		}

		/* Did the tuple increase the distinct multiset size? */
		node->distinct = HLLAddSlot(node->distinct, slot, plannode->numCols, plannode->uniqColIdx, &unique);
		if (unique)
		{
			node->card++;
			break;
		}
	}

	/*
	 * We have a new tuple different from the previous saved tuple (if any).
	 * Save it and return it.  We must copy it because the source subplan
	 * won't guarantee that this source tuple is still accessible after
	 * fetching the next source tuple.
	 */
	return ExecCopySlot(resultTupleSlot, slot);
}

/* ----------------------------------------------------------------
 *		ExecInitContinuousUnique
 *
 *		This initializes the continuous unique node state structures
 *		and	the node's subplan.
 * ----------------------------------------------------------------
 */
ContinuousUniqueState *
ExecInitContinuousUnique(ContinuousUnique *node, EState *estate, int eflags)
{
	ContinuousUniqueState *state;

	state = makeNode(ContinuousUniqueState);
	namecpy(&state->cvname, &node->cvName);

	state->ps.plan = (Plan *) node;
	state->ps.state = estate;

	/*
	 * Tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->ps);

	/*
	 * then initialize outer plan
	 */
	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);

	/*
	 * unique nodes do no projections, so initialize projection info for this
	 * node appropriately
	 */
	ExecAssignResultTypeFromTL(&state->ps);
	state->ps.ps_ProjInfo = NULL;

	/*
	 * Load the multiset from pipeline_query.
	 */
	state->distinct = GetDistinctHLL(NameStr(state->cvname));
	state->init_card = state->card = HLLSize(state->distinct);

	return state;
}

/* ----------------------------------------------------------------
 *		ExecEndContinuousUnique
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void
ExecEndContinuousUnique(ContinuousUniqueState *node)
{
	/* Only update the distinct column if the cardinality has changed */
	if (node->card > node->init_card)
	{
		UpdateDistinctHLL(NameStr(node->cvname), node->distinct);
		node->init_card = node->card;
	}

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	pfree(node->distinct);
	ExecEndNode(outerPlanState(node));
}


void
ExecReScanContinuousUnique(ContinuousUniqueState *node)
{
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);

	if (node->distinct)
	{
		pfree(node->distinct);
		node->distinct = GetDistinctHLL(NameStr(node->cvname));
	}
}
