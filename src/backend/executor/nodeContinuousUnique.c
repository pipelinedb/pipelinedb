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

static uint64_t
hash_tuple(TupleTableSlot *slot, Unique *unique)
{
	TupleDesc desc = slot->tts_tupleDescriptor;
	StringInfo buf = makeStringInfo();
	Size len = 0;
	int i;
	uint64_t out[2];

	for (i = 0; i < unique->numCols; i++)
	{
		bool isnull;
		int attno = unique->uniqColIdx[i];
		Form_pg_attribute att = desc->attrs[attno - 1];
		Datum d = slot_getattr(slot, attno, &isnull);
		Size size;

		if (isnull)
			continue;

		size = datumGetSize(d, att->attbyval, att->attlen);

		if (att->attbyval)
			appendBinaryStringInfo(buf, (char *) &d, size);
		else
			appendBinaryStringInfo(buf, DatumGetPointer(d), size);

		len += size;
	}

	MurmurHash3_x64_128((void *) buf->data, len, 0xdeadbeef, out);
	resetStringInfo(buf);
	return out[0];
}

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
		multiset_add(node->distinct_ms, hash_tuple(slot, plannode));
		if (multiset_card(node->distinct_ms) > node->card)
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
	state->distinct_ms = palloc0(sizeof(multiset_t));

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
	state->distinct_ms = GetDistinctMultiset(NameStr(state->cvname));
	state->orig_card = state->card = (uint64_t) multiset_card(state->distinct_ms);

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
	if (node->card > node->orig_card)
		UpdateDistinctMultiset(NameStr(node->cvname), node->distinct_ms);

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	pfree(node->distinct_ms);
	ExecEndNode(outerPlanState(node));
}


void
ExecReScanContinuousUnique(ContinuousUniqueState *node)
{
	ExecClearTuple(node->ps.ps_ResultTupleSlot);

	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);

	if (node->distinct_ms)
	{
		pfree(node->distinct_ms);
		node->distinct_ms = GetDistinctMultiset(NameStr(node->cvname));
	}
}
