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
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"

static void
get_bytes(TupleTableSlot *slot, int num_attrs, AttrNumber *attrs, StringInfo buf)
{
	TupleDesc desc = slot->tts_tupleDescriptor;
	int i;

	num_attrs = num_attrs == -1 ? desc->natts : num_attrs;

	for (i = 0; i < num_attrs; i++)
	{
		bool isnull;
		AttrNumber attno = attrs == NULL ? i + 1 : attrs[i];
		Form_pg_attribute att = slot->tts_tupleDescriptor->attrs[attno - 1];
		Datum d = slot_getattr(slot, attno, &isnull);
		Size size;

		if (isnull)
			continue;

		size = datumGetSize(d, att->attbyval, att->attlen);

		if (att->attbyval)
			appendBinaryStringInfo(buf, (char *) &d, size);
		else
			appendBinaryStringInfo(buf, DatumGetPointer(d), size);
	}
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
	MemoryContext oldcontext;
	StringInfoData buf;

	/*
	 * get information from the node
	 */
	outerPlan = outerPlanState(node);
	resultTupleSlot = node->ps.ps_ResultTupleSlot;

	oldcontext = MemoryContextSwitchTo(node->tmpContext);
	initStringInfo(&buf);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * now loop, returning only non-duplicate tuples. We assume that the
	 * tuples arrive in sorted order so we can detect duplicates easily. The
	 * first tuple of each group is returned.
	 */
	for (;;)
	{
		StringInfoData buf;
		bool missing;

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

		oldcontext = MemoryContextSwitchTo(node->tmpContext);
		get_bytes(slot, plannode->numCols, plannode->uniqColIdx, &buf);
		MemoryContextSwitchTo(oldcontext);

		missing = !BloomFilterContains(node->distinct, buf.data, buf.len);

		if (missing)
		{
			BloomFilterAdd(node->distinct, buf.data, buf.len);
			node->dirty = true;
			break;
		}

		resetStringInfo(&buf);
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
	MemoryContext oldcontext;

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

	state->tmpContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "ContinuousUnique",
							  ALLOCSET_DEFAULT_MINSIZE,
							  ALLOCSET_DEFAULT_INITSIZE,
							  ALLOCSET_DEFAULT_MAXSIZE);

	/*
	 * Load the multiset from pipeline_query.
	 */
	oldcontext = MemoryContextSwitchTo(state->tmpContext);
	state->distinct = GetDistinctBloomFilter(NameStr(state->cvname));
	MemoryContextSwitchTo(oldcontext);

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
	if (IsCombiner && node->dirty)
	{
		UpdateDistinctBloomFilter(NameStr(node->cvname), node->distinct);
		node->dirty = false;
	}

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	MemoryContextDelete(node->tmpContext);

	ExecEndNode(outerPlanState(node));
}

void
ExecEndBatchContinuousUnique(ContinuousUniqueState *node)
{
	MemoryContext oldcontext;

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	MemoryContextReset(node->tmpContext);

	oldcontext = MemoryContextSwitchTo(node->tmpContext);
	node->distinct = GetDistinctBloomFilter(NameStr(node->cvname));
	MemoryContextSwitchTo(oldcontext);
}

void
ExecReScanContinuousUnique(ContinuousUniqueState *node)
{
	MemoryContext oldcontext;

	ExecClearTuple(node->ps.ps_ResultTupleSlot);
	MemoryContextReset(node->tmpContext);

	if (node->ps.lefttree->chgParam == NULL)
		ExecReScan(node->ps.lefttree);

	node->dirty = false;

	oldcontext = MemoryContextSwitchTo(node->tmpContext);
	node->distinct = GetDistinctBloomFilter(NameStr(node->cvname));
	MemoryContextSwitchTo(oldcontext);
}
