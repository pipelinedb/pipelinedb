/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * nodeStreamTableJoin.c
 *	  routines to support stream-table joins
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeStreamTableJoin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/execdebug.h"
#include "executor/nodeStreamTablejoin.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"


static List *
get_rescan_nodes(PlanState *root)
{
	List *result = NIL;

	result = lappend(result, root);

	if (outerPlanState(root))
		result = list_concat(result, get_rescan_nodes(outerPlanState(root)));
	if (innerPlanState(root))
		result = list_concat(result, get_rescan_nodes(innerPlanState(root)));

	return result;
}

static TupleTableSlot *
stream_batch_next(StreamTableJoinState *node)
{
	PlanState *inner = innerPlanState(node);
	TupleTableSlot *slot = inner->ps_ResultTupleSlot;

	if (!tuplestore_gettupleslot(node->stj_StreamBatch, true, false, slot))
		ExecClearTuple(slot);

	return slot;
}

/*
 * unpin_stream_events
 *
 * We don't unpin events until they're actually done being joined
 * against the outer node. This prevents races and guarantees that
 * our events are always valid.
 */
static void
unpin_stream_events(StreamTableJoinState *node)
{
	StreamScanState *scan = (StreamScanState *) innerPlanState(node);
	ListCell *lc;
	foreach(lc, scan->pinned)
	{
		StreamBufferSlot *sbs = (StreamBufferSlot *) lfirst(lc);
		StreamBufferUnpinSlot(scan->reader, sbs);
	}
	list_free(scan->pinned);
	scan->pinned = NIL;
}

static bool
has_inner_batch(StreamTableJoinState *node)
{
	PlanState *inner = innerPlanState(node);
	TupleTableSlot *slot;

	if (!node->stj_NeedNewInner)
		return true;

	unpin_stream_events(node);
	for (;;)
	{
		slot = ExecProcNode(inner);
		if (TupIsNull(slot))
			break;

		tuplestore_puttupleslot(node->stj_StreamBatch, slot);
		node->stj_NeedNewInner = false;
	}

	tuplestore_rescan(node->stj_StreamBatch);
	ExecReScan((PlanState *) node);

	return !node->stj_NeedNewInner;
}

/*
 * disable_batching
 *
 * We disable batching semantics on the outer node of stream-table joins
 * because it doesn't make sense for tables.
 */
static void
disable_batching(PlanState *root)
{
	if (root == NULL)
		return;

	DisableBatching(root);

	disable_batching(root->lefttree);
	disable_batching(root->righttree);
}

StreamTableJoinState *
ExecInitStreamTableJoin(StreamTableJoin *node, EState *estate, int eflags)
{
	StreamTableJoinState *state;
	StreamScanState *sscan;

	state = makeNode(StreamTableJoinState);
	state->js.ps.plan = (Plan *) node;
	state->js.ps.state = estate;

	ExecAssignExprContext(estate, &state->js.ps);

	/*
	 * Initialize child expressions
	 */
	state->js.ps.targetlist = (List *)
		ExecInitExpr((Expr *) node->join.plan.targetlist,
					 (PlanState *) state);
	state->js.ps.qual = (List *)
		ExecInitExpr((Expr *) node->join.plan.qual,
					 (PlanState *) state);
	state->js.jointype = node->join.jointype;
	state->js.joinqual = (List *)
		ExecInitExpr((Expr *) node->join.joinqual,
					 (PlanState *) state);

	/*
	 * Initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */
	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(state) = ExecInitNode(innerPlan(node), estate, eflags);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->js.ps);

	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			state->stj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(state)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&state->js.ps);
	ExecAssignProjectionInfo(&state->js.ps, NULL);

	state->js.ps.ps_TupFromTlist = false;
	state->stj_NeedNewInner = true;
	state->stj_NeedNewOuter = true;
	state->stj_MatchedOuter = false;

	/* 1kB per event seems like a reasonable amount of memory to reserve for a batch */
	state->stj_StreamBatch = tuplestore_begin_heap(false, true, 1 * 1024 * estate->cq_batch_size);

	sscan = (StreamScanState *) innerPlanState(state);
	sscan->unpin = false;

	/* the table side of the join shouldn't use batching semantics */
	disable_batching(outerPlanState(state));

	return state;
}

TupleTableSlot *
ExecStreamTableJoin(StreamTableJoinState *node)
{
	StreamTableJoin *stj;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	List *joinqual;
	List *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	stj = (StreamTableJoin *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Check to see if we're still projecting out tuples from a previous join
	 * tuple (because there is a function-returning-set in the projection
	 * expressions).  If so, try to project another one.
	 */
	if (node->js.ps.ps_TupFromTlist)
	{
		TupleTableSlot *result;
		ExprDoneCond isDone;

		result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);
		if (isDone == ExprMultipleResult)
			return result;
		/* Done with that source tuple... */
		node->js.ps.ps_TupFromTlist = false;
	}

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.  Note this can't happen
	 * until we're done projecting out tuples from a join tuple.
	 */
	ResetExprContext(econtext);

	/* make sure we have a full stream batch to scan in the inner loop */
	if (!has_inner_batch(node))
		return NULL;

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->stj_NeedNewOuter)
		{
			outerTupleSlot = ExecProcNode(outerPlan);

			if (TupIsNull(outerTupleSlot))
			{
				tuplestore_clear(node->stj_StreamBatch);
				node->stj_NeedNewInner = true;

				return NULL;
			}

			econtext->ecxt_outertuple = outerTupleSlot;
			node->stj_NeedNewOuter = false;
			node->stj_MatchedOuter = false;

			/*
			 * Fetch the values of any outer Vars that must be passed to the
			 * inner scan, and store them in the appropriate PARAM_EXEC slots.
			 */
			foreach(lc, stj->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
				int			paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				prm->value = slot_getattr(outerTupleSlot,
										  nlp->paramval->varattno,
										  &(prm->isnull));
				/* Flag parameter value as changed */
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
													 paramno);
			}

			 /* rescan the stream batch */
			 tuplestore_rescan(node->stj_StreamBatch);
		}

		innerTupleSlot = stream_batch_next(node);
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			node->stj_NeedNewOuter = true;

			if (!node->stj_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->stj_NullInnerTupleSlot;

				if (otherqual == NIL || ExecQual(otherqual, econtext, false))
				{
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					TupleTableSlot *result;
					ExprDoneCond isDone;

					result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

					if (isDone != ExprEndResult)
					{
						node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);

						return result;
					}
				}
				else
				{
					InstrCountFiltered2(node, 1);
				}
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

		/*
		 * At this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		if (ExecQual(joinqual, econtext, false))
		{
			node->stj_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->stj_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * In a semijoin, we'll consider returning the first match, but
			 * after that we're done with this outer tuple.
			 */
			if (node->js.jointype == JOIN_SEMI)
				node->stj_NeedNewOuter = true;

			if (otherqual == NIL || ExecQual(otherqual, econtext, false))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				TupleTableSlot *result;
				ExprDoneCond isDone;

				result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

				if (isDone != ExprEndResult)
				{
					node->js.ps.ps_TupFromTlist = (isDone == ExprMultipleResult);

					return result;
				}
			}
			else
			{
				InstrCountFiltered2(node, 1);
			}
		}
		else
		{
			InstrCountFiltered1(node, 1);
		}

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
	}

	return NULL;
}

void
ExecEndStreamTableJoin(StreamTableJoinState *node)
{
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

/*
 * ExecReScanStreamTableJoin
 *
 * Certain child nodes only call ReScan under certain conditions,
 * so we call it ourselves on all relevant nodes to make sure that
 * it's not skipped.
 */
void
ExecReScanStreamTableJoin(StreamTableJoinState *node)
{
	ListCell *lc;
	PlanState *ps = (PlanState *) node;
	List *nodes = list_concat(get_rescan_nodes(outerPlanState(ps)),
			get_rescan_nodes(innerPlanState(ps)));

	foreach(lc, nodes)
	{
		ps = (PlanState *) lfirst(lc);
		ExecReScan(ps);
	}

	node->stj_NeedNewOuter = true;
	list_free(nodes);
}
