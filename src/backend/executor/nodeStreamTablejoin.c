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
#include "utils/memutils.h"

StreamTableJoinState *
ExecInitStreamTableJoin(StreamTableJoin *node, EState *estate, int eflags)
{
	StreamTableJoinState *state;

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
			state->nullslot =
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
	state->needouter = true;
	state->matchedouter = false;

	/* 10kB per event seems like a reasonable amount of memory to reserve for a batch */
	state->streambatch = tuplestore_begin_heap(false, true, 10 * 1024 * estate->cq_batch_size);
	state->batchsize = 0;

	return state;
}

static TupleTableSlot *
stream_batch_next(StreamTableJoinState *node)
{
	PlanState *inner = innerPlanState(node);
	TupleTableSlot *slot = inner->ps_ResultTupleSlot;

	if (!tuplestore_gettupleslot(node->streambatch, true, false, slot))
		ExecClearTuple(slot);

	return slot;
}

static int
has_stream_batch(StreamTableJoinState *node)
{
	PlanState *inner = innerPlanState(node);
	TupleTableSlot *slot;

	if (node->batchsize)
		return node->batchsize;

	for (;;)
	{
		slot = ExecProcNode(inner);
		if (TupIsNull(slot))
			break;

		tuplestore_puttupleslot(node->streambatch, slot);
		node->batchsize++;
	}

	tuplestore_rescan(node->streambatch);

	return node->batchsize;
}

static void
batch_done(StreamTableJoinState *node)
{
	tuplestore_clear(node->streambatch);
	node->batchsize = 0;
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
	if (!has_stream_batch(node))
		return NULL;

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->needouter)
		{
			outerTupleSlot = ExecProcNode(outerPlan);

			if (TupIsNull(outerTupleSlot))
			{
				ExecReScan(outerPlan);
				batch_done(node);

				return NULL;
			}

			econtext->ecxt_outertuple = outerTupleSlot;
			node->needouter = false;
			node->matchedouter = false;

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
			 tuplestore_rescan(node->streambatch);
		}

		innerTupleSlot = stream_batch_next(node);
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot))
		{
			node->needouter = true;

			if (!node->matchedouter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nullslot;

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
			node->matchedouter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->needouter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * In a semijoin, we'll consider returning the first match, but
			 * after that we're done with this outer tuple.
			 */
			if (node->js.jointype == JOIN_SEMI)
				node->needouter = true;

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
}

void
ExecEndStreamTableJoin(StreamTableJoinState *node)
{
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));
}

void
ExecReScanStreamTableJoin(StreamTableJoinState *node)
{

}
