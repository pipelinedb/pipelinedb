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

#include "access/htup_details.h"
#include "executor/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/nodeHash.h"
#include "executor/nodeHashjoin.h"
#include "executor/nodeStreamTablejoin.h"
#include "nodes/execnodes.h"
#include "utils/memutils.h"


#define HJ_FILL_HASHTABLE		1
#define HJ_NEED_NEW_OUTER		2
#define HJ_SCAN_BUCKET			3
#define HJ_FILL_OUTER_TUPLE		4
#define HJ_FILL_INNER_TUPLES	5
#define HJ_NEED_NEW_BATCH		6

/* Returns true if doing null-fill on outer relation */
#define HJ_FILL_OUTER(hjstate)	((hjstate)->hj_NullInnerTupleSlot != NULL)
/* Returns true if doing null-fill on inner relation */
#define HJ_FILL_INNER(hjstate)	((hjstate)->hj_NullOuterTupleSlot != NULL)


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

StreamTableJoinState *
ExecInitStreamTableJoin(StreamTableJoin *node, EState *estate, int eflags)
{
	ListCell *lc;
	StreamTableJoinState *state;
	List *lclauses = NIL;
	List *rclauses = NIL;
	List *hoperators = NIL;
	Hash *hashnode;
	HashState *hashstate;

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
	state->hashclauses = (List *)
		ExecInitExpr((Expr *) node->hashclauses,
					 (PlanState *) state);

	hashnode = (Hash *) innerPlan(node);
	outerPlanState(state) = ExecInitNode(outerPlan(node), estate, eflags);
	innerPlanState(state) = ExecInitNode((Plan *) hashnode, estate, eflags);

	/*
	 * tuple table initialization
	 */
	ExecInitResultTupleSlot(estate, &state->js.ps);
	state->hj_OuterTupleSlot = ExecInitExtraTupleSlot(estate);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			state->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(state)));
			break;
		case JOIN_RIGHT:
			state->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(outerPlanState(state)));
			break;
		case JOIN_FULL:
			state->hj_NullOuterTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(outerPlanState(state)));
			state->hj_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
								 ExecGetResultType(innerPlanState(state)));
			break;
		default:
			elog(ERROR, "unrecognized join type: %d",
				 (int) node->join.jointype);
	}

	/*
	 * now for some voodoo.  our temporary tuple slot is actually the result
	 * tuple slot of the Hash node (which is our inner plan).  we can do this
	 * because Hash nodes don't return tuples via ExecProcNode() -- instead
	 * the hash join node uses ExecScanHashBucket() to get at the contents of
	 * the hash table.  -cim 6/9/91
	 */
	{
		HashState  *hashstate = (HashState *) innerPlanState(state);
		TupleTableSlot *slot = hashstate->ps.ps_ResultTupleSlot;

		state->hj_HashTupleSlot = slot;
	}

	/*
	 * initialize tuple type and projection info
	 */
	ExecAssignResultTypeFromTL(&state->js.ps);
	ExecAssignProjectionInfo(&state->js.ps, NULL);

	ExecSetSlotDescriptor(state->hj_OuterTupleSlot,
						  ExecGetResultType(outerPlanState(state)));

	/*
	 * initialize hash-specific info
	 */
	state->hj_HashTable = NULL;
	state->hj_FirstOuterTupleSlot = NULL;
	state->hj_CurHashValue = 0;
	state->hj_CurBucketNo = 0;
	state->hj_CurSkewBucketNo = -1;
	state->hj_CurTuple = NULL;

	/*
	 * Deconstruct the hash clauses into outer and inner argument values, so
	 * that we can evaluate those subexpressions separately.  Also make a list
	 * of the hash operator OIDs, in preparation for looking up the hash
	 * functions to use.
	 */
	foreach(lc, state->hashclauses)
	{
		FuncExprState *fstate = (FuncExprState *) lfirst(lc);
		OpExpr	   *hclause;

		Assert(IsA(fstate, FuncExprState));
		hclause = (OpExpr *) fstate->xprstate.expr;
		Assert(IsA(hclause, OpExpr));
		lclauses = lappend(lclauses, linitial(fstate->args));
		rclauses = lappend(rclauses, lsecond(fstate->args));
		hoperators = lappend_oid(hoperators, hclause->opno);
	}
	state->hj_OuterHashKeys = lclauses;
	state->hj_InnerHashKeys = rclauses;
	state->hj_HashOperators = hoperators;

	/* child Hash node needs to evaluate inner hash keys, too */
	hashstate = (HashState *) innerPlanState(state);
	hashstate->hashkeys = rclauses;

	state->js.ps.ps_TupFromTlist = false;
	state->hj_JoinState = HJ_FILL_HASHTABLE;
	state->hj_MatchedOuter = false;
	state->hj_OuterNotEmpty = false;

	state->hj_HashTable = ExecHashTableCreate(hashnode, state->hj_HashOperators, HJ_FILL_INNER(state));

	return state;
}

/*
 * ExecHashJoinOuterGetTuple
 *
 *		get the next outer tuple for hashjoin: either by
 *		executing the outer plan node in the first pass, or from
 *		the temp files for the hashjoin batches.
 *
 * Returns a null slot if no more outer tuples (within the current batch).
 *
 * On success, the tuple's hash value is stored at *hashvalue --- this is
 * either originally computed, or re-read from the temp file.
 */
static TupleTableSlot *
ExecHashJoinOuterGetTuple(PlanState *outerNode,
						  StreamTableJoinState *stj,
						  uint32 *hashvalue)
{
	HashJoinTable hashtable = stj->hj_HashTable;
	TupleTableSlot *slot;

	/*
	 * Check to see if first outer tuple was already fetched by
	 * ExecHashJoin() and not used yet.
	 */
	slot = stj->hj_FirstOuterTupleSlot;
	if (!TupIsNull(slot))
		stj->hj_FirstOuterTupleSlot = NULL;
	else
		slot = ExecProcNode(outerNode);

	while (!TupIsNull(slot))
	{
		/*
		 * We have to compute the tuple's hash value.
		 */
		ExprContext *econtext = stj->js.ps.ps_ExprContext;

		econtext->ecxt_outertuple = slot;
		if (ExecHashGetHashValue(hashtable, econtext,
				stj->hj_OuterHashKeys,
								 true,		/* outer tuple */
								 HJ_FILL_OUTER(stj),
								 hashvalue))
		{
			/* remember outer relation is not empty for possible rescan */
			stj->hj_OuterNotEmpty = true;

			return slot;
		}

		/*
		 * That tuple couldn't match because of a NULL, so discard it and
		 * continue with the next one.
		 */
		slot = ExecProcNode(outerNode);
	}

	return NULL;
}

TupleTableSlot *
ExecStreamTableJoin(StreamTableJoinState *node)
{
	PlanState *outerNode;
	HashState *hashNode;
	List *joinqual;
	List *otherqual;
	ExprContext *econtext;
	ExprDoneCond isDone;
	TupleTableSlot *outerTupleSlot;
	uint32		hashvalue;
	int			batchno;

	/*
	 * get information from HashJoin node
	 */
	joinqual = node->js.joinqual;
	otherqual =  node->js.ps.qual;
	hashNode = (HashState *) innerPlanState(node);
	outerNode = outerPlanState(node);
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

	/*
	 * Run the hash join state machine
	 *
	 */
	for (;;)
	{
		switch (node->hj_JoinState)
		{
			case HJ_FILL_HASHTABLE:

				/*
				 * First time through: build hash table for inner relation.
				 */
				if (HJ_FILL_INNER(node))
				{
					/* no chance to not build the hash table */
					node->hj_FirstOuterTupleSlot = NULL;
				}
				else if (HJ_FILL_OUTER(node) ||
						 (outerNode->plan->startup_cost < hashNode->ps.plan->total_cost &&
						  !node->hj_OuterNotEmpty))
				{
					node->hj_FirstOuterTupleSlot = ExecProcNode(outerNode);
					if (TupIsNull(node->hj_FirstOuterTupleSlot))
					{
						node->hj_OuterNotEmpty = false;
						ExecReScan((PlanState *) node);
						return NULL;
					}
					else
						node->hj_OuterNotEmpty = true;
				}
				else
					node->hj_FirstOuterTupleSlot = NULL;

				/*
				 * Initialize the hash table
				 */
				ExecHashTableReset(node->hj_HashTable);

				/*
				 * Execute the Hash node to fill the hash table with a stream batch
				 */
				hashNode->hashtable = node->hj_HashTable;
				(void) MultiExecProcNode((PlanState *) hashNode);

				/*
				 * Reset OuterNotEmpty for scan.  (It's OK if we fetched a
				 * tuple above, because ExecHashJoinOuterGetTuple will
				 * immediately set it again.)
				 */
				node->hj_OuterNotEmpty = false;
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				/* FALL THRU */

			case HJ_NEED_NEW_OUTER:

				/*
				 * We don't have an outer tuple, try to get the next one
				 */
				outerTupleSlot = ExecHashJoinOuterGetTuple(outerNode,
														   node,
														   &hashvalue);

				if (TupIsNull(outerTupleSlot))
				{
					/* end of batch, or maybe whole join */
					if (HJ_FILL_INNER(node))
					{
						/* set up to scan for unmatched inner tuples */
						ExecPrepHashTableForUnmatched(node);
						node->hj_JoinState = HJ_FILL_INNER_TUPLES;
					}
					else
					{
						ExecReScan((PlanState *) node);

						return NULL;
					}
				}

				econtext->ecxt_outertuple = outerTupleSlot;
				node->hj_MatchedOuter = false;

				/*
				 * Find the corresponding bucket for this tuple in the main
				 * hash table or skew hash table.
				 */
				node->hj_CurHashValue = hashvalue;
				ExecHashGetBucketAndBatch(node->hj_HashTable, hashvalue,
										  &node->hj_CurBucketNo, &batchno);
				node->hj_CurTuple = NULL;

				/* OK, let's scan the bucket for matches */
				node->hj_JoinState = HJ_SCAN_BUCKET;

				/* FALL THRU */

			case HJ_SCAN_BUCKET:

				/*
				 * Scan the selected hash bucket for matches to current outer
				 */
				if (!ExecScanHashBucket(node, econtext))
				{
					/* out of matches; check for possible outer-join fill */
					node->hj_JoinState = HJ_FILL_OUTER_TUPLE;
					continue;
				}

				/*
				 * We've got a match, but still need to test non-hashed quals.
				 * ExecScanHashBucket already set up all the state needed to
				 * call ExecQual.
				 *
				 * If we pass the qual, then save state for next call and have
				 * ExecProject form the projection, store it in the tuple
				 * table, and return the slot.
				 *
				 * Only the joinquals determine tuple match status, but all
				 * quals must pass to actually return the tuple.
				 */
				if (joinqual == NIL || ExecQual(joinqual, econtext, false))
				{
					node->hj_MatchedOuter = true;
					HeapTupleHeaderSetMatch(HJTUPLE_MINTUPLE(node->hj_CurTuple));

					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->hj_JoinState = HJ_NEED_NEW_OUTER;
						continue;
					}

					/*
					 * In a semijoin, we'll consider returning the first
					 * match, but after that we're done with this outer tuple.
					 */
					if (node->js.jointype == JOIN_SEMI)
						node->hj_JoinState = HJ_NEED_NEW_OUTER;

					if (otherqual == NIL ||
						ExecQual(otherqual, econtext, false))
					{
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult)
						{
							node->js.ps.ps_TupFromTlist =
								(isDone == ExprMultipleResult);
							return result;
						}
					}
					else
						InstrCountFiltered2(node, 1);
				}
				else
					InstrCountFiltered1(node, 1);
				break;

			case HJ_FILL_OUTER_TUPLE:

				/*
				 * The current outer tuple has run out of matches, so check
				 * whether to emit a dummy outer-join tuple.  Whether we emit
				 * one or not, the next state is NEED_NEW_OUTER.
				 */
				node->hj_JoinState = HJ_NEED_NEW_OUTER;

				if (!node->hj_MatchedOuter &&
					HJ_FILL_OUTER(node))
				{
					/*
					 * Generate a fake join tuple with nulls for the inner
					 * tuple, and return it if it passes the non-join quals.
					 */
					econtext->ecxt_innertuple = node->hj_NullInnerTupleSlot;

					if (otherqual == NIL ||
						ExecQual(otherqual, econtext, false))
					{
						TupleTableSlot *result;

						result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

						if (isDone != ExprEndResult)
						{
							node->js.ps.ps_TupFromTlist =
								(isDone == ExprMultipleResult);
							return result;
						}
					}
					else
						InstrCountFiltered2(node, 1);
				}
				break;

			case HJ_FILL_INNER_TUPLES:

				/*
				 * We have finished a batch, but we are doing right/full join,
				 * so any unmatched inner tuples in the hashtable have to be
				 * emitted before we continue to the next batch.
				 */
				if (!ExecScanHashTableForUnmatched(node, econtext))
				{
					/* no more unmatched tuples */
					node->hj_JoinState = HJ_NEED_NEW_OUTER;
					continue;
				}

				/*
				 * Generate a fake join tuple with nulls for the outer tuple,
				 * and return it if it passes the non-join quals.
				 */
				econtext->ecxt_outertuple = node->hj_NullOuterTupleSlot;

				if (otherqual == NIL ||
					ExecQual(otherqual, econtext, false))
				{
					TupleTableSlot *result;

					result = ExecProject(node->js.ps.ps_ProjInfo, &isDone);

					if (isDone != ExprEndResult)
					{
						node->js.ps.ps_TupFromTlist =
							(isDone == ExprMultipleResult);
						return result;
					}
				}
				else
					InstrCountFiltered2(node, 1);
				break;

			default:
				elog(ERROR, "unrecognized hashjoin state: %d",
					 (int) node->hj_JoinState);
		}
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

	if (node->hj_HashTable->nbatch == 1 &&
		node->js.ps.righttree->chgParam == NULL)
	{
		/*
		 * Okay to reuse the hash table; needn't rescan inner, either.
		 *
		 * However, if it's a right/full join, we'd better reset the
		 * inner-tuple match flags contained in the table.
		 */
		if (HJ_FILL_INNER(node))
			ExecHashTableResetMatchFlags(node->hj_HashTable);

		node->hj_OuterNotEmpty = false;
		node->hj_JoinState = HJ_FILL_HASHTABLE;
	}

	/* Always reset intra-tuple state */
	node->hj_CurHashValue = 0;
	node->hj_CurBucketNo = 0;
	node->hj_CurSkewBucketNo = INVALID_SKEW_BUCKET_NO;
	node->hj_CurTuple = NULL;
	node->hj_MatchedOuter = false;
	node->hj_FirstOuterTupleSlot = NULL;
	node->js.ps.ps_TupFromTlist = false;

	foreach(lc, nodes)
	{
		ps = (PlanState *) lfirst(lc);
		ExecReScan(ps);
	}

	list_free(nodes);
}
