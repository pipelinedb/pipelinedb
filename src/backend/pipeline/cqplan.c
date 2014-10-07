/*-------------------------------------------------------------------------
 *
 * cqplan.h
 * 		Functionality for generating/modifying CQ plans
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqplan.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pipeline_combine.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "pipeline/cqplan.h"
#include "utils/syscache.h"

#define AGGKIND_IS_CQ_COMBINER(aggkind) ((aggkind) == AGGKIND_CQ_COMBINER)
#define AGGKIND_IS_CQ_WORKER(aggkind) ((aggkind) == AGGKIND_CQ_WORKER)


static Oid
get_trans_type(Aggref *agg)
{
	Oid result = InvalidOid;
	HeapTuple combTuple;
	HeapTuple	aggTuple = SearchSysCache(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid), 0, 0, 0);
	Form_pg_aggregate aggform;
	Form_pipeline_combine combform;

	if (!HeapTupleIsValid(aggTuple))
		elog(ERROR, "could not determine transition type: cache lookup failed for aggregate %u", agg->aggfnoid);

	aggform = (Form_pg_aggregate) GETSTRUCT(aggTuple);
	ReleaseSysCache(aggTuple);

	result = aggform->aggtranstype;

	/*
	 * If we have a transition out function, its output type will be the
	 * transition output type
	 */
	combTuple = SearchSysCache1(PIPELINECOMBINETRANSFNOID, ObjectIdGetDatum(aggform->aggtransfn));
	if (HeapTupleIsValid(combTuple))
	{
		combform = (Form_pipeline_combine) GETSTRUCT(combTuple);
		if (OidIsValid(combform->transouttype))
			result = combform->transouttype;

		ReleaseSysCache(combTuple);
	}

	return result;
}

/*
 * CQ combiners expect transition values from worker processes, so we need
 * to modify the combiner's Aggrefs accordingly.
 */
void
SetCQPlanRefs(PlannedStmt *pstmt)
{
	Plan *plan = pstmt->planTree;
	Agg *agg;
	ListCell *lc;

	if (!IsA(plan, Agg))
		return;

	agg = (Agg *) plan;

	if (pstmt->cq_state->ptype == CQWorker)
		agg->resultState = AGG_TRANSITION;
	else if (pstmt->cq_state->ptype == CQCombiner)
		agg->resultState = AGG_COMBINE;

	/*
	 * There are two cases we need to handle here:
	 *
	 * 1. If we're a worker process, we need to set any aggref output types
	 *    to the type of their transition out function, if the agg has one.
	 *
	 * 2. If we're a combiner process, we need to set the aggref's input
	 *    type to the output type of worker processes.
	 */
	foreach(lc, plan->targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Expr *expr = (Expr *) te->expr;
		if (IsA(expr, Aggref))
		{
			Aggref *aggref = (Aggref *) expr;
			ListCell *lc;
			Oid transtype = get_trans_type(aggref);

			if (agg->resultState == AGG_TRANSITION)
				aggref->aggtype = transtype;

			if (agg->resultState != AGG_COMBINE)
				continue;

			foreach(lc, aggref->args)
			{
				TargetEntry *t = (TargetEntry *) lfirst(lc);
				Var *v;

				if (!IsA(t->expr, Var))
					elog(ERROR, "unsupported continuous aggregate argument type: %d", nodeTag(t->expr));

				v = (Var *) t->expr;
				v->vartype = transtype;
			}
		}
	}
}
