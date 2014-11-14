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
#include "catalog/pipeline_query_fn.h"
#include "commands/pipelinecmds.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "pipeline/cqanalyze.h"
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
	combTuple = SearchSysCache2(PIPELINECOMBINETRANSFNOID,
			ObjectIdGetDatum(aggform->aggfinalfn),
			ObjectIdGetDatum(aggform->aggtransfn));

	if (HeapTupleIsValid(combTuple))
	{
		combform = (Form_pipeline_combine) GETSTRUCT(combTuple);
		if (OidIsValid(combform->transouttype))
			result = combform->transouttype;

		ReleaseSysCache(combTuple);
	}

	return result;
}

static TargetEntry *
make_store_target(TargetEntry *tostore, char *resname, AttrNumber attno, Oid aggtype, Oid transtype)
{
	TargetEntry *argte;
	TargetEntry *result;
	Var *var;
	Aggref *aggref = makeNode(Aggref);
	Aggref *sibling;

	if (!IsA(tostore->expr, Aggref))
		elog(ERROR, "store aggrefs can only store the results of other aggrefs");

	sibling = (Aggref *) tostore->expr;

	aggref->aggfnoid = sibling->aggfnoid;
	aggref->aggtype = aggtype;
	aggref->aggfilter = InvalidOid;
	aggref->aggstar = InvalidOid;
	aggref->aggvariadic = false;
	aggref->aggkind = AGGKIND_STORE;
	aggref->location = sibling->location;
	aggref->aggresultstate = AGG_FINALIZE_COMBINE;

	var = makeVar(OUTER_VAR, attno, transtype, InvalidOid, InvalidOid, 0);
	var->varoattno = attno + 1;

	argte = makeTargetEntry((Expr *) var, 1, NULL, false);
	aggref->args = list_make1(argte);

	result = makeTargetEntry((Expr *) aggref, attno, resname, false);

	return result;
}

/*
 * CQ combiners expect transition values from worker processes, so we need
 * to modify the combiner's Aggrefs accordingly.
 */
void
SetCQPlanRefs(PlannedStmt *pstmt, char* matrelname)
{
	Plan *plan = pstmt->planTree;
	ListCell *lc;
	AttrNumber attno = 1;
	List *targetlist = NIL;
	TupleDesc matdesc;
	CQProcessType ptype = pstmt->cq_state->ptype;
	int i;
	Agg *agg;

	if (!IsA(plan, Agg))
		return;

	agg = (Agg *) plan;

	matdesc = RelationNameGetTupleDesc(matrelname);

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
		TargetEntry *toappend = te;
		char *origresname;
		TargetEntry *storete = NULL;

		/* Ignore junk columns from the targetlist */
		if (te->resjunk)
			continue;

		origresname = pstrdup(toappend->resname);

		if (IsA(expr, Aggref))
		{
			Aggref *aggref = (Aggref *) expr;
			Oid hiddentype = GetCombineStateColumnType(te);
			AttrNumber hidden = 0;
			Oid transtype;

			/* The hidden column is always stored adjacent to the column for the aggref */
			if (OidIsValid(hiddentype))
				hidden = attno + 1;

			aggref->aggresultstate = AGG_TRANSITION;
			transtype = get_trans_type(aggref);

			/*
			 * If this Aggref has hidden state associated with it, we create an Aggref
			 * that finalizes the transition state from the hidden column and stores
			 * it into the visible column.
			 *
			 * This must happen before the combiner transform that follows this block because the
			 * input to the Aggref will the hidden column (if one exists).
			 */
			if (AttributeNumberIsValid(hidden))
			{
				storete = make_store_target(te, origresname, attno, aggref->aggtype, transtype);
				toappend->resname = NameStr(matdesc->attrs[attno]->attname);
				attno++;
			}

			/*
			 * If we're a combiner, this Aggref will take one argument whose type is
			 * the transition output type of this Aggref on the worker.
			 */
			if (ptype == CQCombiner)
			{
				Var *v = makeVar(OUTER_VAR, attno, transtype, InvalidOid, InvalidOid, 0);
				TargetEntry *arte = makeTargetEntry((Expr *) v, 1, toappend->resname, false);

				aggref->args = list_make1(arte);
				aggref->aggresultstate = AGG_COMBINE;
			}

			aggref->aggtype = transtype;
		}
		else
		{
			if (ptype == CQCombiner)
			{
				/*
				 * Replace any non-Aggref expression with a Var
				 * which has the same type and this TargetEntry's expr
				 * and its varattno is equal to the resno of this
				 * TargetEntry.
				 */
				Var *var;
				AttrNumber oldVarAttNo = 0;

				if (IsA(expr, Var))
				{
					var = (Var *) expr;
					oldVarAttNo = var->varattno;
				}
				else
				{
					Oid type = exprType((Node *) expr);
					int32 typmod = exprTypmod((Node *) expr);
					var = makeVar(OUTER_VAR, toappend->resno, type, typmod, InvalidOid, 0);
				}

				var->varattno = attno;
				te->expr = (Expr *) var;

				/* Fix grpColIdx to reflect the index in the tuple from worker */
				if (AttributeNumberIsValid(oldVarAttNo) && oldVarAttNo != var->varattno)
				{
					for (i = 0; i < agg->numCols; i++)
					{
						if (agg->grpColIdx[i] == oldVarAttNo)
							agg->grpColIdx[i] = var->varattno;
					}
				}
			}
		}

		/* add the extra store Aggref */
		if (storete)
			targetlist = lappend(targetlist, storete);

		toappend->resno = attno;
		targetlist = lappend(targetlist, toappend);
		attno++;
	}

	/*
	 * XXX(derek) At this point, the target list should match the tuple descriptor of the
	 * materialization table. However, this assumes things about the ordering of columns
	 * and their corresponding hidden columns. Ideally, the combiner and worker should be
	 * able to work with any ordering of attributes as long as they're all present. Then,
	 * when combining with on-disk tuples, we could reorder attributes as necessary if
	 * we detect different orderings. Another option is to have strong guarantees about
	 * attribute ordering when creating materializtion tables which we can rely on here.
	 *
	 * For now, let's just explode if there is an
	 * inconsistency detected here. This would be a shitty error for a user to get though,
	 * because there's nothing they can do about it.
	 */
	if (list_length(targetlist) != matdesc->natts)
		elog(ERROR, "continuous query target list is inconsistent with materialization table schema");

	for (i = 0; i < matdesc->natts; i++)
	{
		TargetEntry *te = list_nth(targetlist, i);
		if (strcmp(te->resname, NameStr(matdesc->attrs[i]->attname)) != 0)
			elog(ERROR, "continuous query target list is inconsistent with materialization table schema");
	}

	plan->targetlist = targetlist;

	/*
	 * This is where the combiner gets its input rows from, so it needs to expect whatever types
	 * the worker is going to output
	 */
	if (IsA(plan->lefttree, TuplestoreScan))
		plan->lefttree->targetlist = targetlist;
}
