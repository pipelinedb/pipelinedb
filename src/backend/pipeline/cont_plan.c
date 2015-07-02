/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cont_plan.c
 * 		Functionality for generating/modifying CQ plans
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_plan.c
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
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/parse_oper.h"
#include "pipeline/cont_plan.h"
#include "pipeline/cqanalyze.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"


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

static List *
make_tupstore_tlist(TupleDesc desc)
{
	AttrNumber attno;
	List *tlist = NIL;

	for (attno = 1; attno <= desc->natts; attno++)
	{
		Form_pg_attribute attr = desc->attrs[attno - 1];
		Var *var = makeVar(1, attno, attr->atttypid, attr->atttypmod, attr->attcollation, 0);
		TargetEntry *entry = makeTargetEntry((Expr *) var, attno, NameStr(attr->attname), false);

		tlist = lappend(tlist, entry);
	}

	return tlist;
}

/*
 * get_combiner_join_rel
 *
 * Gets the input rel for a combine plan, which only ever needs to read from a TuplestoreScan
 * because the workers have already done most of the work
 */
static RelOptInfo *
get_combiner_join_rel(PlannerInfo *root, int levels_needed, List *initial_rels)
{
	RelOptInfo *rel;
	Path *path;

	rel = standard_join_search(root, levels_needed, initial_rels);
	rel->pathlist = NIL;

	path =  create_tuplestore_scan_path(rel);

	add_path(rel, path);
	set_cheapest(rel);

	return rel;
}

/*
 * CQ combiners expect transition values from worker processes, so we need
 * to modify the combiner's Aggrefs accordingly.
 */
static void
set_plan_refs(PlannedStmt *pstmt, ContinuousView *view)
{
	Plan *plan = pstmt->planTree;
	ListCell *lc;
	AttrNumber attno = 1;
	List *targetlist = NIL;
	TupleDesc matdesc;
	Relation matrel;
	int i;
	Agg *agg;

	if (!IsA(plan, Agg))
		return;

	agg = (Agg *) plan;

	matrel = heap_openrv(view->matrel, NoLock);
	matdesc = CreateTupleDescCopyConstr(RelationGetDescr(matrel));
	heap_close(matrel, NoLock);

	/*
	 * There are two cases we need to handle here:
	 *
	 * 1. If we're a worker process, we need to set any Aggref output types
	 *    to the type of their transition out function, if the agg has one.
	 *
	 * 2. If we're a combiner process, we need to set the Aggref's input
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
			Oid hiddentype = GetCombineStateColumnType(te->expr);
			AttrNumber hidden = 0;
			Oid transtype;

			/* The hidden column is always stored adjacent to the column for the Aggref */
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
			if (pstmt->is_combine)
			{
				Var *v = makeVar(OUTER_VAR, attno, transtype, InvalidOid, InvalidOid, 0);
				TargetEntry *arte = makeTargetEntry((Expr *) v, 1, toappend->resname, false);

				aggref->args = list_make1(arte);
				aggref->aggresultstate = AGG_COMBINE;
			}

			aggref->aggtype = transtype;

			/* CQs have their own way of handling DISTINCT */
			aggref->aggdistinct = NIL;
		}
		else
		{
			if (pstmt->is_combine)
			{
				/*
				 * Replace any non-Aggref expression with a Var which has the same type
				 * as this TargetEntry's expr and its varattno is equal to the resno of
				 * this TargetEntry.
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
					Oid typcoll = exprCollation((Node *) expr);
					var = makeVar(1, toappend->resno, type, typmod, typcoll, 0);
				}

				var->varattno = attno;
				var->vartype = matdesc->attrs[attno - 1]->atttypid;
				te->expr = (Expr *) var;

				/* Fix grpColIdx to reflect the index in the tuple from worker */
				if (AttributeNumberIsValid(oldVarAttNo) && oldVarAttNo != var->varattno)
				{
					Oid eq;

					for (i = 0; i < agg->numCols; i++)
					{
						if (agg->grpColIdx[i] != oldVarAttNo)
							continue;

						get_sort_group_operators(exprType((Node *) var),
								false, true, false, NULL, &eq, NULL, NULL);

						agg->grpColIdx[i] = var->varattno;
						agg->grpOperators[i] = eq;
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
	 * attribute ordering when creating materialization tables which we can rely on here.
	 *
	 * For now, let's just explode if there is an inconsistency detected here. This would
	 * be a bad error for a user to get though, because there's nothing they can do about it.
	 */
	if (list_length(targetlist) != matdesc->natts)
		elog(ERROR, "continuous query target list is inconsistent with materialization table schema");

	for (i = 0; i < matdesc->natts; i++)
	{
		TargetEntry *te = list_nth(targetlist, i);
		if (pg_strcasecmp(te->resname, NameStr(matdesc->attrs[i]->attname)) != 0)
			elog(ERROR, "continuous query target list is inconsistent with materialization table schema");
	}

	plan->targetlist = targetlist;

	if (pstmt->is_combine)
	{
		RangeTblEntry *rte;
		char *refname;


		/* Make the TuplestoreScan's target list mimic the TupleDesc of the materialization table */
		if (IsA(plan->lefttree, TuplestoreScan))
			plan->lefttree->targetlist = make_tupstore_tlist(matdesc);

		/* The materialization table's RTE is used as a pseudo RTE for the TuplestoreScan */
		rte = makeNode(RangeTblEntry);
		refname = view->matrel->relname;
		rte->rtekind = RTE_RELATION;
		rte->alias = view->matrel->alias;
		rte->inFromCl = true;
		rte->requiredPerms = ACL_SELECT;
		rte->checkAsUser = InvalidOid;
		rte->selectedCols = NULL;
		rte->modifiedCols = NULL;
		rte->relname = refname;

		rte->eref = makeAlias(refname, NIL);
		rte->relkind = RELKIND_RELATION;
		rte->relid = matdesc->attrs[0]->attrelid;

		/* Replace the rtable list with a single RTE for the matrel */
		list_free_deep(pstmt->rtable);
		pstmt->rtable = list_make1(rte);
	}
}

static PlannedStmt *
get_plan_from_stmt(Oid id, Node *node, const char *sql, bool is_combine)
{
	Query *query;
	PlannedStmt	*plan;

	query = linitial(pg_analyze_and_rewrite(node, sql, NULL, 0));
	query->isContinuous = true;
	query->isCombine = is_combine;
	query->cq_id = id;

	plan = pg_plan_query(query, 0, NULL);
	plan->is_continuous = true;
	plan->is_combine = is_combine;
	plan->cq_id = id;

	/*
	 * Unique plans get transformed into ContinuousUnique plans for
	 * continuous query processes.
	 */
	if (IsA(plan->planTree, Unique))
	{
		ContinuousUnique *cunique = makeNode(ContinuousUnique);
		Unique *unique = (Unique *) plan->planTree;

		memcpy((char *) &cunique->unique, (char *) unique, sizeof(Unique));

		cunique->cq_id = id;
		cunique->unique.plan.type = T_ContinuousUnique;

		plan->planTree = (Plan *) cunique;

		Assert(IsA(plan->planTree->lefttree, Sort));

		/* Strip out the sort since its not needed */
		plan->planTree->lefttree = plan->planTree->lefttree->lefttree;
	}

	return plan;
}

static PlannedStmt*
get_worker_plan(ContinuousView *view)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(view->query);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = GetSelectStmtForCQWorker(selectstmt, NULL);
	selectstmt->forContinuousView = true;

	return get_plan_from_stmt(view->id, (Node *) selectstmt, view->query, false);
}

static PlannedStmt*
get_combiner_plan(ContinuousView *view)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;
	PlannedStmt *result;

	parsetree_list = pg_parse_query(view->query);
	Assert(list_length(parsetree_list) == 1);

	join_search_hook = get_combiner_join_rel;
	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = GetSelectStmtForCQCombiner(selectstmt);
	selectstmt->forContinuousView = true;

	result = get_plan_from_stmt(view->id, (Node *) selectstmt, view->query, true);
	join_search_hook = NULL;

	return result;
}

PlannedStmt *
GetContPlan(ContinuousView *view)
{
	PlannedStmt *plan;
	ContQueryProcType type;

	if (MyContQueryProc)
		type = MyContQueryProc->type;
	else
		type = Scheduler; /* dummy invalid type */

	switch (type)
	{
	case Combiner:
		plan = get_combiner_plan(view);
		break;
	case Worker:
		plan = get_worker_plan(view);
		break;
	default:
		ereport(ERROR, (errmsg("only continuous query processes can generate continuous query plans")));
	}

	set_plan_refs(plan, view);

	return plan;
}

/*
 * SetCombinerPlanTuplestorestate
 */
TuplestoreScan *
SetCombinerPlanTuplestorestate(PlannedStmt *plan, Tuplestorestate *tupstore)
{
	TuplestoreScan *scan;

	if (IsA(plan->planTree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree;
	else if ((IsA(plan->planTree, Agg) || IsA(plan->planTree, ContinuousUnique)) &&
			IsA(plan->planTree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree;
	else if (IsA(plan->planTree, Agg) &&
			IsA(plan->planTree->lefttree, Sort) &&
			IsA(plan->planTree->lefttree->lefttree, TuplestoreScan))
		scan = (TuplestoreScan *) plan->planTree->lefttree->lefttree;
	else
		elog(ERROR, "couldn't find TuplestoreScan node in combiner's plan");

	scan->store = tupstore;

	return scan;
}
