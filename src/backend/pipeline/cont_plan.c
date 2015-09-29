/*-------------------------------------------------------------------------
 *
 * cont_plan.c
 * 		Functionality for generating/modifying CQ plans
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_plan.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
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
#include "parser/analyze.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_plan.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"


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
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, NULL, Worker);

	return get_plan_from_stmt(view->id, (Node *) selectstmt, view->query, selectstmt->forCombiner);
}

static PlannedStmt*
get_combiner_plan(ContinuousView *view)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;
	PlannedStmt *result;

	parsetree_list = pg_parse_query(view->query);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, NULL, Combiner);
	join_search_hook = get_combiner_join_rel;

	PG_TRY();
	{
		result = get_plan_from_stmt(view->id, (Node *) selectstmt, view->query, true);
		join_search_hook = NULL;
		post_parse_analyze_hook = NULL;
	}
	PG_CATCH();
	{
		/*
		 * These hooks won't be reset if there's an error, so we need to make
		 * sure that they're not set for whatever query is run next in this xact.
		 */
		join_search_hook = NULL;
		post_parse_analyze_hook = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

PlannedStmt *
GetContPlan(ContinuousView *view, ContQueryProcType type)
{
	PlannedStmt *plan = NULL;

	Assert(type == Worker || type == Combiner);

	if (type == Worker)
		plan = get_worker_plan(view);
	else if (type == Combiner)
		plan = get_combiner_plan(view);

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

/*
 * GetGroupHashIndexExpr
 *
 * Returns the function expression used to index the given matrel
 */
FuncExpr *
GetGroupHashIndexExpr(int group_len, ResultRelInfo *ri)
{
	FuncExpr *result = NULL;
	int i;

	/*
	 * In order for the hashed group index to be usable, we must use an expression
	 * that is equivalent to the index expression in the group lookup. The best way
	 * to do this is to just copy the actual index expression.
	 */
	for (i = 0; i < ri->ri_NumIndices; i++)
	{
		IndexInfo *idx = ri->ri_IndexRelationInfo[i];
		Node *n;
		FuncExpr *func;

		if (!idx->ii_Expressions || list_length(idx->ii_Expressions) != 1)
			continue;

		n = linitial(idx->ii_Expressions);
		if (!IsA(n, FuncExpr))
			continue;

		func = (FuncExpr *) n;
		if ((func->funcid != HASH_GROUP_OID && func->funcid != LS_HASH_GROUP_OID) ||
				list_length(func->args) != group_len)
			continue;

		result = copyObject(func);
		break;
	}

	return result;
}
