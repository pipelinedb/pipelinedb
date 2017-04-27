/*-------------------------------------------------------------------------
 *
 * planner.c
 * 		Functionality for generating/modifying CQ plans
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/planner.c
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_combine.h"
#include "catalog/pipeline_query_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
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
#include "pipeline/analyzer.h"
#include "pipeline/planner.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

ProcessUtility_hook_type SaveUtilityHook = NULL;

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
	query->cqId = id;

	plan = pg_plan_query(query, 0, NULL);

	plan->isContinuous = true;

	return plan;
}

static SelectStmt *
get_worker_select_stmt(ContQuery* view, SelectStmt** viewptr)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt->swStepFactor = view->sw_step_factor;
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt,
												   viewptr, Worker);

	return selectstmt;
}

static PlannedStmt *
get_worker_plan(ContQuery *view)
{
	SelectStmt* stmt = get_worker_select_stmt(view, NULL);
	return get_plan_from_stmt(view->id, (Node *) stmt, view->sql, false);
}

static PlannedStmt *
get_plan_with_hook(Oid id, Node *node, const char* sql, bool is_combine)
{
	PlannedStmt *result;
	post_parse_analyze_hook_type save_post_parse_analyze_hook = post_parse_analyze_hook;

	join_search_hook = get_combiner_join_rel;

	PG_TRY();
	{
		result = get_plan_from_stmt(id, node, sql, is_combine);
		join_search_hook = NULL;
		post_parse_analyze_hook = save_post_parse_analyze_hook;
	}
	PG_CATCH();
	{
		/*
		 * These hooks won't be reset if there's an error, so we need to make
		 * sure that they're not set for whatever query is run next in this xact.
		 */
		join_search_hook = NULL;
		post_parse_analyze_hook = save_post_parse_analyze_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

PlannedStmt *
GetContinuousViewOverlayPlan(ContQuery *view)
{
	SelectStmt	*selectstmt;
	SelectStmt	*viewstmt;

	selectstmt = get_worker_select_stmt(view, &viewstmt);
	selectstmt = viewstmt;

	return get_plan_with_hook(view->id, (Node*) selectstmt,
							  view->sql, false);
}

PlannedStmt *
GetContinuousViewOverlayPlanMod(ContQuery *view, ViewModFunction mod_fn)
{
	SelectStmt	*selectstmt;
	SelectStmt	*viewstmt;

	selectstmt = get_worker_select_stmt(view, &viewstmt);
	selectstmt = viewstmt;

	mod_fn(selectstmt);

	return get_plan_with_hook(view->id, (Node*) selectstmt,
							  view->sql, false);
}

static PlannedStmt *
get_combiner_plan(ContQuery *view)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt->swStepFactor = view->sw_step_factor;
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, NULL, Combiner);
	join_search_hook = get_combiner_join_rel;

	return get_plan_with_hook(view->id, (Node*) selectstmt, view->sql, true);
}

PlannedStmt *
GetContPlan(ContQuery *view, ContQueryProcType type)
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
	else if ((IsA(plan->planTree, Agg)) &&
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
GetGroupHashIndexExpr(ResultRelInfo *ri)
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
		if (func->funcid != HASH_GROUP_OID && func->funcid != LS_HASH_GROUP_OID)
			continue;

		result = copyObject(func);
		break;
	}

	return result;
}


EState *
CreateEState(QueryDesc *query_desc)
{
	EState *estate;

	estate = CreateExecutorState();
	estate->es_param_list_info = query_desc->params;
	estate->es_snapshot = RegisterSnapshot(query_desc->snapshot);
	estate->es_crosscheck_snapshot =
		RegisterSnapshot(query_desc->crosscheck_snapshot);
	estate->es_instrument = query_desc->instrument_options;
	estate->es_range_table = query_desc->plannedstmt->rtable;
	estate->es_continuous = query_desc->plannedstmt->isContinuous;
	estate->es_lastoid = InvalidOid;
	estate->es_processed = estate->es_filtered = 0;

	if (query_desc->plannedstmt->nParamExec > 0)
		estate->es_param_exec_vals = (ParamExecData *)
			palloc0(query_desc->plannedstmt->nParamExec *
					sizeof(ParamExecData));

	estate->es_top_eflags |= EXEC_FLAG_SKIP_TRIGGERS;

	return estate;
}

void
SetEStateSnapshot(EState *estate)
{
	estate->es_snapshot = GetTransactionSnapshot();
	estate->es_snapshot->active_count++;
	estate->es_snapshot->copied = true;
	PushActiveSnapshot(estate->es_snapshot);
}

void
UnsetEStateSnapshot(EState *estate)
{
	PopActiveSnapshot();
	estate->es_snapshot = NULL;
}

/*
 * get_res_target
 */
static ResTarget *
get_res_target(char *name, List *tlist)
{
	ListCell *lc;

	Assert(name);

	foreach(lc, tlist)
	{
		ResTarget *rt = (ResTarget *) lfirst(lc);
		if (pg_strcasecmp(rt->name, name) == 0)
			return rt;
	}

	elog(ERROR, "column \"%s\" does not exist", name);

	return NULL;
}

/*
 * create_index_on_matrel
 *
 * Given a CREATE INDEX query on a continuous view, modify it to
 * create the index on the continuous view's matrel instead.
 */
static void
create_index_on_matrel(IndexStmt *stmt)
{
	ContQuery *cv;
	RangeVar *cv_name = stmt->relation;
	SelectStmt	*viewstmt;
	List *tlist;
	ListCell *lc;
	bool is_sw = IsSWContView(cv_name);

	stmt->relation = GetMatRelName(cv_name);
	cv = GetContQueryForView(cv_name);
	get_worker_select_stmt(cv, &viewstmt);

	Assert(viewstmt);
	tlist = viewstmt->targetList;

	foreach(lc, stmt->indexParams)
	{
		IndexElem *elem = (IndexElem *) lfirst(lc);
		ResTarget *res;

		if (!elem->name)
			continue;

		/*
		 * If the column isn't a plain column reference, then it's wrapped in
		 * a finalize call so we need to index on that with an expression index
		 */
		res = get_res_target(elem->name, tlist);
		if (!IsA(res->val, ColumnRef))
		{
			if (is_sw)
				elog(ERROR, "sliding-window aggregate columns cannot be indexed");
			elem->expr = copyObject(res->val);
			elem->name = NULL;
		}
	}
}

/*
 * ProcessUtilityOnContView
 *
 * Hook to intercept relevant utility queries run on continuous views
 */
void
ProcessUtilityOnContView(Node *parsetree, const char *sql, ProcessUtilityContext context,
													  ParamListInfo params, DestReceiver *dest, char *tag)
{
	if (IsA(parsetree, IndexStmt))
	{
		IndexStmt *stmt = (IndexStmt *) parsetree;
		if (IsAContinuousView(stmt->relation))
			create_index_on_matrel(stmt);
	}
	else if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vstmt = (VacuumStmt *) parsetree;
		/*
		 * If the user is trying to vacuum a CV, what they're really
		 * trying to do is create it on the CV's materialization table, so rewrite
		 * the name of the target relation if we need to.
		 */
		if (vstmt->relation && IsAContinuousView(vstmt->relation))
			vstmt->relation = GetMatRelName(vstmt->relation);
	}

	if (SaveUtilityHook != NULL)
		(*SaveUtilityHook) (parsetree, sql, context, params, dest, tag);
	else
		standard_ProcessUtility(parsetree, sql, context, params, dest, tag);
}
