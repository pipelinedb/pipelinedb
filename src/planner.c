/*-------------------------------------------------------------------------
 *
 * planner.c
 * 		Functionality for generating/modifying CQ plans
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 */
#include "postgres.h"

#include "analyzer.h"
#include "catalog.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "compat.h"
#include "config.h"
#include "executor/executor.h"
#include "matrel.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "physical_group_lookup.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "planner.h"
#include "rewrite/rewriteHandler.h"
#include "tcop/tcopprot.h"
#include "tuplestore_scan.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

planner_hook_type save_planner_hook = NULL;

/*
 * InstallPlannerHooks
 */
void
InstallPlannerHooks(void)
{
	save_planner_hook = planner_hook;
	planner_hook = PipelinePlanner;
}

/*
 * make_aggs_partial
 */
static void
make_aggs_partial(Plan *plan)
{
	ListCell *tlc;
	Agg *agg_plan;

	/* Pull the Agg plan out */
	for (;;)
	{
		if (plan == NULL)
			break;
		if (IsA(plan, Agg))
			break;
		plan = outerPlan(plan);
	}

	/* If there are no aggregates, there's nothing to do */
	if (plan == NULL)
		return;

	agg_plan = (Agg *) plan;
	agg_plan->aggsplit = AGGSPLIT_INITIAL_SERIAL;

	foreach(tlc, plan->targetlist)
	{
		ListCell *lc;
		TargetEntry *te = (TargetEntry *) lfirst(tlc);
		List *nodes = pull_var_clause((Node *) te->expr, PVC_INCLUDE_AGGREGATES);

		foreach(lc, nodes)
		{
			Node *n = lfirst(lc);
			Aggref *agg;

			if (!IsA(n, Aggref))
				continue;

			agg = (Aggref *) n;
			mark_partial_aggref(agg, AGGSPLIT_INITIAL_SERIAL);
		}
	}
}

/*
 * make_aggs_combinable
 */
static void
make_aggs_combinable(Plan *combiner_plan, Plan *worker_plan)
{
	ListCell *tlc;
	Agg *combiner_agg_plan;
	Agg *worker_agg_plan;

	/* Pull the Agg plan out */
	for (;;)
	{
		if (combiner_plan == NULL)
			break;
		if (IsA(combiner_plan, Agg))
			break;
		combiner_plan = outerPlan(combiner_plan);
	}

	/* If there are no aggregates, there's nothing to do */
	if (combiner_plan == NULL)
		return;

	combiner_agg_plan = (Agg *) combiner_plan;
	combiner_agg_plan->aggsplit = AGGSPLIT_INITIAL_SERIAL;

	for (;;)
	{
		if (worker_plan == NULL)
			break;
		if (IsA(worker_plan, Agg))
			break;
		worker_plan = outerPlan(worker_plan);
	}

	Assert(worker_plan != NULL);
	worker_agg_plan = (Agg *) worker_plan;

	combiner_agg_plan->aggsplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE | AGGSPLITOP_SERIALIZE | AGGSPLITOP_SKIPFINAL;

	foreach(tlc, combiner_plan->targetlist)
	{
		ListCell *lc;
		TargetEntry *ctle = (TargetEntry *) lfirst(tlc);
		TargetEntry *wtle;
		List *nodes = pull_var_clause((Node *) ctle->expr, PVC_INCLUDE_AGGREGATES);
		List *aggs = NIL;
		Aggref *cagg;
		Aggref *wagg;
		TargetEntry *ctle_arg;
		Oid type;

		foreach(lc, nodes)
		{
			Node *n = lfirst(lc);
			if (!IsA(n, Aggref))
				continue;
			aggs = lappend(aggs, (Aggref *) n);
		}

		Assert(list_length(aggs) <= 1);

		if (!aggs)
			continue;

		cagg = (Aggref *) linitial(aggs);
		ctle_arg = (TargetEntry *) linitial(cagg->args);

		wtle = get_tle_by_resno(worker_agg_plan->plan.targetlist, ctle->resno);
		Assert(IsA(wtle->expr, Aggref));

		wagg = (Aggref *) wtle->expr;

		/* Mark the combiner aggregate as partial */
		type = cagg->aggtype;
		cagg = copyObject(wagg);
		cagg->aggfilter = NULL;
		cagg->aggtype = type;
		cagg->aggsplit = 0;
		cagg->args = list_make1((Node *) ctle_arg);

		mark_partial_aggref(cagg, combiner_agg_plan->aggsplit);

		/* Replace the original combiner targetlist entry with the partial aggregate */
		ctle->expr = (Expr *) cagg;
	}
}

/*
 * get_plan_from_stmt
 */
static PlannedStmt *
get_plan_from_stmt(Oid id, RawStmt *node, const char *sql, bool is_combine, bool rewrite_combines)
{
	Query *query;
	PlannedStmt	*plan;

	query = linitial(pg_analyze_and_rewrite(node, sql, NULL, 0, NULL));

	QuerySetContQueryId(query, id);

	if (rewrite_combines)
		RewriteCombineAggs(query);

	plan = pg_plan_query(query, 0, NULL);

	return plan;
}

/*
 * get_worker_plan
 */
static PlannedStmt *
get_worker_plan(ContQuery *view)
{
	RawStmt* stmt = GetContWorkerSelectStmt(view, NULL);
	PlannedStmt *plan;

	plan = get_plan_from_stmt(view->id, stmt, view->sql, false, true);

	make_aggs_partial(plan->planTree);

	return plan;
}

/*
 * add_tuplestore_scan_path
 */
static void
add_tuplestore_scan_path(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Path *path = (Path *) CreateTuplestoreScanPath(root, rel, rte);

	rel->pathlist = list_make1(path);
}

/*
 * get_tuplestore_scan_plan
 */
static PlannedStmt *
get_tuplestore_scan_plan(Oid id, RawStmt *node, const char* sql, bool is_combine, bool rewrite_combines)
{
	PlannedStmt *result;

	PG_TRY();
	{
		set_rel_pathlist_hook = add_tuplestore_scan_path;
		result = get_plan_from_stmt(id, node, sql, is_combine, rewrite_combines);
		set_rel_pathlist_hook = NULL;
	}
	PG_CATCH();
	{
		/*
		 * Hooks won't be reset if there's an error, so we need to make
		 * sure that they're not set for whatever query is run next in this xact.
		 */
		set_rel_pathlist_hook = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return result;
}

/*
 * GetContViewOverlayPlan
 */
PlannedStmt *
GetContViewOverlayPlan(ContQuery *view)
{
	RawStmt *raw = GetContViewOverlayStmt(view);
	PlannedStmt *result;
	Query *wq;

	/*
	 * We need to apply any top-level functions in the overlay query's targetlist since
	 * the CQ does not apply them.
	 */
	wq = GetContWorkerQuery(view);
	FinalizeOverlayStmtAggregates((SelectStmt *) raw->stmt, wq);

	result = get_tuplestore_scan_plan(view->id, raw, view->sql, false, true);

	return result;
}

/*
 * remove_custom_paths
 *
 * Remove any previously added CustomPaths from the given rel
 */
static void
remove_custom_paths(RelOptInfo *rel)
{
	ListCell *lc;
	List *pathlist = NIL;
	foreach(lc, rel->pathlist)
	{
		Path *p = (Path *) lfirst(lc);
		if (IsA(p, CustomPath))
		{
			/*
			 * Pull off any CustomPaths by replace them with their child Path
			 */
			CustomPath *c = (CustomPath *) p;
			pathlist = lappend(pathlist, linitial(c->custom_paths));
		}
		else
		{
			pathlist = lappend(pathlist, p);
		}
	}
	rel->pathlist = pathlist;
}

/*
 * allow_star_schema_join
 */
static inline bool
allow_star_schema_join(PlannerInfo *root,
					   Path *outer_path,
					   Path *inner_path)
{
	Relids		innerparams = PATH_REQ_OUTER(inner_path);
	Relids		outerrelids = outer_path->parent->relids;

	/*
	 * It's a star-schema case if the outer rel provides some but not all of
	 * the inner rel's parameterization.
	 */
	return (bms_overlap(innerparams, outerrelids) &&
			bms_nonempty_difference(innerparams, outerrelids));
}

/*
 * try_nestloop_path
 */
static void
try_nestloop_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  Path *outer_path,
				  Path *inner_path,
				  List *pathkeys,
				  JoinType jointype,
				  JoinPathExtraData *extra)
{
	Relids		required_outer;
	JoinCostWorkspace workspace;

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible --- unless allow_star_schema_join
	 * says to allow it anyway.  Also, we must reject if have_dangerous_phv
	 * doesn't like the look of it, which could only happen if the nestloop is
	 * still parameterized.
	 */
	required_outer = CompatCalcNestLoopRequiredOuter(outer_path,
												  inner_path);
	if (required_outer &&
		((!bms_overlap(required_outer, extra->param_source_rels) &&
		  !allow_star_schema_join(root, outer_path, inner_path)) ||
		 have_dangerous_phv(root,
							outer_path->parent->relids,
							PATH_REQ_OUTER(inner_path))))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}

	/*
	 * Do a precheck to quickly eliminate obviously-inferior paths.  We
	 * calculate a cheap lower bound on the path's cost and then use
	 * add_path_precheck() to see if the path is clearly going to be dominated
	 * by some existing path for the joinrel.  If not, do the full pushup with
	 * creating a fully valid path structure and submitting it to add_path().
	 * The latter two steps are expensive enough to make this two-phase
	 * methodology worthwhile.
	 */
	initial_cost_nestloop(root, &workspace, jointype,
						  outer_path, inner_path, extra);

	if (add_path_precheck(joinrel,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
		add_path(joinrel, (Path *)
				 create_nestloop_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra,
									  outer_path,
									  inner_path,
									  extra->restrictlist,
									  pathkeys,
									  required_outer));
	}
	else
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
	}
}

/*
 * try_physical_group_lookup_path
 */
static void
try_physical_group_lookup_path(PlannerInfo *root,
				  RelOptInfo *joinrel,
				  JoinType jointype,
				  Path *outer_path,
				  Path *inner_path,
				  List *pathkeys,
					JoinPathExtraData *extra)
{
	NestPath *nlpath;
	Path *path;

	try_nestloop_path(root, joinrel, outer_path, inner_path,
			pathkeys, jointype, extra);

	if (list_length(joinrel->pathlist) != 1)
		elog(ERROR, "could not create physical group lookup path");

	if (!IsA(linitial(joinrel->pathlist), NestPath))
		return;

	nlpath = (NestPath *) linitial(joinrel->pathlist);
	path = (Path *) CreatePhysicalGroupLookupPath(joinrel, (Path *) nlpath);
	joinrel->pathlist = list_make1(path);
}

/*
 * add_physical_group_lookup_path
 */
static void
add_physical_group_lookup_path(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	Path *scanpath = (Path *) linitial(rel->pathlist);
	Path *path = (Path *) CreatePhysicalGroupLookupPath(rel, scanpath);

	/* If we end up going with a JOIN plan, this will be reverted by remove_custom_paths */
	rel->pathlist = list_make1(path);
}

/*
 * add_physical_group_lookup_join_path
 *
 * If we're doing a combiner lookup of groups to update, then we need to return
 * updatable physical tuples. We have a specific plan for this so that we can predictably
 * control performance and take advantage of certain assumptions we can make about matrels
 * and their indices.
 */
static void
add_physical_group_lookup_join_path(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
		RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra)
{
	List *merge_pathkeys;
	ListCell   *outerlc;
	RangeTblEntry *inner = planner_rt_fetch(innerrel->relid, root);

	/* only consider plans for which the VALUES scan is the outer */
	if (inner->rtekind == RTE_VALUES || inner->rtekind == RTE_SUBQUERY)
		return;

	remove_custom_paths(innerrel);
	remove_custom_paths(outerrel);

	/* a physical group lookup is the only path we want to consider */
	joinrel->pathlist = NIL;

	merge_pathkeys = build_join_pathkeys(root, joinrel, jointype,
			outerrel->cheapest_total_path->pathkeys);

	foreach (outerlc, outerrel->pathlist)
	{
		ListCell *innerlc;
		Path *outerpath = (Path *) lfirst(outerlc);

		foreach(innerlc, innerrel->pathlist)
		{
			Path *innerpath = (Path *) lfirst(innerlc);

			try_physical_group_lookup_path(root,
								joinrel,
								jointype,
								outerpath,
								innerpath,
								merge_pathkeys,
								extra);
		}

		foreach(innerlc, innerrel->cheapest_parameterized_paths)
		{
			Path *innerpath = (Path *) lfirst(innerlc);

			try_physical_group_lookup_path(root,
								joinrel,
								jointype,
								outerpath,
								innerpath,
								merge_pathkeys,
								extra);
		}
	}
}

/*
 * GetGroupsLookupPlan
 */
PlannedStmt *
GetGroupsLookupPlan(Query *query)
{
	PlannedStmt *plan;
	set_join_pathlist_hook_type save_join_hook = set_join_pathlist_hook;
	set_rel_pathlist_hook_type save_rel_hook = set_rel_pathlist_hook;

	/*
	 * Perform everything from here in a try/catch so we can ensure the join path list hook
	 * is restored to whatever it was set to before we were called
	 */
	PG_TRY();
	{
		set_rel_pathlist_hook = add_physical_group_lookup_path;
		set_join_pathlist_hook = add_physical_group_lookup_join_path;

		PushActiveSnapshot(GetTransactionSnapshot());
		plan = pg_plan_query(query, 0, NULL);
		PopActiveSnapshot();

		set_join_pathlist_hook = save_join_hook;
		set_rel_pathlist_hook = save_rel_hook;
	}
	PG_CATCH();
	{
		set_join_pathlist_hook = save_join_hook;
		set_rel_pathlist_hook = save_rel_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	return plan;
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
	Oid hash_group_oid = GetHashGroupOid();
	Oid ls_hash_group_oid = GetLSHashGroupOid();

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
		if (func->funcid != hash_group_oid && func->funcid != ls_hash_group_oid)
			continue;

		result = copyObject(func);
		break;
	}

	return result;
}

/*
 * CreateEState
 */
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
	estate->es_lastoid = InvalidOid;
	estate->es_processed = 0;

	CompatPrepareEState(query_desc->plannedstmt, estate);

	estate->es_top_eflags |= EXEC_FLAG_SKIP_TRIGGERS;

	return estate;
}

/*
 * SetEStateSnapshot
 */
void
SetEStateSnapshot(EState *estate)
{
	estate->es_snapshot = GetTransactionSnapshot();
	estate->es_snapshot->active_count++;
	estate->es_snapshot->copied = true;
	PushActiveSnapshot(estate->es_snapshot);
}

/*
 * UnsetEStateSnapshot
 */
void
UnsetEStateSnapshot(EState *estate)
{
	PopActiveSnapshot();
	estate->es_snapshot = NULL;
}

/*
 * get_combiner_plan
 */
static PlannedStmt *
get_combiner_plan(ContQuery *view)
{
	List *parsetree_list;
	SelectStmt *selectstmt;
	RawStmt *result;
	PlannedStmt *combiner_plan;
	PlannedStmt *worker_plan;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	result = (RawStmt *) linitial(parsetree_list);
	selectstmt = TransformSelectStmtForContProcess(view->matrel, (SelectStmt *) result->stmt, NULL, view->sw_step_factor, Combiner);

	result->stmt = (Node *) selectstmt;
	combiner_plan = get_tuplestore_scan_plan(view->id, result, view->sql, true, true);

	worker_plan = get_worker_plan(view);
	make_aggs_combinable(combiner_plan->planTree, worker_plan->planTree);

	return combiner_plan;
}

/*
 * GetContPlan
 */
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
CustomScan *
SetCombinerPlanTuplestorestate(PlannedStmt *plan, Tuplestorestate *tupstore)
{
	CustomScan *scan;
	char *ptr;

	if (IsA(plan->planTree, CustomScan))
		scan = (CustomScan *) plan->planTree;
	else if ((IsA(plan->planTree, Agg)) &&
			IsA(plan->planTree->lefttree, CustomScan))
		scan = (CustomScan *) plan->planTree->lefttree;
	else if (IsA(plan->planTree, Agg) &&
			IsA(plan->planTree->lefttree, Sort) &&
			IsA(plan->planTree->lefttree->lefttree, CustomScan))
		scan = (CustomScan *) plan->planTree->lefttree->lefttree;
	else if (IsA(plan->planTree, Group) &&
			IsA(plan->planTree->lefttree, Sort) &&
			IsA(plan->planTree->lefttree->lefttree, CustomScan))
		scan = (CustomScan *) plan->planTree->lefttree->lefttree;
	else if (IsA(plan->planTree, Group) &&
			IsA(plan->planTree->lefttree, CustomScan))
		scan = (CustomScan *) plan->planTree->lefttree->lefttree;
	else
		elog(ERROR, "couldn't find TuplestoreScan node in combiner's plan: %d", nodeTag(plan->planTree));

	/*
	 * The CustomScan needs access to the given Tuplestorestate, but the scan has to be
	 * copyable so we encode a local-memory pointer to the tuplestore as a string. It's kind
	 * of ugly, but these CustomScans are executed under highly predictable circumstances,
	 * and in this process so technically it's safe.
	 */
	ptr = palloc0(sizeof(Tuplestorestate *));
	memcpy(ptr, &tupstore, sizeof(Tuplestorestate *));

	scan->custom_private = list_make1(makeString(ptr));

	return scan;
}

/*
 * check_matrels_writable
 */
static void
check_matrels_writable(Query *q)
{
	if (!CreatingPipelineDB() && (q->commandType == CMD_INSERT || q->commandType == CMD_UPDATE || q->commandType == CMD_DELETE))
	{
		RangeTblEntry *rte;

		Assert(q->resultRelation);
		rte = rt_fetch(q->resultRelation, q->rtable);

		if (!MatRelWritable() && RelidIsMatRel(rte->relid, NULL))
		{
			RangeVar *cv;
			Relation rel = heap_open(rte->relid, NoLock);
			RangeVar *matrel = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel), -1);

			relation_close(rel, NoLock);

			if (RangeVarIsMatRel(matrel, &cv))
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("cannot change materialization table \"%s\" of continuous view \"%s\"",
								 matrel->relname, cv->relname),
						 errhint("Toggle the \"pipelinedb.matrels_writable\" parameter to change this behavior.")));
		}
	}
}

/*
 * PipelinePlanner
 */
PlannedStmt *
PipelinePlanner(Query *parse, int options, ParamListInfo params)
{
	PlannedStmt *result;

	if (!IsBinaryUpgrade && PipelineDBExists())
	{
		/*
		 * If this is a user query with combine aggregate references, we need to expand them
		 * using the correct combine aggregates.
		 */
		if (parse->commandType == CMD_SELECT && !PipelineContextIsDDL() && !IsContQueryProcess())
			RewriteCombineAggs(parse);
		else
			check_matrels_writable(parse);
	}

	if (save_planner_hook)
		result = (*save_planner_hook) (parse, options, params);
	else
		result = standard_planner(parse, options, params);

	return result;
}
