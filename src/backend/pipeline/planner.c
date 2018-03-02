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
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_combine.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "parser/parse_relation.h"
#include "pipeline/analyzer.h"
#include "pipeline/executor.h"
#include "pipeline/planner.h"
#include "pipeline/physical_group_lookup.h"
#include "pipeline/tuplestore_scan.h"
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

	path = (Path *) CreateTuplestoreScanPath(rel);

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

	QuerySetContQueryId(query, id);

	plan = pg_plan_query(query, 0, NULL);

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
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, viewptr, view->sw_step_factor, Worker);

	return selectstmt;
}

static inline bool
clause_sides_match_join(RestrictInfo *rinfo, RelOptInfo *outerrel,
						RelOptInfo *innerrel)
{
	if (bms_is_subset(rinfo->left_relids, outerrel->relids) &&
		bms_is_subset(rinfo->right_relids, innerrel->relids))
	{
		/* lefthand side is outer */
		rinfo->outer_is_left = true;
		return true;
	}
	else if (bms_is_subset(rinfo->left_relids, innerrel->relids) &&
			 bms_is_subset(rinfo->right_relids, outerrel->relids))
	{
		/* righthand side is outer */
		rinfo->outer_is_left = false;
		return true;
	}
	return false;				/* no good for these input relations */
}

static PlannedStmt *
get_worker_plan(ContQuery *view)
{
	SelectStmt* stmt = get_worker_select_stmt(view, NULL);
	PlannedStmt *plan;

	plan = get_plan_from_stmt(view->id, (Node *) stmt, view->sql, false);

	return plan;
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
	required_outer = calc_nestloop_required_outer(outer_path,
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
						  outer_path, inner_path,
						  extra->sjinfo, &extra->semifactors);

	if (add_path_precheck(joinrel,
						  workspace.startup_cost, workspace.total_cost,
						  pathkeys, required_outer))
	{
		add_path(joinrel, (Path *)
				 create_nestloop_path(root,
									  joinrel,
									  jointype,
									  &workspace,
									  extra->sjinfo,
									  &extra->semifactors,
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
	if (inner->rtekind == RTE_VALUES)
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
	set_join_pathlist_hook_type save_join_hook;
	set_rel_pathlist_hook_type save_rel_hook;

	/*
	 * Perform everything from here in a try/catch so we can ensure the join path list hook
	 * is restored to whatever it was set to before we were called
	 */
	PG_TRY();
	{
		save_rel_hook = set_rel_pathlist_hook;
		set_rel_pathlist_hook = add_physical_group_lookup_path;

		save_join_hook = set_join_pathlist_hook;
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

static PlannedStmt *
get_combiner_plan(ContQuery *view)
{
	List		*parsetree_list;
	SelectStmt	*selectstmt;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	selectstmt = TransformSelectStmtForContProcess(view->matrel, selectstmt, NULL, view->sw_step_factor, Combiner);
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
	estate->es_lastoid = InvalidOid;
	estate->es_processed = 0;

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
 * validate_stream_constraints
 *
 * We allow some stuff that is technically supported by the grammar
 * for CREATE STREAM, so we do some validation here so that we can generate more
 * informative errors than simply syntax errors.
 */
static void
validate_stream_constraints(CreateStmt *stmt)
{
	ListCell *lc;

	foreach(lc, stmt->tableElts)
	{
		Node *n = (Node *) lfirst(lc);
		ColumnDef *cdef;

		if (!IsA(n, ColumnDef))
			continue;

		cdef = (ColumnDef *) n;
		if (cdef->constraints)
		{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot create constraint on stream %s", stmt->relation->relname),
						 errhint("Constraints are currently unsupported on streams.")));
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
	ContExecutionLock exec_lock = NULL;
	PipelineDDLLock ddl_lock = NULL;
	Relation rel = NULL;
	bool isstream = false;

	PG_TRY();
	{
		if (IsA(parsetree, CreateContViewStmt) || IsA(parsetree, CreateContTransformStmt))
		{
			// if transform, verify no constraints
			Node *node;

			if (IsA(parsetree, CreateContViewStmt))
				node = ((CreateContViewStmt *) parsetree)->query;
			else
				node = ((CreateContTransformStmt *) parsetree)->query;

			/*
			 * Indicate to the analyzer/planner hooks that we're executing a CREATE CONTINUOUS statement
			 */
			PipelineContextSetIsDDL();
			ddl_lock = AcquirePipelineDDLLock();

			/* The grammar should enforce this */
			Assert(IsA(node, SelectStmt));
		}
		else if (IsA(parsetree, IndexStmt))
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
		else if (IsA(parsetree, DropStmt))
		{
			DropStmt *stmt = (DropStmt *) parsetree;
			/*
			 * If we're dropping a CQ, we must acquire the continuous execution lock,
			 * which when held exclusively prevents all CQs (they acquire it as a shared lock)
			 * from being executed by workers/combiners. This prevents deadlocks between
			 * this process and workers/combiners, which can all acquire contended locks in
			 * basically any order they want.
			 *
			 * So we simply ensure mutual exclusion between droppers and workers/combiners execution.
			 */
			if (list_length(stmt->objects) == 1 && IsA(linitial(stmt->objects), List))
			{
				RangeVar *rv = makeRangeVarFromNameList(linitial(stmt->objects));

				if (IsPipelineObject(rv)||
						stmt->removeType == OBJECT_SCHEMA ||
						stmt->removeType == OBJECT_TYPE ||
						stmt->removeType == OBJECT_FUNCTION)
				{
					exec_lock = AcquireContExecutionLock(AccessExclusiveLock);
					ddl_lock = AcquirePipelineDDLLock();
				}
			}
		}
		else if (IsA(parsetree, CreateForeignTableStmt))
		{
			/*
			 * If we're creating a stream, we perform a constraints check since constraints are not
			 * supported by streams although they are supported by the grammar.
			 */
			if (pg_strcasecmp(((CreateForeignTableStmt *) parsetree)->servername, PIPELINEDB_SERVER) == 0)
			{
				CreateStmt *stmt = (CreateStmt *) parsetree;

				validate_stream_constraints(stmt);
				isstream = true;
				transformCreateStreamStmt((CreateForeignTableStmt *) stmt);
			}
		}
		else if (IsA(parsetree, AlterTableStmt))
		{
			/*
			 * Streams only support adding columns,
			 * so if we're altering a stream ensure that's all we're doing.
			 */
			AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

			if (RangeVarIsForStream(stmt->relation, true))
			{
				ListCell *lc;
				foreach(lc, stmt->cmds)
				{
					AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);
					if (cmd->subtype != AT_AddColumn && cmd->subtype != AT_ChangeOwner)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										(errmsg("streams only support ADD COLUMN or OWNER TO actions"))));
					}
				}
			}
		}
		else if (IsA(parsetree, SelectStmt))
		{
			// if transform anywhere in FROM, error!
			// we should probably start breaking this up into smaller functions :)
		}

		if (SaveUtilityHook != NULL)
			(*SaveUtilityHook) (parsetree, sql, context, params, dest, tag);
		else
			standard_ProcessUtility(parsetree, sql, context, params, dest, tag);

		if (exec_lock)
			ReleaseContExecutionLock(exec_lock);

		if (ddl_lock)
			ReleasePipelineDDLLock(ddl_lock);

		/*
		 * If we created a stream, we need to create a pipeline_stream entry in addition
		 * to the underlying foreign table.
		 */
		if (isstream)
		{
			CreateForeignTableStmt *stmt = (CreateForeignTableStmt *) parsetree;
			Oid relid = RangeVarGetRelid(stmt->base.relation, NoLock, false);
			CreatePipelineStreamEntry(stmt, relid);
		}

		/*
		 * We may have dropped a PipelineDB object, so we reconcile our own catalog tables
		 * to ensure any orphaned entries are removed in this transaction.
		 */
		if (IsA(parsetree, DropStmt))
			ReconcilePipelineStreams();

		/*
		 * Clear analyzer/planner context flags
		 */
		ClearPipelineContext();
	}
	PG_CATCH();
	{
		ClearPipelineContext();

		PG_RE_THROW();
	}
	PG_END_TRY();

	if (rel)
		heap_close(rel, NoLock);
}
