/*-------------------------------------------------------------------------
 *
 * pgxcplan.c
 *
 *	  Functions for generating a PGXC plans.
 *
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/backend/optimizer/plan/pgxcplan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/gtm.h"
#include "access/sysattr.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#ifdef PGXC
#include "commands/trigger.h"
#endif
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "optimizer/pathnode.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parse_oper.h"
#include "parser/parsetree.h"
#include "parser/parse_func.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/execRemote.h"
#include "rewrite/rewriteManip.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"
#include "utils/tqual.h"
#include "access/htup_details.h"

/* Context for collecting range tables in a Query tree */
typedef struct
{
	List *crte_rtable;
} collect_RTE_context;

static void validate_part_col_updatable(const Query *query);
static bool contains_temp_tables(List *rtable);
static void pgxc_handle_unsupported_stmts(Query *query);
static PlannedStmt *pgxc_FQS_planner(Query *query, int cursorOptions,
										ParamListInfo boundParams);
static PlannedStmt *pgxc_handle_exec_direct(Query *query, int cursorOptions,
												ParamListInfo boundParams);
static RemoteQuery *pgxc_FQS_create_remote_plan(Query *query,
												ExecNodes *exec_nodes,
												bool is_exec_direct);
static bool pgxc_locate_grouping_columns(PlannerInfo *root, List *tlist,
											AttrNumber *grpColIdx);
static List *pgxc_process_grouping_targetlist(List *local_tlist,
												bool single_node_grouping);
static List *pgxc_process_having_clause(List *remote_tlist, bool single_node_grouping,
												Node *havingQual, List **local_qual,
												List **remote_qual);
static Expr *pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex);
static List *pgxc_separate_quals(List *quals, List **local_quals, bool has_aggs);
static Query *pgxc_build_shippable_query_recurse(PlannerInfo *root,
													RemoteQueryPath *rqpath,
													List **unshippable_quals,
													List **rep_tlist);
static RemoteQuery *make_remotequery(List *qptlist, List *qpqual,
										Index scanrelid);
static RangeTblEntry *make_dummy_remote_rte(char *relname, Alias *alias);
static List *pgxc_build_shippable_tlist(List *tlist, List *unshippabl_quals,
										bool has_aggs);
static List *pgxc_add_to_flat_tlist(List *remote_tlist, Node *expr,
												Index ressortgroupref);
static void pgxc_rqplan_adjust_vars(RemoteQuery *rqplan, Node *node);

static CombineType get_plan_combine_type(CmdType commandType, char baselocatortype);

static List *pgxc_collect_RTE(Query *query);
static bool pgxc_collect_RTE_walker(Node *node, collect_RTE_context *crte_context);

static void pgxc_add_returning_list(RemoteQuery *rq, List *ret_list,
									int rel_index);
static void pgxc_build_dml_statement(PlannerInfo *root, CmdType cmdtype,
									Index resultRelationIndex,
									RemoteQuery *rqplan,
									List *sourceTargetList);
static void pgxc_dml_add_qual_to_query(Query *query, int param_num,
									AttrNumber sys_col_attno, Index varno);
static Param *pgxc_make_param(int param_num, Oid param_type);
static void pgxc_add_param_as_tle(Query *query, int param_num, Oid param_type,
									char *resname);
/*
 * pgxc_separate_quals
 * Separate the quals into shippable and unshippable quals. Return the shippable
 * quals as return value and unshippable quals as local_quals
 */
static List *
pgxc_separate_quals(List *quals, List **unshippabl_quals, bool has_aggs)
{
	ListCell	*l;
	List		*shippabl_quals = NIL;
	/*
	 * If the caller knows that there can be aggregates in the calls, we better
	 * take steps for the same. See prologue of pgxc_is_expr_shippable().
	 */
	bool		tmp_has_aggs;
	bool		*tmp_bool_ptr = has_aggs ? &tmp_has_aggs : NULL;

	*unshippabl_quals = NIL;
	foreach(l, quals)
	{
		Expr *clause = lfirst(l);

		if (pgxc_is_expr_shippable(clause, tmp_bool_ptr))
			shippabl_quals = lappend(shippabl_quals, clause);
		else
			*unshippabl_quals = lappend(*unshippabl_quals, clause);
	}
	return shippabl_quals;
}

/*
 * pgxc_build_shippable_tlist
 * Scan the target list expected from this relation, to see if there are any
 * expressions which are not shippable. If a member in the target list has
 * unshippable expression, we can not ship the expression corresponding to
 * that member as is to the datanode. Hence, pull the vars in that
 * expression and add those vars to the target list to be shipped to the
 * datanode. If no such expression exists, we can add the member to the
 * datanode target list as is.
 * We need the values of VARs in the quals to be applied on Coordinator. Add
 * those in the target list for the Datanode.
 */
static List *
pgxc_build_shippable_tlist(List *tlist, List *unshippabl_quals, bool has_aggs)
{
	ListCell				*lcell;
	List					*remote_tlist = NIL;
	PVCAggregateBehavior	pvc_agg_spec = has_aggs ? PVC_INCLUDE_AGGREGATES
														: PVC_REJECT_AGGREGATES;
	List					*unshippable_expr = list_copy(unshippabl_quals);
	List					*aggs_n_vars;
	bool					tmp_has_aggs;

	/*
	 * Add all the shippable members as they are to the target list being built,
	 * and add the Var nodes from the unshippable members.
	 */
	foreach(lcell, tlist)
	{
		TargetEntry 			*tle = lfirst(lcell);
		Expr					*expr;
		bool					*tmp_ptr;

		/*
		 * If there are aggregates in the targetlist, we should say so while
		 * checking the expression shippability.
		 */
		tmp_ptr = has_aggs ? &tmp_has_aggs : NULL;

		if (!IsA(tle, TargetEntry))
			elog(ERROR, "Expected TargetEntry node, but got node with type %d",
							nodeTag(tle));
		expr = tle->expr;
		/*
		 * It's crucial that we pass TargetEntry to the
		 * pgxc_is_expr_shippable(). Helps in detecting top level RowExpr, which
		 * are unshippable
		 */
		if (pgxc_is_expr_shippable((Expr *)tle, tmp_ptr))
			remote_tlist = pgxc_add_to_flat_tlist(remote_tlist, (Node *)expr,
															tle->ressortgroupref);
		else
			unshippable_expr = lappend(unshippable_expr, expr);
	}

	/*
	 * Now collect the aggregates (if needed) and Vars from the unshippable
	 * expressions (from targetlist and unshippable quals and add them to
	 * remote targetlist.
	 */
	aggs_n_vars = pull_var_clause((Node *)unshippable_expr, pvc_agg_spec,
											PVC_RECURSE_PLACEHOLDERS);
	foreach(lcell, aggs_n_vars)
	{
		Node	*agg_or_var = lfirst(lcell);
		Assert(IsA(agg_or_var, Aggref) || IsA(agg_or_var, Var));
		/* If it's an aggregate expression, better it be shippable. */
		Assert(!IsA(agg_or_var, Aggref) ||
				pgxc_is_expr_shippable((Expr *)agg_or_var, &tmp_has_aggs));
		remote_tlist = pgxc_add_to_flat_tlist(remote_tlist, agg_or_var, 0);
	}

	return remote_tlist;
}

/*
 * pgxc_build_shippable_query_baserel
 * builds a shippable Query structure for a base relation. Since there is only
 * one relation involved in this Query, all the Vars need to be restamped. Hence
 * we need a mapping of actuals Vars to those in the Query being built. This
 * mapping is provided by rep_tlist.
 */
static Query *
pgxc_build_shippable_query_baserel(PlannerInfo *root, RemoteQueryPath *rqpath,
									List **unshippable_quals, List **rep_tlist)
{
	RelOptInfo	*baserel = rqpath->path.parent;
	List		*tlist;
	List		*scan_clauses;
	List		*shippable_quals;
	Query		*query;
	RangeTblEntry *rte = rt_fetch(baserel->relid, root->parse->rtable);
	ListCell	*lcell;
	RangeTblRef	*rtr;

	if ((baserel->reloptkind != RELOPT_BASEREL &&
			baserel->reloptkind != RELOPT_OTHER_MEMBER_REL) ||
		baserel->rtekind != RTE_RELATION)
		elog(ERROR, "can not generate shippable query for base relations of type other than plain tables");

	*rep_tlist = NIL;
	*unshippable_quals = NIL;
	/*
	 * The RTE needs to be marked to be included in FROM clause and to prepend
	 * it with ONLY (so that it does not include results from children.
	 */
	rte->inh = false;
	rte->inFromCl = true;

	/*
	 * Get the scan clauses for this relation and separate them into shippable
	 * and non-shippable quals. We need to include the pseudoconstant quals as
	 * well.
	 */
	scan_clauses = pgxc_order_qual_clauses(root, baserel->baserestrictinfo);
	scan_clauses = list_concat(extract_actual_clauses(scan_clauses, false),
								extract_actual_clauses(scan_clauses, true));

	/* Replace any outer-relation variables with nestloop params */
	if (rqpath->path.param_info)
	{
		scan_clauses = (List *)
			pgxc_replace_nestloop_params(root, (Node *) scan_clauses);
	}

	shippable_quals = pgxc_separate_quals(scan_clauses, unshippable_quals, false);
	shippable_quals = copyObject(shippable_quals);

	/*
	 * Build the target list for this relation. We included only the Vars to
	 * start with.
	 */
	tlist = pgxc_build_path_tlist(root, &(rqpath->path));

	tlist = pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
										PVC_RECURSE_PLACEHOLDERS);
	tlist = list_concat(tlist, pull_var_clause((Node *)*unshippable_quals,
												PVC_REJECT_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS));

	/*
	 * The target list that we built just now represents the result of the
	 * query being built. This serves as a reference for building the
	 * encapsulating queries. So, copy it. We then modify the Vars to change
	 * their varno with 1 for the query being built
	 */
	*rep_tlist = add_to_flat_tlist(NIL, tlist);

	/* Build the query */
	query = makeNode(Query);
	query->commandType = CMD_SELECT;
	query->rtable = list_make1(rte);
	query->targetList = copyObject(*rep_tlist);
	query->jointree = (FromExpr *)makeNode(FromExpr);

	rtr = makeNode(RangeTblRef);
	rtr->rtindex = list_length(query->rtable);
	/* There can be only one relation */
	Assert(rtr->rtindex == 1);

	query->jointree->fromlist = list_make1(rtr);
	query->jointree->quals = (Node *)make_ands_explicit(shippable_quals);

	/* Collect the Var nodes in the entire query and restamp the varnos */
	tlist = list_concat(pull_var_clause((Node *)query->targetList, PVC_REJECT_AGGREGATES,
															PVC_RECURSE_PLACEHOLDERS),
						pull_var_clause((Node *)query->jointree->quals,
											PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS));
	foreach (lcell, tlist)
	{
		Var *var = lfirst(lcell);
		Assert(IsA(var, Var));
		if (var->varno != baserel->relid)
			elog(ERROR, "can not handle multiple relations in a single baserel");
		var->varno = rtr->rtindex;
	}

	return query;
}

/*
 * pgxc_generate_colnames
 * Given a prefix and number of names to generate, the function returns a list
 * of names of the form prefix_{1,...,num_cols}
 */
static List *
pgxc_generate_colnames(char *prefix, int num_cols)
{
	List	*colnames = NIL;
	StringInfo	colname = makeStringInfo();
	int		cnt_col;
	for (cnt_col = 0; cnt_col < num_cols; cnt_col++)
	{
		appendStringInfo(colname, "%s_%d", prefix, cnt_col + 1);
		colnames = lappend(colnames, makeString(pstrdup(colname->data)));
		resetStringInfo(colname);
	}
	pfree(colname->data);
	pfree(colname);
	return colnames;
}

/*
 * pgxc_build_shippable_query_jointree
 * builds a shippable Query structure for a join relation. Since there are only
 * two relations involved in this Query, all the Vars need to be restamped. Hence
 * we need a mapping of actuals Vars to those in the Query being built. This
 * mapping is provided by rep_tlist.
 */
static Query *
pgxc_build_shippable_query_jointree(PlannerInfo *root, RemoteQueryPath *rqpath,
									List **unshippable_quals, List **rep_tlist)
{
	/* Variables for the part of the Query representing the JOIN */
	Query			*join_query;
	FromExpr		*from_expr;
	List			*rtable = NIL;
	List			*tlist;
	JoinExpr		*join_expr;
	List			*join_clauses;
	List			*other_clauses;
	List			*varlist;
	RangeTblEntry	*join_rte;
	/* Variables for the left side of the JOIN */
	Query			*left_query;
	List			*left_us_quals;
	List			*left_rep_tlist;
	RangeTblEntry	*left_rte;
	Alias			*left_alias;
	List			*left_colnames;
	char			*left_aname = "l";	/* For the time being! Do we really care
										 * about unique alias names across the
										 * tree
										 */
	RangeTblRef		*left_rtr;
	List			*left_colvars;

	/* Variables for the right side of the JOIN */
	Query			*right_query;
	List			*right_us_quals;
	List			*right_rep_tlist;
	RangeTblEntry	*right_rte;
	Alias 			*right_alias;
	List			*right_colnames;
	char			*right_aname = "r";
	RangeTblRef		*right_rtr;
	List			*right_colvars;
	/* Miscellaneous variables */
	ListCell		*lcell;

	if (!rqpath->leftpath || !rqpath->rightpath)
		elog(ERROR, "a join relation path should have both left and right paths");

	/*
	 * Build the query representing the left side of JOIN and add corresponding
	 * RTE with proper aliases
	 */
	left_query = pgxc_build_shippable_query_recurse(root, rqpath->leftpath,
													&left_us_quals,
													&left_rep_tlist);
	left_colnames = pgxc_generate_colnames("a", list_length(left_rep_tlist));
	left_alias = makeAlias(left_aname, left_colnames);
	/*
	 * As of now (1st Nov. 2013) we do not expect a LATERAL query to be shipped
	 * through this function. See notes in prologue of
	 * create_remotequery_path().
	 */
	left_rte = addRangeTableEntryForSubquery(NULL, left_query, left_alias,
												false, false);
	rtable = lappend(rtable, left_rte);
	left_rtr = makeNode(RangeTblRef);
	left_rtr->rtindex = list_length(rtable);
	/*
	 * Build the query representing the right side of JOIN and add corresponding
	 * RTE with proper aliases
	 */
	right_query = pgxc_build_shippable_query_recurse(root, rqpath->rightpath,
													&right_us_quals,
													&right_rep_tlist);
	right_colnames = pgxc_generate_colnames("a", list_length(right_rep_tlist));
	right_alias = makeAlias(right_aname, right_colnames);
	/*
	 * As of now (1st Nov. 2013) we do not expect a LATERAL query to be shipped
	 * through this function. See notes in prologue of
	 * create_remotequery_path().
	 */
	right_rte = addRangeTableEntryForSubquery(NULL, right_query, right_alias,
											  false, false);
	rtable = lappend(rtable, right_rte);
	right_rtr = makeNode(RangeTblRef);
	right_rtr->rtindex = list_length(rtable);

	if (left_us_quals || right_us_quals)
		elog(ERROR, "unexpected unshippable quals in JOIN tree");

	/*
	 * Prepare quals to be included in the query when they are shippable, return
	 * back the unshippable quals. We need to restamp the Vars in the clauses
	 * to match the JOINing queries, so make a copy of those.
	 */
	extract_actual_join_clauses(rqpath->join_restrictlist, &join_clauses,
									&other_clauses);
	join_clauses = copyObject(join_clauses);
	other_clauses = list_concat(other_clauses,
								extract_actual_clauses(rqpath->join_restrictlist,
														true));
	/*
	 * Replace any outer-relation variables with nestloop params.
	 * Do this before separating shippable and unshippable quals.
	 * The quals with nested loop parameters are not shippable.
	 */
	if (rqpath->path.param_info)
	{
		join_clauses = (List *)
			pgxc_replace_nestloop_params(root, (Node *) join_clauses);
		other_clauses = (List *)
			pgxc_replace_nestloop_params(root, (Node *) other_clauses);
	}
	other_clauses = pgxc_separate_quals(other_clauses, unshippable_quals, false);
	other_clauses = copyObject(other_clauses);

	/* Assert what we checked back in pgxc_is_join_shippable() */
	if (!pgxc_is_expr_shippable((Expr *)join_clauses, NULL))
		elog(ERROR, "join with unshippable join clauses can not be shipped");
	/*
	 * Build the targetlist for this relation and also the targetlist
	 * representing the query targetlist. The representative target list is in
	 * the form that rest of the plan can understand. The Vars in the JOIN Query
	 * targetlist need to be restamped with the right varno (left or right) and
	 * varattno to match the columns of the JOINing queries.
	 */
	tlist = pgxc_build_path_tlist(root, &(rqpath->path));
	varlist = list_concat(pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS),
						pull_var_clause((Node *)*unshippable_quals, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS));
	*rep_tlist = add_to_flat_tlist(NIL, varlist);
	list_free(varlist);
	tlist = copyObject(*rep_tlist);
	varlist = list_concat(pull_var_clause((Node *)tlist, PVC_REJECT_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS),
							pull_var_clause((Node *)join_clauses,
												PVC_REJECT_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS));
	varlist = list_concat(varlist, pull_var_clause((Node *)other_clauses,
													PVC_REJECT_AGGREGATES,
													PVC_RECURSE_PLACEHOLDERS));
	foreach (lcell, varlist)
	{
		Var	*var = lfirst(lcell);
		TargetEntry *tle;
		Assert(IsA(var, Var));

		tle = tlist_member((Node *)var, left_rep_tlist);
		if (tle)
		{
			var->varno = left_rtr->rtindex;
			var->varattno = tle->resno;
		}
		else
		{
			tle = tlist_member((Node *)var, right_rep_tlist);
			if (tle)
			{
				var->varno = right_rtr->rtindex;
				var->varattno = tle->resno;
			}
			else
			{
				elog(ERROR, "can not find var with varno = %d and varattno = %d",
								var->varno, var->varattno);
			}
		}
	}
	list_free(varlist);

	/* Build the Join expression with the above left and right queries */
	join_expr = makeNode(JoinExpr);
	join_expr->jointype = rqpath->jointype;
	join_expr->larg = (Node *)left_rtr;
	join_expr->rarg = (Node *)right_rtr;
	join_expr->quals = (Node *)make_ands_explicit(join_clauses);

	/* Build the RTE for JOIN query being created and add it to the rtable */
	/* We need to construct joinaliasvars from the joining RTEs */
	expandRTE(left_rte, left_rtr->rtindex, 0, -1, false, NULL, &left_colvars);
	expandRTE(right_rte, right_rtr->rtindex, 0, -1, false, NULL, &right_colvars);
	join_rte = addRangeTableEntryForJoin(NULL,
										list_concat(copyObject(left_colnames),
													copyObject(right_colnames)),
										rqpath->jointype,
										list_concat(left_colvars, right_colvars),
										NULL, false);
	rtable = lappend(rtable, join_rte);
	/* Put the index of this RTE in Join expression */
	join_expr->rtindex = list_length(rtable);

	/* Build the From clause of the JOIN query */
	from_expr = makeNode(FromExpr);
	from_expr->fromlist = list_make1(join_expr);
	from_expr->quals = (Node *)make_ands_explicit(other_clauses);

	/* Build the query now */
	join_query = makeNode(Query);
	join_query->commandType = CMD_SELECT;
	join_query->rtable = rtable;
	join_query->jointree = from_expr;
	join_query->targetList = tlist;

	return join_query;
}

/*
 * pgxc_build_shippable_query_recurse
 * Recursively build Query structure for the RelOptInfo in the given RemoteQuery
 * path.
 */
static Query *
pgxc_build_shippable_query_recurse(PlannerInfo *root, RemoteQueryPath *rqpath,
							List **unshippable_quals, List **rep_tlist)
{
	RelOptInfo	*parent_rel = rqpath->path.parent;
	Query		*result_query = NULL;

	switch (parent_rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
			result_query = pgxc_build_shippable_query_baserel(root, rqpath,
																unshippable_quals,
																rep_tlist);
		break;

		case RELOPT_JOINREL:
			result_query = pgxc_build_shippable_query_jointree(root, rqpath,
																unshippable_quals,
																rep_tlist);
		break;

		default:
			elog(ERROR, "Creating remote query plan for relations of type %d is not supported",
							parent_rel->reloptkind);
	}
	return result_query;
}

/*
 * pgxc_rqplan_adjust_vars
 * Adjust the Var nodes in given node to be suitable to the
 * rqplan->remote_query. We don't need a expression mutator here, since we are
 * replacing scalar members in the Var nodes.
 */
static void
pgxc_rqplan_adjust_vars(RemoteQuery *rqplan, Node *node)
{
	List		*var_list = pull_var_clause(node, PVC_RECURSE_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS);
	ListCell	*lcell_var;

	/*
	 * For every Var node in base_tlist find a matching Var node in ref_tlist.
	 * Get the Var node in the with same resno. This is
	 * assumed to the Var node corresponding to the Var node in base_tlist in
	 * the "query" targetlist is copied into the later.
	 */
	foreach (lcell_var, var_list)
	{
		TargetEntry	*ref_tle;
		TargetEntry	*qry_tle;
		Var *var = (Var *)lfirst(lcell_var);

		ref_tle = tlist_member((Node *)var, rqplan->coord_var_tlist);
		qry_tle = get_tle_by_resno(rqplan->query_var_tlist, ref_tle->resno);
		if (!IsA(qry_tle->expr, Var))
			elog(ERROR, "expected a VAR node but got node of type %d", nodeTag(qry_tle->expr));
		*var = *(Var *)(qry_tle->expr);
	}
}

/*
 * pgxc_rqplan_build_statement
 * Given a RemoteQuery plan generate the SQL statement from Query structure
 * inside it.
 */
static void
pgxc_rqplan_build_statement(RemoteQuery *rqplan)
{
	StringInfo sql = makeStringInfo();
	deparse_query(rqplan->remote_query, sql, NULL, rqplan->rq_finalise_aggs,
					rqplan->rq_sortgroup_colno);
	if (rqplan->sql_statement)
		pfree(rqplan->sql_statement);
	rqplan->sql_statement = sql->data;
	return;
}

/*
 * pgxc_rqplan_adjust_tlist
 * The function adjusts the targetlist of remote_query in RemoteQuery node
 * according to the plan's targetlist. This function should be
 * called whenever we modify or set plan's targetlist (plan->targetlist).
 */
extern void
pgxc_rqplan_adjust_tlist(RemoteQuery *rqplan)
{
	Plan	*plan = &(rqplan->scan.plan);
	List	*base_tlist;
	List	*query_tlist;
	/*
	 * Build the target list to be shipped to the datanode from the targetlist
	 * expected from the plan.
	 */
	base_tlist = pgxc_build_shippable_tlist(plan->targetlist, plan->qual,
											rqplan->remote_query->hasAggs);

	query_tlist = copyObject(base_tlist);
	/* Replace the targetlist of remote_query with the new base_tlist */
	pgxc_rqplan_adjust_vars(rqplan, (Node *)query_tlist);
	rqplan->remote_query->targetList = query_tlist;
	rqplan->base_tlist = base_tlist;
	/* Build the SQL query statement for the modified remote_query */
	pgxc_rqplan_build_statement(rqplan);
}

/*
 * pgxc_build_shippable_query
 * Builds a shippable Query for given RemoteQuery path to be stuffed in given
 * RemoteQuery plan.
 */
static void
pgxc_build_shippable_query(PlannerInfo *root, RemoteQueryPath *covering_path,
							RemoteQuery *result_node)
{
	Query		*query;
	List		*rep_tlist;
	List		*unshippable_quals;

	/*
	 * Build Query representing the result of the JOIN tree. During the process
	 * we also get the set of unshippable quals to be applied after getting the
	 * results from the datanode and the targetlist representing the results of
	 * the query, in a form which plan nodes on Coordinator can understand
	 */
	query = pgxc_build_shippable_query_recurse(root, covering_path,
															&unshippable_quals,
															&rep_tlist);
	result_node->scan.plan.qual = unshippable_quals;
	result_node->query_var_tlist = query->targetList;
	result_node->coord_var_tlist = rep_tlist;
	result_node->remote_query = query;

	pgxc_rqplan_adjust_tlist(result_node);
}

/*
 * create_remotequery_plan
 * The function creates a remote query plan corresponding to the path passed in.
 * It creates a query statement to fetch the results corresponding to the
 * RelOptInfo in the given RemoteQuery path. It first builds the Query structure
 * and deparses it to generate the query statement.
 * At the end it creates a dummy RTE corresponding to this RemoteQuery plan to
 * be added in the rtable list in Query.
 */
Plan *
create_remotequery_plan(PlannerInfo *root, RemoteQueryPath *best_path)
{
	RelOptInfo		*rel = best_path->path.parent;	/* relation for which plan is
													 * being built
													 */
	RemoteQuery		*result_node;	/* the built plan */
	List			*tlist;			/* expected target list */
	RangeTblEntry	*dummy_rte;			/* RTE for the remote query node being
										 * added.
										 */
	Index			dummy_rtindex;
	char			*rte_name;

	/* Get the target list required from this plan */
	tlist = pgxc_build_path_tlist(root, &(best_path->path));
	result_node = makeNode(RemoteQuery);
	result_node->scan.plan.targetlist = tlist;
	pgxc_build_shippable_query(root, best_path, result_node);

	/*
	 * Create and append the dummy range table entry to the range table.
	 * Note that this modifies the master copy the caller passed us, otherwise
	 * e.g EXPLAIN VERBOSE will fail to find the rte the Vars built below.
	 * PGXC_TODO: If there is only a single table, should we set the table name as
	 * the name of the rte?
	 */
	rte_name = "_REMOTE_TABLE_QUERY_";
	if (rel->reloptkind == RELOPT_BASEREL ||
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL)
	{
		RangeTblEntry *rel_rte = rt_fetch(rel->relid, root->parse->rtable);
		char	*tmp_rtename = get_rel_name(rel_rte->relid);
		if (tmp_rtename)
			rte_name = tmp_rtename;
	}

	dummy_rte = make_dummy_remote_rte(rte_name,
										makeAlias("_REMOTE_TABLE_QUERY_", NIL));
	root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
	dummy_rtindex = list_length(root->parse->rtable);

	result_node->scan.scanrelid = dummy_rtindex;
	result_node->read_only = true;
	/* result_node->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate); */
	/* result_node->has_row_marks = query->hasForUpdate; */
	result_node->exec_nodes = best_path->rqpath_en;
	/*
	 * For replicated results, we need to choose one of the nodes, if there are
	 * many of them.
	 */
	if (IsExecNodesReplicated(result_node->exec_nodes))
		result_node->exec_nodes->nodeList =
						GetPreferredReplicationNode(result_node->exec_nodes->nodeList);

	result_node->is_temp = best_path->rqhas_temp_rel;

	if (!result_node->exec_nodes)
		elog(ERROR, "No distribution information found for remote query path");

	pgxc_copy_path_costsize(&(result_node->scan.plan), (Path *)best_path);

	/* PGXCTODO - get better estimates */
 	result_node->scan.plan.plan_rows = 1000;

	result_node->rq_save_command_id = root->parse->has_to_save_cmd_id;
	/*
	 * If there is a pseudoconstant, we should create a gating plan on top of
	 * this node. We must have included the pseudoconstant qual in the remote
	 * query as well, but gating the plan improves performance in case the plan
	 * quals happens to be false.
	 */
	if (root->hasPseudoConstantQuals)
	{
		List *quals = NULL;
		switch(rel->reloptkind)
		{
			case RELOPT_BASEREL:
			case RELOPT_OTHER_MEMBER_REL:
				quals = rel->baserestrictinfo;
			break;

			case RELOPT_JOINREL:
				quals = best_path->join_restrictlist;
			break;

			default:
				elog(ERROR, "creating remote query plan for relations of type %d is not supported",
							rel->reloptkind);
		}
		return pgxc_create_gating_plan(root, (Plan *)result_node, quals);
	}

	return (Plan *)result_node;
}

static RemoteQuery *
make_remotequery(List *qptlist, List *qpqual, Index scanrelid)
{
	RemoteQuery *node = makeNode(RemoteQuery);
	Plan	   *plan = &node->scan.plan;

	/* cost should be inserted by caller */
	plan->targetlist = qptlist;
	plan->qual = qpqual;
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scan.scanrelid = scanrelid;
	node->read_only = true;
	node->has_row_marks = false;

	return node;
}

/*
 * pgxc_add_returning_list
 *
 * This function adds RETURNING var list to the passed remote query node
 * It first pulls all vars from the returning list.
 * It then iterates over all the vars and picks all belonging
 * to the remote relation. The refined vars list is then copied in plan target
 * list as well as base_tlist of remote query.
 *
 * Parameters:
 * rq             : The remote query node to whom the returning
 *                  list is to be added
 * ret_list       : The returning list
 * rel_index      : The index of the concerned relation in RTE list
 */
static void
pgxc_add_returning_list(RemoteQuery *rq, List *ret_list, int rel_index)
{
	List		*shipableReturningList = NULL;
	List		*varlist;
	ListCell	*lc;

	/* Do we have to add a returning clause or not? */
	if (ret_list == NULL)
		return;

	/*
	 * Returning lists cannot contain aggregates and
	 * we are not supporting place holders for now
	 */
	varlist = pull_var_clause((Node *)ret_list, PVC_REJECT_AGGREGATES,
								PVC_REJECT_PLACEHOLDERS);

	/*
	 * For every entry in the returning list if the entry belongs to the
	 * same table as the one whose index is passed then add it to the
	 * shippable returning list
	 */
	foreach (lc, varlist)
	{
		Var *var = lfirst(lc);

		if (var->varno == rel_index)
			shipableReturningList = add_to_flat_tlist(shipableReturningList,
														list_make1(var));
	}

	/*
	 * If the user query had RETURNING clause and here we find that
	 * none of the items in the returning list are shippable
	 * we intend to send RETURNING NULL to the datanodes
	 * Otherwise no rows will be returned from the datanodes
	 * and no rows will be projected to the upper nodes in the
	 * execution tree.
	 */
	if ((shipableReturningList == NIL ||
		list_length(shipableReturningList) <= 0) &&
		list_length(ret_list) > 0)
	{
		Expr *null_const = (Expr *)makeNullConst(INT4OID, -1, InvalidOid);

		shipableReturningList = add_to_flat_tlist(shipableReturningList,
												list_make1(null_const));
	}

	/*
	 * Copy the refined var list in plan target list as well as
	 * base_tlist of the remote query node
	 */
	rq->scan.plan.targetlist = list_copy(shipableReturningList);
	rq->base_tlist = list_copy(shipableReturningList);
}

Plan *
pgxc_make_modifytable(PlannerInfo *root, Plan *topplan)
{
	ModifyTable *mt = (ModifyTable *)topplan;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	/*
	 * PGXC should apply INSERT/UPDATE/DELETE to a Datanode. We are overriding
	 * normal Postgres behavior by modifying final plan or by adding a node on
	 * top of it.
	 * If the optimizer finds out that there is nothing to UPDATE/INSERT/DELETE
	 * in the table/s (say using constraint exclusion), it does not add modify
	 * table plan on the top. We should send queries to the remote nodes only
	 * when there is something to modify.
	 */
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
		topplan = create_remotedml_plan(root, topplan, mt->operation);

	return topplan;
}

/*
 * pgxc_make_param
 *
 * Helper function to make a parameter
 */
static Param *
pgxc_make_param(int param_num, Oid param_type)
{
	Param	*param;

	param = makeNode(Param);
	/* Parameter values are supplied from outside the plan */
	param->paramkind = PARAM_EXTERN;
	/* Parameters are numbered from 1 to n */
	param->paramid = param_num;
	param->paramtype = param_type;
	/* The following members are not required for deparsing */
	param->paramtypmod = -1;
	param->paramcollid = InvalidOid;
	param->location = -1;

	return param;
}

/*
 * pgxc_add_param_as_tle
 *
 * Helper function to add a parameter to the target list of the query
 */
static void
pgxc_add_param_as_tle(Query *query, int param_num, Oid param_type,
						char *resname)
{
	Param		*param;
	TargetEntry	*res_tle;

	param = pgxc_make_param(param_num, param_type);
	res_tle = makeTargetEntry((Expr *)param, param_num, resname, false);
	query->targetList = lappend(query->targetList, res_tle);
}

/*
 * pgxc_dml_add_qual_to_query
 *
 * This function adds a qual of the form sys_col_name = $? to a query
 * It is required while adding quals like ctid = $2 or xc_node_id = $3 to DMLs
 *
 * Parameters Description
 * query         : The qual will be added to this query
 * param_num     : The parameter number to use while adding the qual
 * sys_col_attno : Which system column to use for LHS of the = operator
 *               : SelfItemPointerAttributeNumber for ctid
 *               : XC_NodeIdAttributeNumber for xc_node_id
 * varno         : Index of this system column's relation in range table
 */
static void
pgxc_dml_add_qual_to_query(Query *query, int param_num,
							AttrNumber sys_col_attno, Index varno)
{
	Var			*lhs_var;
	Expr		*qual;
	Param		*rhs_param;

	/* Make a parameter expr for RHS of the = operator */
	rhs_param = pgxc_make_param(param_num, INT4OID);

	/* Make a system column ref expr for LHS of the = operator */
	lhs_var = makeVar(varno, sys_col_attno, INT4OID, -1, InvalidOid, 0);

	/* Make the new qual sys_column_name = $? */
	qual = make_op(NULL, list_make1(makeString("=")), (Node *)lhs_var,
									(Node *)rhs_param, -1);

	/* Add the qual to the qual list */
	query->jointree->quals = (Node *)lappend((List *)query->jointree->quals,
										(Node *)qual);
}

/*
 * pgxc_build_dml_statement
 *
 * Construct a Query structure for the query to be fired on the datanodes
 * and deparse it. Fields not set remain memzero'ed as set by makeNode.
 * Following is a description of all members of Query structure
 * when used for deparsing of non FQSed DMLs in XC.
 *
 * querySource		: Can be set to QSRC_ORIGINAL i.e. 0
 * queryId			: Not used in deparsing, can be 0
 * canSetTag		: Not used in deparsing, can be false
 * utilityStmt		: A DML is not a utility statement, keep it NULL
 * resultRelation	: Index of the target relation will be sent by the caller
 * hasAggs			: Our DML won't contain any aggregates in tlist, so false
 * hasWindowFuncs	: Our DML won't contain any window funcs in tlist, so false
 * hasSubLinks		: RemoteQuery does not support subquery, so false
 * hasDistinctOn	: Our DML wont contain any DISTINCT clause, so false
 * hasRecursive		: WITH RECURSIVE wont be specified in our DML, so false
 * hasModifyingCTE	: Our DML will not be in WITH, so false
 * hasForUpdate		: FOR UPDATE/SHARE can be there but not untill we support it
 * cteList			: WITH list will be NULL in our case
 * rtable			: We can set the rtable as being the same as the original query
 * jointree			: In XC we plan non FQSed DML's in such a maner that the
 *					: DML's to be sent to the datanodes do not contain joins,
 *					: so the join tree will not contain any thing in fromlist,
 *					: It will however contain quals and the number of quals
 *					: will always be fixed to two in case of UPDATE/DELETE &
 *					: zero in case of an INSERT. The quals will be of the
 *					: form ctid = $4 or xc_node_id = $5
 * targetList		: For DELETEs it will be NULL
 *					: For INSERTs it will be a list of params. The number of
 *					:             params will be the same as the number of
 *					:             enteries in the source data plan target list
 *					:             The targetList specifies the VALUES caluse
 *					:             e.g. INSERT INTO TAB VALUES ($1, $2, ...)
 *					: For UPDATEs it will be a list of parameters, the number
 *					:             of parameters will be the same as the number
 *					:             entries in the original query, however the
 *					:             parameter numbers will be the one where
 *					:             the target entry of the original query occurs
 *					:             in the source data plan target list
 *					:             The targetList specified the SET clause
 *					:             e.g. UPDATE tab SET c1 = $3, c2 = $5 ....
 * returningList	: will be provided by pgxc_add_returning_list
 * groupClause		: Our DML won't contin any so NULL.
 * havingQual		: Our DML won't contin any so NULL.
 * windowClause		: Our DML won't contin any so NULL.
 * distinctClause	: Our DML won't contin any so NULL.
 * sortClause		: Our DML won't contin any so NULL.
 * limitOffset		: Our DML won't contin any so NULL.
 * limitCount		: Our DML won't contin any so NULL.
 * rowMarks			: Will be NULL for now, may be used when we provide support
 *					: for WHERE CURRENT OF.
 * setOperations	: Our DML won't contin any so NULL.
 * constraintDeps	: Our DML won't contin any so NULL.
 * sql_statement	: Original query is not required for deparsing
 * is_local			: Not required for deparsing, keep 0
 * has_to_save_cmd_id	: Not required for deparsing, keep 0
 */
static void
pgxc_build_dml_statement(PlannerInfo *root, CmdType cmdtype,
						Index resultRelationIndex, RemoteQuery *rqplan,
						List *sourceTargetList)
{
	Query			*query_to_deparse;
	RangeTblRef		*target_table_ref;
	RangeTblEntry	*res_rel;
	ListCell		*elt;
	bool			ctid_found = false;
	bool			node_id_found = false;
	int				col_att = 0;
	int				ctid_param_num;
	ListCell		*lc;
	bool			can_use_pk_for_rep_change = false;
	int16			*indexed_col_numbers = NULL;
	int				index_col_count = 0;

	/* Make sure we are dealing with DMLs */
	if (cmdtype != CMD_UPDATE &&
		cmdtype != CMD_INSERT &&
		cmdtype != CMD_DELETE)
		return;

	/* First construct a reference to an entry in the query's rangetable */
	target_table_ref = makeNode(RangeTblRef);

	/* RangeTblRef::rtindex will be the same as indicated by the caller */
	target_table_ref->rtindex = resultRelationIndex;

	query_to_deparse = makeNode(Query);
	query_to_deparse->commandType = cmdtype;
	query_to_deparse->resultRelation = resultRelationIndex;

	/*
	 * While copying the range table to the query to deparse make sure we do
	 * not copy RTE's of type RTE_JOIN because set_deparse_for_query
	 * function expects that each RTE_JOIN is accompanied by a JoinExpr in
	 * Query's jointree, which is not true in case of XC's DML planning.
	 * We therefore fill the RTE's of type RTE_JOIN with dummy RTE entries.
	 * If each RTE of type RTE_JOIN is not accompanied by a corresponding
	 * JoinExpr in Query's jointree then set_deparse_for_query crashes
	 * when trying to set_join_column_names, because set_using_names did not
	 * call identify_join_columns to put valid values in
	 * deparse_columns::leftrti & deparse_columns::rightrti
	 * Instead of putting a check in set_join_column_names to return in case
	 * of invalid values in leftrti or rightrti, it is preferable to change
	 * code here and skip RTE's of type RTE_JOIN while copying
	 */
	foreach(lc, root->parse->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->rtekind == RTE_JOIN)
		{
			RangeTblEntry	*dummy_rte;
			char			*rte_name;

			rte_name = "_DUMMY_RTE_";
			dummy_rte = make_dummy_remote_rte(rte_name,
									makeAlias("_DUMMY_RTE_", NIL));

			query_to_deparse->rtable = lappend(query_to_deparse->rtable, dummy_rte);
		}
		else
		{
			query_to_deparse->rtable = lappend(query_to_deparse->rtable, rte);
		}
	}

	res_rel = rt_fetch(resultRelationIndex, query_to_deparse->rtable);
	Assert(res_rel->rtekind == RTE_RELATION);

	/* This RTE should appear in FROM clause of the SQL statement constructed */
	res_rel->inFromCl = true;

	query_to_deparse->jointree = makeNode(FromExpr);

	can_use_pk_for_rep_change = (cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE) &&
				IsRelationReplicated(GetRelationLocInfo(res_rel->relid));

	if (can_use_pk_for_rep_change)
	{
		index_col_count = pgxc_find_unique_index(res_rel->relid,
												&indexed_col_numbers);
		if (index_col_count <= 0)
		{
			if (RequirePKeyForRepTab)
				elog(ERROR, "Either primary key/unique index is required for a non-FQS DML on a replicated table");
			can_use_pk_for_rep_change = false;
		}

		if (can_use_pk_for_rep_change)
		{
			if (is_pk_being_changed(root->parse, indexed_col_numbers,
									index_col_count))
			{
				if (RequirePKeyForRepTab)
					elog(ERROR, "The primary key/column included in the unique index cannot be updated in a non-FQS query for a replicated table");
				can_use_pk_for_rep_change = false;
			}
		}
	}

	rqplan->rq_use_pk_for_rep_change = can_use_pk_for_rep_change;

	/*
	 * Prepare a param list for INSERT queries
	 * While doing so note the position of ctid, xc_node_id in source data
	 * plan's target list provided by the caller.
	 */
	foreach(elt, sourceTargetList)
	{
		TargetEntry	*tle = lfirst(elt);

		col_att++;

		/* The position of ctid/xc_node_id is not required for INSERT */
		if (!can_use_pk_for_rep_change && tle->resjunk &&
			(cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE))
		{
			Var *v = (Var *)tle->expr;

			if (v->varno == resultRelationIndex)
			{
				if (v->varattno == XC_NodeIdAttributeNumber)
				{
					if (node_id_found)
						elog(ERROR, "Duplicate node_ids not expected in source target list");
					node_id_found = true;
				}
				else if (v->varattno == SelfItemPointerAttributeNumber)
				{
					if (ctid_found)
						elog(ERROR, "Duplicate ctids not expected in source target list");
					ctid_found = true;
				}
			}

			continue;
		}

		/*
		 * Make sure the entry in the source target list belongs to the
		 * target table of the DML
		 */
		if (tle->resorigtbl != 0 && tle->resorigtbl != res_rel->relid)
			continue;

		if (cmdtype == CMD_INSERT)
		{
			/* Make sure the column has not been dropped */
			if (get_rte_attribute_is_dropped(res_rel, col_att))
				continue;

			/*
			 * Create the param to be used for VALUES caluse ($1, $2 ...)
			 * and add it to the query target list
			 */
			pgxc_add_param_as_tle(query_to_deparse, tle->resno,
									exprType((Node *) tle->expr), NULL);
		}
	}

	/*
	 * In XC we will update *all* the table attributes to reduce code
	 * complexity in finding the columns being updated, it works whether
	 * we have before row triggers defined on the table or not.
	 * The code complexity arises for the case of a table with child tables,
	 * where columns are added to parent using ALTER TABLE. The attribute
	 * number of the added column is different in parnet and child table.
	 * In this case we first have to use TargetEntry::resno to find the name
	 * of the column being updated in parent table, and then find the attribute
	 * number of that particular column in the child. This makes code complex.
	 * In comaprison if we choose to update all the columns of the table
	 * irrespective of the columns being updated, the code becomes simple
	 * and easy to read.
	 * Performance comparison between the two approaches (updating all columns
	 * and updating only the columns that were in the target list) shows that
	 * both the approaches give similar TPS in hour long runs of DBT1.
	 * In XC UPDATE will look like :
	 * UPDATE ... SET att1 = $1, att1 = $2, .... attn = $n WHERE ctid = $(n+1)
	 */
	if (cmdtype == CMD_UPDATE)
	{
		int			natts = get_relnatts(res_rel->relid);
		int			attnum;

		for (attnum = 1; attnum <= natts; attnum++)
		{
			/* Make sure the column has not been dropped */
			if (get_rte_attribute_is_dropped(res_rel, attnum))
				continue;

			pgxc_add_param_as_tle(query_to_deparse, attnum,
								get_atttype(res_rel->relid, attnum),
								get_attname(res_rel->relid, attnum));
		}

		/*
		 * The data row generated for BIND has all required values, plus NULL
		 * values for attributes that are not SET. The first n parameters are
		 * the n table attributes, followed by ctid and optionally node_id. So
		 * we know that the ctid has to be n + 1.
		 */
		if (!can_use_pk_for_rep_change)
		{
			ctid_param_num = natts + 1;
		}
	}
	if (cmdtype == CMD_DELETE)
	{
		if (!can_use_pk_for_rep_change)
		{
			/*
			 * Since there is no data to update, the first param is going to be
			 * ctid.
			 */
			ctid_param_num = 1;
		}
	}

	/* Add quals like ctid = $4 AND xc_node_id = $6 to the UPDATE/DELETE query */
	if (cmdtype == CMD_UPDATE || cmdtype == CMD_DELETE)
	{
		/*
		 * If it is not replicated, we can use CTID, otherwise we need
		 * to use a defined primary key
		 */
		if (!can_use_pk_for_rep_change)
		{
			if (!ctid_found)
				elog(ERROR, "Source data plan's target list does not contain ctid colum");

			/*
			 * Beware, the ordering of ctid and node_id is important ! ctid should
			 * be followed by node_id, not vice-versa, so as to be consistent with
			 * the data row to be generated while binding the parameters for the
			 * update statement.
			 */
			pgxc_dml_add_qual_to_query(query_to_deparse, ctid_param_num,
							SelfItemPointerAttributeNumber, resultRelationIndex);

			if (node_id_found)
			{
				pgxc_dml_add_qual_to_query(query_to_deparse, ctid_param_num + 1,
								XC_NodeIdAttributeNumber, resultRelationIndex);
			}
		}
		else
		{
			/*
			 * Add all the columns of the primary key or unique index
			 * in the where clause of update / delete on the replicated table
			 */
			int i;
			for (i = 0; i < index_col_count; i++)
			{
				int			pkattno = indexed_col_numbers[i];

				col_att = 0;
				foreach(elt, sourceTargetList)
				{
					TargetEntry	*tle = lfirst(elt);
					Var *v;
			
					col_att++;
			
					v = (Var *)tle->expr;
		
					if (v->varno == resultRelationIndex &&
						v->varattno == pkattno)
					{
						break;
					}
				}

				pgxc_dml_add_qual_to_query(query_to_deparse, col_att,
							pkattno, resultRelationIndex);
			}

			/*
			 * Save the maximum number of parameters that should be sent down
			 * to the datanode while executing this delete. Sending extra
			 * parametrs causes problems in case those extra contain composite
			 * types, because input of annon composite types is not implemented
			 * For example see the delete query in JDBC regression
			 * File : XC_02_AbsenteesTest.java
			 * Function : testDeletePersonnel_3
			 */
			if (cmdtype == CMD_DELETE)
				rqplan->rq_max_param_num = col_att;
		}
		query_to_deparse->jointree->quals = (Node *)make_andclause(
						(List *)query_to_deparse->jointree->quals);
	}

	if (indexed_col_numbers != NULL)
		pfree(indexed_col_numbers);

	/* pgxc_add_returning_list copied returning list in base_tlist */
	if (rqplan->base_tlist)
		query_to_deparse->returningList = list_copy(rqplan->base_tlist);

	rqplan->remote_query = query_to_deparse;

	pgxc_rqplan_build_statement(rqplan);
}

/*
 * create_remotedml_plan()
 *
 * For every target relation, add a remote query node to carry out remote
 * operations.
 */
Plan *
create_remotedml_plan(PlannerInfo *root, Plan *topplan, CmdType cmdtyp)
{
	ModifyTable			*mt = (ModifyTable *)topplan;
	ListCell			*rel;
	int					relcount = -1;
	RelationAccessType	accessType;

	/* We expect to work only on ModifyTable node */
	if (!IsA(topplan, ModifyTable))
		elog(ERROR, "Unexpected node type: %d", topplan->type);

	switch(cmdtyp)
	{
		case CMD_UPDATE:
		case CMD_DELETE:
			accessType = RELATION_ACCESS_UPDATE;
			break;

		case CMD_INSERT:
			accessType = RELATION_ACCESS_INSERT;
			break;

		default:
			elog(ERROR, "Unexpected command type: %d", cmdtyp);
			return NULL;
	}

	/*
	 * For every result relation, build a remote plan to execute remote DML.
	 */
	foreach(rel, mt->resultRelations)
	{
		Index			resultRelationIndex = lfirst_int(rel);
		RangeTblEntry	*res_rel;
		RelationLocInfo	*rel_loc_info;
		RemoteQuery		*fstep;
		RangeTblEntry	*dummy_rte;			/* RTE for the remote query node */
		Plan			*sourceDataPlan;	/* plan producing source data */
		char			*relname;

		relcount++;

		res_rel = rt_fetch(resultRelationIndex, root->parse->rtable);

		/* Bad relation ? */
		if (res_rel == NULL || res_rel->rtekind != RTE_RELATION)
			continue;

		relname = get_rel_name(res_rel->relid);

		/* Get location info of the target table */
		rel_loc_info = GetRelationLocInfo(res_rel->relid);
		if (rel_loc_info == NULL)
			continue;

		fstep = make_remotequery(NIL, NIL, resultRelationIndex);

		/*
		 * DML planning generates its own parameters that refer to the source
		 * data plan.
		 */
		fstep->rq_params_internal = true;

		fstep->is_temp = IsTempTable(res_rel->relid);
		fstep->read_only = false;

		if (mt->returningLists)
			pgxc_add_returning_list(fstep,
									list_nth(mt->returningLists, relcount),
									resultRelationIndex);

		/* Get the plan that is supposed to supply source data to this plan */
		sourceDataPlan = list_nth(mt->plans, relcount);

		pgxc_build_dml_statement(root, cmdtyp, resultRelationIndex, fstep,
									sourceDataPlan->targetlist);

		fstep->combine_type = get_plan_combine_type(cmdtyp,
													rel_loc_info->locatorType);

		if (cmdtyp == CMD_INSERT)
		{
			fstep->exec_nodes = makeNode(ExecNodes);
			fstep->exec_nodes->accesstype = accessType;
			fstep->exec_nodes->baselocatortype = rel_loc_info->locatorType;
			fstep->exec_nodes->primarynodelist = NULL;
			fstep->exec_nodes->nodeList = rel_loc_info->nodeList;
		}
		else
		{
			fstep->exec_nodes = GetRelationNodes(rel_loc_info, 0, true,
												UNKNOWNOID, accessType);
		}
		fstep->exec_nodes->en_relid = res_rel->relid;

		if (cmdtyp != CMD_DELETE)
			fstep->exec_nodes->en_expr = pgxc_set_en_expr(res_rel->relid,
														resultRelationIndex);

		dummy_rte = make_dummy_remote_rte(relname,
										makeAlias("REMOTE_DML_QUERY", NIL));
		root->parse->rtable = lappend(root->parse->rtable, dummy_rte);
		fstep->scan.scanrelid = list_length(root->parse->rtable);

		mt->remote_plans = lappend(mt->remote_plans, fstep);
	}

	return (Plan *)mt;
}

/*
 * create_remotegrouping_plan
 * Check if the grouping and aggregates can be pushed down to the
 * Datanodes.
 * Right now we can push with following restrictions
 * 1. there are plain aggregates (no expressions involving aggregates) and/or
 *    expressions in group by clauses
 * 2. No distinct or order by clauses
 * 3. No windowing clause
 * 4. No having clause
 *
 * Inputs
 * root - planerInfo root for this query
 * agg_plan - local grouping plan produced by grouping_planner()
 *
 * PGXCTODO: work on reducing these restrictions as much or document the reasons
 * why we need the restrictions, in these comments themselves. In case of
 * replicated tables, we should be able to push the whole query to the data
 * node in case there are no local clauses.
 */
Plan *
create_remotegrouping_plan(PlannerInfo *root, Plan *local_plan)
{
	Query		*query = root->parse;
	Sort		*sort_plan;
	RemoteQuery	*remote_scan;	/* remote query in the passed in plan */
	Plan		*temp_plan;
	List		*base_tlist;
	RangeTblEntry	*dummy_rte;
	int			numGroupCols;
	AttrNumber	*grpColIdx;
	List		*remote_qual;
	List 		*local_qual;
	bool		single_node_grouping;/* Can grouping be entirely performed
									 * on a given datanode?
									 */
	List		*remote_scan_tlist;
	Plan 		*local_plan_left;

	/* Remote grouping is not enabled, don't do anything */
	if (!enable_remotegroup)
		return local_plan;

	/* If the query has ungrouped columns which are functionally dependent upon
	 * grouping columns, we can not push GROUP BY clause and aggregates. The
	 * reason being two fold
	 * 1. We generate the query to be pushed down by using a lot of subqueries,
	 * representing various relations in the JOIN tree. PostreSQL can not handle
	 * functional dependence involving subqueries. Thus pushing down GROUP BY
	 * and aggregates results in error, since Datanode/s find ungrouped columns
	 * in the query which are not part of GROUP BY clause.
	 * 2. A query with ungrouped columns functionally dependent upon GROUP BY
	 * columns is likely to produce single row groups (e.g. grouping by primary
	 * key or unique key etc.). If we push down aggregates in such query, we
	 * will end up un-necessarily applying combination function at the
	 * coordinator, thus decreasing the performance. Thus for such queries it's
	 * unlikely that we will gain performance by pushing down.
	 */
	if (query->constraintDeps && list_length(query->constraintDeps) > 0)
		return local_plan;

	/*
	 * PGXCTODO: These optimizations do not work in presence of the window functions,
	 * because of the target list adjustments. The targetlist set for the passed
	 * in Group/Agg plan nodes contains window functions if any, but gets
	 * changed while planning for windowing. So, for now stay away :)
	 */
	if (query->hasWindowFuncs)
		return local_plan;

	/* for now only Agg/Group plans */
	if (local_plan && IsA(local_plan, Agg))
	{
		numGroupCols = ((Agg *)local_plan)->numCols;
		grpColIdx = ((Agg *)local_plan)->grpColIdx;
	}
	else if (local_plan && IsA(local_plan, Group))
	{
		numGroupCols = ((Group *)local_plan)->numCols;
		grpColIdx = ((Group *)local_plan)->grpColIdx;
	}
	else
		return local_plan;

	/*
	 * We expect plan tree as Group/Agg->Sort->Result->Material->RemoteQuery,
	 * Result, Material nodes are optional. Sort is compulsory for Group but not
	 * for Agg.
	 * anything else is not handled right now.
	 */
	temp_plan = local_plan->lefttree;
	remote_scan = NULL;
	sort_plan = NULL;
	if (temp_plan && IsA(temp_plan, Sort))
	{
		sort_plan = (Sort *)temp_plan;
		temp_plan = temp_plan->lefttree;
	}

	if (temp_plan && IsA(temp_plan, RemoteQuery))
		remote_scan = (RemoteQuery *)temp_plan;

	if (!remote_scan)
		return local_plan;
	/*
	 * for Group plan we expect Sort under the Group, which is always the case,
	 * the condition below is really for some possibly non-existent case.
	 */
	if (IsA(local_plan, Group) && !sort_plan)
		return local_plan;
	/*
	 * If this is grouping/aggregation by sorting, we need the enable_remotesort
	 * to be ON to push the sorting and grouping down to the Datanode.
	 */
	if (sort_plan && !enable_remotesort)
		return local_plan;
	/*
	 * If the remote_scan has any quals on it, those need to be executed before
	 * doing anything. Hence we won't be able to push any aggregates or grouping
	 * to the Datanode.
	 * If it has any SimpleSort in it, then sorting is intended to be applied
	 * before doing anything. Hence can not push any aggregates or grouping to
	 * the Datanode.
	 */
	if (remote_scan->scan.plan.qual)
		return local_plan;

	/*
	 * Grouping_planner may add Sort node to sort the rows
	 * based on the columns in GROUP BY clause. Hence the columns in Sort and
	 * those in Group node in should be same. The columns are usually in the
	 * same order in both nodes, hence check the equality in order. If this
	 * condition fails, we can not handle this plan for now.
	 */
	if (sort_plan)
	{
		int cntCols;
		if (sort_plan->numCols != numGroupCols)
			return local_plan;
		for (cntCols = 0; cntCols < numGroupCols; cntCols++)
		{
			if (sort_plan->sortColIdx[cntCols] != grpColIdx[cntCols])
				return local_plan;
		}
	}

	/*
	 * If group by clause has distribution column in it or if there is only one
	 * datanode involved, the evaluation of aggregates and grouping involves
	 * only a single datanode.
	 */
	if (pgxc_query_has_distcolgrouping(query, remote_scan->exec_nodes) ||
		list_length(remote_scan->exec_nodes->nodeList) == 1)
		single_node_grouping = true;
	else
		single_node_grouping = false;
	/*
	 * If we are able to completely evaluate the aggregates on datanodes, we
	 * need to ask datanode/s to finalise the aggregates
	 */
	remote_scan->rq_finalise_aggs = single_node_grouping;
	/*
	 * We want sort and group references to be included as column numbers in the
	 * query to be sent to the datanodes.
	 */
	remote_scan->rq_sortgroup_colno = true;

	/*
	 * Build the targetlist of the query plan.
	 */
	remote_scan_tlist = pgxc_process_grouping_targetlist(local_plan->targetlist,
													single_node_grouping);
	remote_scan_tlist = pgxc_process_having_clause(remote_scan_tlist, single_node_grouping,
											query->havingQual, &local_qual,
											&remote_qual);
	/*
	 * If can not construct a targetlist shippable to the Datanode. Resort to
	 * the plan created by grouping_planner()
	 */
	if (!remote_scan_tlist)
		return local_plan;
	/*
	 * Copy the quals to be included in the Query to be sent to the datanodes,
	 * so that the nodes (e.g. Vars) can be changed.
	 */
	remote_qual = copyObject(remote_qual);
	/* Modify the targetlist of the JOIN plan with the new targetlist */
	remote_scan->scan.plan.targetlist = remote_scan_tlist;
	/* we need to adjust the Vars in HAVING clause to be shipped */
	pgxc_rqplan_adjust_vars(remote_scan, (Node *)remote_qual);
	/* Attach the local quals at appropriate plan */
	if (single_node_grouping)
		remote_scan->scan.plan.qual = local_qual;
	else
		local_plan->qual = local_qual;

	remote_scan->remote_query->havingQual =
		(Node *)(remote_qual ? make_ands_explicit(remote_qual) : NULL);

	/*
	 * Generate the targetlist to be shipped to the datanode, so that we can
	 * check whether we are able to ship the grouping clauses to the datanode/s.
	 * If the original query has aggregates, set the same in Query to be sent to
	 * the datanodes, so that we have right expectations about the aggregates
	 * while generating the targetlist.
	 */
	remote_scan->remote_query->hasAggs = query->hasAggs;
	pgxc_rqplan_adjust_tlist(remote_scan);
	base_tlist = remote_scan->base_tlist;


	/*
	 * recompute the column ids of the grouping columns,
	 * the group column indexes computed earlier point in the
	 * targetlists of the scan plans under Agg/Group node. But now the grouping
	 * column indexes will be pointing in the targetlist of the new
	 * RemoteQuery, hence those need to be recomputed
	 * If we couldn't locate a particular GROUP BY
	 */
	if (!pgxc_locate_grouping_columns(root, base_tlist, grpColIdx))
		return local_plan;
	/*
	 * We have adjusted the targetlist of the RemoteQuery underneath to suit the
	 * GROUP BY clause. Now if sorting is used for aggregation we need to push
	 * down the ORDER BY clause as well. In this case, the targetlist of the
	 * Sort plan should be same as that of the RemoteQuery plan.
	 */
	if (sort_plan)
	{
		Assert(query->groupClause);
		Assert(sort_plan->plan.lefttree == (Plan *)remote_scan);
		sort_plan = make_sort_from_groupcols(root, query->groupClause,
												grpColIdx, (Plan *)remote_scan);
		local_plan_left = create_remotesort_plan(root, (Plan *)sort_plan);
		if (IsA(local_plan_left, Sort))
			Assert(((Sort *)local_plan_left)->srt_start_merge);
	}
	else
		local_plan_left = (Plan *)remote_scan;

	/* Add the GROUP BY clause to the JOIN query */
	remote_scan->remote_query->groupClause = query->groupClause;

	/*
	 * We have added new clauses to the query being shipped to the datanode/s,
	 * re-build the quals.
	 */
	pgxc_rqplan_build_statement(remote_scan);

	/* Change the dummy RTE added to show the new place of reduction */
	dummy_rte = rt_fetch(remote_scan->scan.scanrelid, query->rtable);
	dummy_rte->relname = "__REMOTE_GROUP_QUERY__";
	dummy_rte->eref = makeAlias("__REMOTE_GROUP_QUERY__", NIL);

	/*
	 * If we can finalise the aggregates on the datanode/s, we don't need the
	 * covering Agg/Group plan.
	 */
	if (single_node_grouping)
		return local_plan_left;
	else
	{
		local_plan->lefttree = local_plan_left;
		/* indicate that we should apply collection function directly */
		if (IsA(local_plan, Agg))
			((Agg *)local_plan)->skip_trans = true;
		return local_plan;
	}
}

/*
 * pgxc_locate_grouping_columns
 * Locates the grouping clauses in the given target list. This is very similar
 * to locate_grouping_columns except that there is only one target list to
 * search into.
 * PGXCTODO: Can we reuse locate_grouping_columns() instead of writing this
 * function? But this function is optimized to search in the same target list.
 */
static bool
pgxc_locate_grouping_columns(PlannerInfo *root, List *tlist,
								AttrNumber *groupColIdx)
{
	int			keyno = 0;
	ListCell   *gl;

	Assert(!root->parse->groupClause || groupColIdx != NULL);

	foreach(gl, root->parse->groupClause)
	{
		SortGroupClause *grpcl = (SortGroupClause *) lfirst(gl);
		TargetEntry *te = get_sortgroupclause_tle(grpcl, tlist);
		if (!te)
			return false;
		groupColIdx[keyno++] = te->resno;
	}
	return true;
}

/*
 * pgxc_add_to_flat_tlist
 * Add the given node to the target list to be sent to the Datanode. This is
 * similar to add_to_flat_tlist, except that it preserves the ressortgroupref
 * member in TargetEntry.
 *
 * Note about ressortgroupref
 * While constructing targetlist to be shipped to the datanode/s we try to
 * accomodate the GROUP BY or ORDER BY clauses if possible. So, we need to have
 * the ressortgroupref set in the query targetlist. Hence this function. We
 * consider following cases to set the given ressortgroupref.
 * 1. If the expression does not already exist, it is added to the list and
 * given ressortgroupref is set.
 * 2. If the expression already exists, and does not have ressortgroupref in the
 * target entry, the given one is set.
 * 3. PGXCTODO: If the expression already exists but has a different ressortgroupref set
 * in the target entry, we set that in target entry to 0 to avoid conflicts.
 * This would disable further optimizations but for the time being it's safe for
 * correctness.
 */
static List *
pgxc_add_to_flat_tlist(List *remote_tlist, Node *expr, Index ressortgroupref)
{
	TargetEntry *remote_tle;


	remote_tle = tlist_member(expr, remote_tlist);

	if (!remote_tle)
	{
		remote_tle = makeTargetEntry(copyObject(expr),
							  list_length(remote_tlist) + 1,
							  NULL,
							  false);
		/* Copy GROUP BY/SORT BY reference for the locating group by columns */
		remote_tle->ressortgroupref = ressortgroupref;
		remote_tlist = lappend(remote_tlist, remote_tle);
	}
	else
	{
		if (remote_tle->ressortgroupref == 0)
			remote_tle->ressortgroupref = ressortgroupref;
		else if (ressortgroupref == 0)
		{
			/* do nothing remote_tle->ressortgroupref has the right value */
		}
		else
		{
			/*
			 * if the expression's TLE already has a Sorting/Grouping reference,
			 * and caller has passed a non-zero one as well, the same expression
			 * is being used twice in the targetlist with different aliases. Do
			 * not set ressortgroupref for either of them. The callers need to
			 * set the ressortgroupref because they want to find the sort or
			 * group column references in the targetlists. If we don't set
			 * ressortgroupref, the optimizations will fail, which at least
			 * doesn't have any correctness issue.
			 */
			if (remote_tle->ressortgroupref != ressortgroupref)
				remote_tle->ressortgroupref = 0;
		}
	}

	return remote_tlist;
}
/*
 * pgxc_process_grouping_targetlist
 * The function scans the targetlist to build the targetlist of RemoteQuery
 * plan. While doing so, it checks for possibility of shipping the aggregates to
 * the datanodes. Either we push none or all of the aggregates and the aggregate
 * expressions need to shippable.
 * If aggregates can be completely evaluated on datanode/s, we don't need covering
 * Agg/Group plan. RemoteQuery node replaces the covering Agg/Group node. In
 * such case the targetlist passed in becomes targetlist of the RemoteQuery
 * node.
 */
static List *
pgxc_process_grouping_targetlist(List *local_tlist, bool single_node_grouping)
{
	List		*remote_tlist = NIL;
	ListCell	*temp;

	if (single_node_grouping)
	{
		/* Check that all the aggregates in the targetlist are shippable */
		List 		*aggs_n_vars = pull_var_clause((Node *)local_tlist, PVC_INCLUDE_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS);
		ListCell	*lcell;
		bool		tmp_has_aggs;

		foreach (lcell, aggs_n_vars)
		{
			Expr	*av_expr = (Expr *)lfirst(lcell);
			Assert(IsA(av_expr, Aggref) || IsA(av_expr, Var));

			/*
			 * If the tree routed under the aggregate expression is not
			 * shippable, we can not ship the entire targetlist.
			 */
			if (IsA(av_expr, Aggref) &&
				!pgxc_is_expr_shippable(av_expr, &tmp_has_aggs))
				return NULL;
		}
		remote_tlist = copyObject(local_tlist);
	}
	else
	{
		/*
		 * For every expression check if it has aggregates in it, if it has
		 * check whether those aggregates are shippable and include the
		 * aggregates and Vars outside the aggregates in the RemoteQuery plan's
		 * targetlist.
		 * We can push all aggregates or none. Hence even if there is a single
		 * non-shippable aggregate, we can not ship any other aggregates, and
		 * resort to the complete aggregation and grouping on the datanode.
		 * If the expression does not have any aggregates, add it as it is to
		 * the RemoteQuery plan's targetlist
		 */
		foreach(temp, local_tlist)
		{
			TargetEntry	*local_tle = lfirst(temp);
			Node		*expr = (Node *)local_tle->expr;
			bool		has_agg = false;
			bool		tmp_has_aggs;
			ListCell	*lcell;
			List		*aggs_n_vars;

			aggs_n_vars = pull_var_clause(expr, PVC_INCLUDE_AGGREGATES,
												PVC_RECURSE_PLACEHOLDERS);

			/*
			 * See if there are any aggregates in the current target entry and
			 * whether those aggregates are shippable.
			 */
			foreach (lcell, aggs_n_vars)
			{
				Expr	*av_expr = (Expr *)lfirst(lcell);
				Assert(IsA(av_expr, Aggref) || IsA(av_expr, Var));

				/*
				 * If the tree rooted under the aggregate expression is not
				 * shippable, we can not ship the entire targetlist.
				 */
				if (IsA(av_expr, Aggref))
				{
					has_agg = true;
					if (!pgxc_is_expr_shippable(av_expr, &tmp_has_aggs))
						return NULL;
				}
			}

			/*
			 * If there are aggregates in the target list or if the expression
			 * is not shippable in itself, we should just project the aggregates
			 * and Vars outside the aggregates to upper nodes, since rest of
			 * the expression needs to be evaluated after we have finalised the
			 * aggregates.
			 */
			if (has_agg || !pgxc_is_expr_shippable((Expr *)local_tle, NULL))
			{
				/*
				 * It's fine to use add_to_flat_tlist here, since broken down
				 * targetlist entry can not have ressortgroupref set.
				 */
				remote_tlist = add_to_flat_tlist(remote_tlist, aggs_n_vars);
			}
			else
			{
				TargetEntry 	*remote_tle = copyObject(local_tle);
				remote_tle->resno = list_length(remote_tlist) + 1;
				remote_tlist = lappend(remote_tlist, remote_tle);
			}
		}
	}
	return remote_tlist;
}

/*
 * pgxc_process_having_clause
 * For every expression in the havingQual take following action
 * 1. If it has aggregates, which can be evaluated at the Datanodes, add those
 *    aggregates to the targetlist and modify the local aggregate expressions to
 *    point to the aggregate expressions being pushed to the Datanode. Add this
 *    expression to the local qual to be evaluated locally.
 * 2. If the expression does not have aggregates and the whole expression can be
 *    evaluated at the Datanode, add the expression to the remote qual to be
 *    evaluated at the Datanode.
 * 3. If qual contains an aggregate which can not be evaluated at the data
 *    node, the parent group plan can not be reduced to a remote_query.
 */
static List *
pgxc_process_having_clause(List *remote_tlist, bool single_node_grouping,
							Node *havingQual, List **local_qual,
							List **remote_qual)
{
	ListCell	*temp;
	List		*quals;

	*remote_qual = NIL;
	*local_qual = NIL;

	if (!havingQual)
		return remote_tlist;
	/*
	 * PGXCTODO: we expect the quals in the form of List only. Is there a
	 * possibility that the quals will be another form?
	 */
	if (!IsA(havingQual, List))
		return NULL;

	quals = (List *)havingQual;
	foreach(temp, quals)
	{
		Node	*expr = lfirst(temp);
		bool	has_aggs;
		List	*vars_n_aggs;
		bool	shippable_qual;

		shippable_qual = pgxc_is_expr_shippable((Expr *)expr, &has_aggs);

		/*
		 * If the expression is not shippable OR
		 * if there are aggregates involved in the expression, whole expression
		 * can not be pushed to the Datanode if grouping involves more than one
		 * datanode. Pick up the aggregates and the VAR nodes not covered by
		 * aggreagetes.
		 * We can push all aggregates or none. Hence even if there is a single
		 * non-shippable aggregate, we can not ship any other aggregates, and
		 * resort to the complete aggregation and grouping on the datanode.
		 */
		if (!shippable_qual || (!single_node_grouping && has_aggs))
		{
			ListCell	*lcell;

			/* Pull the aggregates and var nodes from the quals */
			vars_n_aggs = pull_var_clause(expr, PVC_INCLUDE_AGGREGATES,
											PVC_RECURSE_PLACEHOLDERS);
			/* copy the aggregates into the remote target list */
			foreach (lcell, vars_n_aggs)
			{
				Node 	*agg_or_var = lfirst(lcell);
				bool	tmp_has_agg;

				Assert(IsA(agg_or_var, Aggref) || IsA(agg_or_var, Var));
				if (IsA(agg_or_var, Aggref) &&
					!pgxc_is_expr_shippable((Expr *)agg_or_var, &tmp_has_agg))
					return NULL;

				/*
				 * If the aggregation can not be evaluated on a single, we will
				 * need a covering Agg/Group plan, where the aggregates will be
				 * finalised and quals will be applied. In that case, the
				 * targetlist of the RemoteQuery plan needs to have the
				 * transitioned aggregates to be projected to Agg/Group plan.
				 */
				if (!single_node_grouping)
					remote_tlist = pgxc_add_to_flat_tlist(remote_tlist,
																agg_or_var, 0);
			}
			*local_qual = lappend(*local_qual, expr);
		}
		else
			*remote_qual = lappend(*remote_qual, expr);
	}
	return remote_tlist;
}

/*
 * pgxc_set_en_expr
 * Try to find the expression of distribution column to calculate node at plan execution
 */
static Expr *
pgxc_set_en_expr(Oid tableoid, Index resultRelationIndex)
{
	HeapTuple tp;
	Form_pg_attribute partAttrTup;
	Var	*var;
	RelationLocInfo *rel_loc_info;

	/* Get location info of the target table */
	rel_loc_info = GetRelationLocInfo(tableoid);
	if (rel_loc_info == NULL)
		 return NULL;

	/*
	 * For hash/modulo distributed tables, the target node must be selected
	 * at the execution time based on the partition column value.
	 *
	 * For round robin distributed tables, tuples must be divided equally
	 * between the nodes.
	 *
	 * For replicated tables, tuple must be inserted in all the Datanodes
	 *
	 * XXX Need further testing for replicated and round-robin tables
	 */
	if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH &&
		rel_loc_info->locatorType != LOCATOR_TYPE_MODULO)
		return NULL;

	tp = SearchSysCache(ATTNUM,
						ObjectIdGetDatum(tableoid),
						Int16GetDatum(rel_loc_info->partAttrNum),
						0, 0);
	partAttrTup = (Form_pg_attribute) GETSTRUCT(tp);

	/*
	 * Create a Var for the distribution column and set it for
	 * execution time evaluation of target node. ExecEvalVar() picks
	 * up values from ecxt_scantuple if Var does not refer either OUTER
	 * or INNER varno. We utilize that mechanism to pick up values from
	 * the tuple returned by the current plan node
	 */
	var = makeVar(resultRelationIndex,
				  rel_loc_info->partAttrNum,
				  partAttrTup->atttypid,
				  partAttrTup->atttypmod,
				  partAttrTup->attcollation,
				  0);
	ReleaseSysCache(tp);

	return (Expr *) var;
}

static RangeTblEntry *
make_dummy_remote_rte(char *relname, Alias *alias)
{
	RangeTblEntry *dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_REMOTE_DUMMY;

	/* use a dummy relname... */
	dummy_rte->relname		 = relname;
	dummy_rte->eref			 = alias;

	return dummy_rte;
}

/*
 * make_ctid_col_ref
 *
 * creates a Var for a column referring to ctid
 */

static Var *
make_ctid_col_ref(Query *qry)
{
	ListCell		*lc1, *lc2;
	RangeTblEntry		*rte1, *rte2;
	int			tableRTEs, firstTableRTENumber;
	RangeTblEntry		*rte_in_query = NULL;
	AttrNumber		attnum;
	Oid			vartypeid;
	int32			type_mod;
	Oid			varcollid;

	/*
	 * If the query has more than 1 table RTEs where both are different, we can not add ctid to the query target list
	 * We should in this case skip adding it to the target list and a WHERE CURRENT OF should then
	 * fail saying the query is not a simply update able scan of table
	 */

	tableRTEs = 0;
	foreach(lc1, qry->rtable)
	{
		rte1 = (RangeTblEntry *) lfirst(lc1);

		if (rte1->rtekind == RTE_RELATION)
		{
			tableRTEs++;
			if (tableRTEs > 1)
			{
				/*
				 * See if we get two RTEs in case we have two references
				 * to the same table with different aliases
				 */
				foreach(lc2, qry->rtable)
				{
					rte2 = (RangeTblEntry *) lfirst(lc2);

					if (rte2->rtekind == RTE_RELATION)
					{
						if (rte2->relid != rte1->relid)
						{
							return NULL;
						}
					}
				}
				continue;
			}
			rte_in_query = rte1;
		}
	}

	if (tableRTEs > 1)
	{
		firstTableRTENumber = 0;
		foreach(lc1, qry->rtable)
		{
			rte1 = (RangeTblEntry *) lfirst(lc1);
			firstTableRTENumber++;
			if (rte1->rtekind == RTE_RELATION)
			{
				break;
			}
		}
	}
	else
	{
		firstTableRTENumber = 1;
	}

	attnum = specialAttNum("ctid");
	Assert(rte_in_query);
	get_rte_attribute_type(rte_in_query, attnum, &vartypeid, &type_mod, &varcollid);
	return makeVar(firstTableRTENumber, attnum, vartypeid, type_mod, varcollid, 0);
}


/*
 * Returns true if at least one temporary table is in use
 * in query (and its subqueries)
 */
static bool
contains_temp_tables(List *rtable)
{
	ListCell *item;

	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (IsTempTable(rte->relid))
				return true;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 contains_temp_tables(rte->subquery->rtable))
			return true;
	}

	return false;
}

/*
 * get_plan_combine_type - determine combine type
 *
 * COMBINE_TYPE_SAME - for replicated updates
 * COMBINE_TYPE_SUM - for hash and round robin updates
 * COMBINE_TYPE_NONE - for operations where row_count is not applicable
 *
 * return NULL if it is not safe to be done in a single step.
 */
static CombineType
get_plan_combine_type(CmdType commandType, char baselocatortype)
{

	switch (commandType)
	{
		case CMD_INSERT:
		case CMD_UPDATE:
		case CMD_DELETE:
			return baselocatortype == LOCATOR_TYPE_REPLICATED ?
					COMBINE_TYPE_SAME : COMBINE_TYPE_SUM;

		default:
			return COMBINE_TYPE_NONE;
	}
	/* quiet compiler warning */
	return COMBINE_TYPE_NONE;
}

/*
 * get oid of the function whose name is passed as argument
 */

static Oid
get_fn_oid(char *fn_name, Oid *p_rettype)
{
	Value		*fn_nm;
	List		*fn_name_list;
	FuncDetailCode	fdc;
	bool		retset;
	int		nvargs;
	Oid		*true_typeids;
	Oid		func_oid;

	fn_nm = makeString(fn_name);
	fn_name_list = list_make1(fn_nm);

	fdc = func_get_detail(fn_name_list,
				NULL,			/* argument expressions */
				NULL,			/* argument names */
				0,			/* argument numbers */
				NULL,			/* argument types */
				false,			/* expand variable number or args */
				false,			/* expand defaults */
				&func_oid,		/* oid of the function - returned detail*/
				p_rettype,		/* function return type - returned detail */
				&retset,		/*  - returned detail*/
				&nvargs,		/*  - returned detail*/
				&true_typeids,		/*  - returned detail */
				NULL			/* arguemnt defaults returned*/
				);

	pfree(fn_name_list);
	if (fdc == FUNCDETAIL_NORMAL)
	{
		return func_oid;
	}
	return InvalidOid;
}

/*
 * Append ctid to the field list of step queries to support update
 * WHERE CURRENT OF. The ctid is not sent down to client but used as a key
 * to find target tuple.
 * PGXCTODO: Bug
 * This function modifies the original query to add ctid
 * and nodename in the targetlist. It should rather modify the targetlist of the
 * query to be shipped by the RemoteQuery node.
 */
static void
fetch_ctid_of(Plan *subtree, Query *query)
{
	/* recursively process subnodes */
	if (innerPlan(subtree))
		fetch_ctid_of(innerPlan(subtree), query);
	if (outerPlan(subtree))
		fetch_ctid_of(outerPlan(subtree), query);

	/* we are only interested in RemoteQueries */
	if (IsA(subtree, RemoteQuery))
	{
		RemoteQuery		*step = (RemoteQuery *) subtree;
		TargetEntry		*te1;
		Query			*temp_qry;
		FuncExpr		*func_expr;
		AttrNumber		resno;
		Oid			funcid;
		Oid			rettype;
		Var			*ctid_expr;
		MemoryContext		oldcontext;
		MemoryContext		tmpcontext;

		tmpcontext = AllocSetContextCreate(CurrentMemoryContext,
							"Temp Context",
							ALLOCSET_DEFAULT_MINSIZE,
							ALLOCSET_DEFAULT_INITSIZE,
							ALLOCSET_DEFAULT_MAXSIZE);
		oldcontext = MemoryContextSwitchTo(tmpcontext);

		/* Copy the query tree to make changes to the target list */
		temp_qry = copyObject(query);
		/* Get the number of entries in the target list */
		resno = list_length(temp_qry->targetList);

		/* Make a ctid column ref expr to add in target list */
		ctid_expr = make_ctid_col_ref(temp_qry);
		if (ctid_expr == NULL)
		{
			MemoryContextSwitchTo(oldcontext);
			MemoryContextDelete(tmpcontext);
			return;
		}

		te1 = makeTargetEntry((Expr *)ctid_expr, resno+1, NULL, false);

		/* add the target entry to the query target list */
		temp_qry->targetList = lappend(temp_qry->targetList, te1);

		/* PGXCTODO We can take this call in initialization rather than getting it always */

		/* Get the Oid of the function */
		funcid = get_fn_oid("pgxc_node_str", &rettype);
		if (OidIsValid(funcid))
		{
			StringInfoData		deparsed_qry;
			TargetEntry		*te2;

			/* create a function expression */
			func_expr = makeFuncExpr(funcid, rettype, NULL, InvalidOid, InvalidOid, COERCION_IMPLICIT); /* Just a tentative fix.  Need Ashutosh's review. K.Suzuki */
			/* make a target entry for function call */
			te2 = makeTargetEntry((Expr *)func_expr, resno+2, NULL, false);
			/* add the target entry to the query target list */
			temp_qry->targetList = lappend(temp_qry->targetList, te2);

			initStringInfo(&deparsed_qry);
			deparse_query(temp_qry, &deparsed_qry, NIL, false, false);

			MemoryContextSwitchTo(oldcontext);

			if (step->sql_statement != NULL)
				pfree(step->sql_statement);

			step->sql_statement = pstrdup(deparsed_qry.data);

			MemoryContextDelete(tmpcontext);
		}
		else
		{
			MemoryContextSwitchTo(oldcontext);
			MemoryContextDelete(tmpcontext);
		}
	}
}

/*
 * Build up a QueryPlan to execute on.
 *
 * This functions tries to find out whether
 * 1. The statement can be shipped to the Datanode and Coordinator is needed
 *    only as a proxy - in which case, it creates a single node plan.
 * 2. The statement can be evaluated on the Coordinator completely - thus no
 *    query shipping is involved and standard_planner() is invoked to plan the
 *    statement
 * 3. The statement needs Coordinator as well as Datanode for evaluation -
 *    again we use standard_planner() to plan the statement.
 *
 * The plan generated in either of the above cases is returned.
 */
PlannedStmt *
pgxc_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result;

	/* handle the un-supported statements, obvious errors etc. */
	pgxc_handle_unsupported_stmts(query);

	result = pgxc_handle_exec_direct(query, cursorOptions, boundParams);
	if (result)
		return result;

	/* see if can ship the query completely */
	result = pgxc_FQS_planner(query, cursorOptions, boundParams);
	if (result)
		return result;

	/* we need Coordinator for evaluation, invoke standard planner */
	result = standard_planner(query, cursorOptions, boundParams);
	return result;
}

static PlannedStmt *
pgxc_handle_exec_direct(Query *query, int cursorOptions,
						ParamListInfo boundParams)
{
	PlannedStmt		*result = NULL;
	PlannerGlobal	*glob;
	PlannerInfo		*root;
	/*
	 * if the query has its utility set, it could be an EXEC_DIRECT statement,
	 * check if it needs to be executed on Coordinator
	 */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, RemoteQuery))
	{
		RemoteQuery *node = (RemoteQuery *)query->utilityStmt;
		/* EXECUTE DIRECT statements on remote nodes don't need Coordinator */
		if (node->exec_direct_type != EXEC_DIRECT_NONE &&
			node->exec_direct_type != EXEC_DIRECT_LOCAL &&
			node->exec_direct_type != EXEC_DIRECT_LOCAL_UTILITY)
		{
			glob = makeNode(PlannerGlobal);
			glob->boundParams = boundParams;
			/* Create a PlannerInfo data structure, usually it is done for a subquery */
			root = makeNode(PlannerInfo);
			root->parse = query;
			root->glob = glob;
			root->query_level = 1;
			root->planner_cxt = CurrentMemoryContext;
			/* build the PlannedStmt result */
			result = makeNode(PlannedStmt);
			/* Try and set what we can, rest must have been zeroed out by makeNode() */
			result->commandType = query->commandType;
			result->canSetTag = query->canSetTag;
			/* Set result relations */
			if (query->commandType != CMD_SELECT)
				result->resultRelations = list_make1_int(query->resultRelation);

			result->planTree = (Plan *)pgxc_FQS_create_remote_plan(query, NULL, true);
			result->rtable = query->rtable;

			/*
			 * Make a flattened version of the rangetable for faster access (this is
			 * OK because the rangetable won't change any more), and set up an empty
			 * array for indexing base relations.
			 */
			setup_simple_rel_arrays(root);

			/*
			 * We need to save plan dependencies, so that dropping objects will
			 * invalidate the cached plan if it depends on those objects. Table
			 * dependencies are available in glob->relationOids and all other
			 * dependencies are in glob->invalItems. These fields can be retrieved
			 * through set_plan_references().
			 */
			result->planTree = set_plan_references(root, result->planTree);
			result->relationOids = glob->relationOids;
			result->invalItems = glob->invalItems;
		}
	}

	return result;
}
/*
 * pgxc_handle_unsupported_stmts
 * Throw error for the statements that can not be handled in XC
 */
static void
pgxc_handle_unsupported_stmts(Query *query)
{
	ListCell *lc;

	/*
	 * PGXCTODO: This validation will not be removed
	 * until we support moving tuples from one node to another
	 * when the partition column of a table is updated
	 */
	if (query->commandType == CMD_UPDATE)
		validate_part_col_updatable(query);

	foreach(lc, query->cteList)
	{
		Query *wqry;
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);
		wqry = (Query *)cte->ctequery;
		if (wqry->commandType == CMD_UPDATE)
			validate_part_col_updatable(wqry);
	}
}

/*
 * pgxc_FQS_planner
 * The routine tries to see if the statement can be completely evaluated on the
 * Datanodes. In such cases Coordinator is not needed to evaluate the statement,
 * and just acts as a proxy. A statement can be completely shipped to the remote
 * node if every row of the result can be evaluated on a single Datanode.
 * For example:
 *
 * 1. SELECT * FROM tab1; where tab1 is a distributed table - Every row of the
 * result set can be evaluated at a single Datanode. Hence this statement is
 * completely shippable even though many Datanodes are involved in evaluating
 * complete result set. In such case Coordinator will be able to gather rows
 * arisign from individual Datanodes and proxy the result to the client.
 *
 * 2. SELECT count(*) FROM tab1; where tab1 is a distributed table - there is
 * only one row in the result but it needs input from all the Datanodes. Hence
 * this is not completely shippable.
 *
 * 3. SELECT count(*) FROM tab1; where tab1 is replicated table - since result
 * can be obtained from a single Datanode, this is a completely shippable
 * statement.
 *
 * fqs in the name of function is acronym for fast query shipping.
 */
static PlannedStmt *
pgxc_FQS_planner(Query *query, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt		*result;
	PlannerGlobal	*glob;
	PlannerInfo		*root;
	ExecNodes		*exec_nodes;
	Plan			*top_plan;

	/* Try by-passing standard planner, if fast query shipping is enabled */
	if (!enable_fast_query_shipping)
		return NULL;

	/* Cursor options may come from caller or from DECLARE CURSOR stmt */
	if (query->utilityStmt &&
		IsA(query->utilityStmt, DeclareCursorStmt))
		cursorOptions |= ((DeclareCursorStmt *) query->utilityStmt)->options;
	/*
	 * If the query can not be or need not be shipped to the Datanodes, don't
	 * create any plan here. standard_planner() will take care of it.
	 */
	exec_nodes = pgxc_is_query_shippable(query, 0);
	if (exec_nodes == NULL)
		return NULL;

	glob = makeNode(PlannerGlobal);
	glob->boundParams = boundParams;
	/* Create a PlannerInfo data structure, usually it is done for a subquery */
	root = makeNode(PlannerInfo);
	root->parse = query;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;

	/*
	 * We decided to ship the query to the Datanode/s, create a RemoteQuery node
	 * for the same.
	 */
	top_plan = (Plan *)pgxc_FQS_create_remote_plan(query, exec_nodes, false);
	/*
	 * If creating a plan for a scrollable cursor, make sure it can run
	 * backwards on demand.  Add a Material node at the top at need.
	 */
	if (cursorOptions & CURSOR_OPT_SCROLL)
	{
		if (!ExecSupportsBackwardScan(top_plan))
			top_plan = materialize_finished_plan(top_plan);
	}

	/*
	 * Make a flattened version of the rangetable for faster access (this is
	 * OK because the rangetable won't change any more), and set up an empty
	 * array for indexing base relations.
	 * setup_simple_rel_arrays() function does not check whether the array has
	 * been already setup or not. If it's already setup, there will be wastage
	 * of memory. We might call standard_planner() after calling
	 * pgxc_FQS_planner() in case the later fails.
	 * In such case, there might be a possibility that setup_simple_rel_arrays()
	 * gets called twice. Therefore we call this function only after we have
	 * decided that there will be FQS plan for given query.
	 */
	setup_simple_rel_arrays(root);

	/*
	 * Just before creating the PlannedStmt, do some final cleanup
	 * We need to save plan dependencies, so that dropping objects will
	 * invalidate the cached plan if it depends on those objects. Table
	 * dependencies are available in glob->relationOids and all other
	 * dependencies are in glob->invalItems. These fields can be retrieved
	 * through set_plan_references().
	 */
	top_plan = set_plan_references(root, top_plan);

	/* build the PlannedStmt result */
	result = makeNode(PlannedStmt);
	/* Try and set what we can, rest must have been zeroed out by makeNode() */
	result->commandType = query->commandType;
	result->canSetTag = query->canSetTag;
	result->utilityStmt = query->utilityStmt;

	/* Set result relations */
	if (query->commandType != CMD_SELECT)
		result->resultRelations = list_make1_int(query->resultRelation);
	result->planTree = top_plan;
	result->rtable = query->rtable;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;

	/*
	 * If query is DECLARE CURSOR fetch CTIDs and node names from the remote node
	 * Use CTID as a key to update/delete tuples on remote nodes when handling
	 * WHERE CURRENT OF.
	 */
	if (query->utilityStmt && IsA(query->utilityStmt, DeclareCursorStmt))
		fetch_ctid_of(result->planTree, query);

	return result;
}

static RemoteQuery *
pgxc_FQS_create_remote_plan(Query *query, ExecNodes *exec_nodes, bool is_exec_direct)
{
	RemoteQuery		*query_step;
	StringInfoData	buf;
	RangeTblEntry	*dummy_rte;
	List			*collected_rtable;

	/* EXECUTE DIRECT statements have their RemoteQuery node already built when analyzing */
	if (is_exec_direct)
	{
		Assert(IsA(query->utilityStmt, RemoteQuery));
		query_step = (RemoteQuery *)query->utilityStmt;
		query->utilityStmt = NULL;
	}
	else
	{
		query_step = makeNode(RemoteQuery);
		query_step->combine_type = COMBINE_TYPE_NONE;
		query_step->exec_type = EXEC_ON_DATANODES;
		query_step->exec_direct_type = EXEC_DIRECT_NONE;
		query_step->exec_nodes = exec_nodes;
	}

	Assert(query_step->exec_nodes);

	/* Deparse query tree to get step query. */
	if (query_step->sql_statement == NULL)
	{
		initStringInfo(&buf);
		/*
		 * We always finalise aggregates on datanodes for FQS.
		 * Use the expressions for ORDER BY or GROUP BY clauses.
		 */
		deparse_query(query, &buf, NIL, true, false);
		query_step->sql_statement = pstrdup(buf.data);
		pfree(buf.data);
	}

	/* Optimize multi-node handling */
	query_step->read_only = (query->commandType == CMD_SELECT && !query->hasForUpdate);
	query_step->has_row_marks = query->hasForUpdate;

	/* Check if temporary tables are in use in query */
	/* PGXC_FQS_TODO: scanning the rtable again for the queries should not be
	 * needed. We should be able to find out if the query has a temporary object
	 * while finding nodes for the objects. But there is no way we can convey
	 * that information here. Till such a connection is available, this is it.
	 */
	if (contains_temp_tables(query->rtable))
		query_step->is_temp = true;

	/*
	 * We need to evaluate some expressions like the ExecNodes->en_expr at
	 * Coordinator, prepare those for evaluation. Ideally we should call
	 * preprocess_expression, but it needs PlannerInfo structure for the same
	 */
	fix_opfuncids((Node *)(query_step->exec_nodes->en_expr));
	/*
	 * PGXCTODO
	 * When Postgres runs insert into t (a) values (1); against table
	 * defined as create table t (a int, b int); the plan is looking
	 * like insert into t (a,b) values (1,null);
	 * Later executor is verifying plan, to make sure table has not
	 * been altered since plan has been created and comparing table
	 * definition with plan target list and output error if they do
	 * not match.
	 * I could not find better way to generate targetList for pgxc plan
	 * then call standard planner and take targetList from the plan
	 * generated by Postgres.
	 */
	query_step->combine_type = get_plan_combine_type(
				query->commandType, query_step->exec_nodes->baselocatortype);
	/*
	 * Walk the query tree collecting the rtables from the subqueries. We need
	 * this rtable to construct the rtable to be examined for permissions at
	 * the time of executing this plan. Collect the RTEs before we append the
	 * dummy RTE for RemoteQuery plan being built.
	 */
	 collected_rtable = pgxc_collect_RTE(query);
	/*
	 * Create a dummy RTE for the remote query being created. Append the dummy
	 * range table entry to the range table. Note that this modifies the master
	 * copy the caller passed us, otherwise e.g EXPLAIN VERBOSE will fail to
	 * find the rte the Vars built below refer to. Also create the tuple
	 * descriptor for the result of this query from the base_tlist (targetlist
	 * we used to generate the remote node query).
	 */
	dummy_rte = makeNode(RangeTblEntry);
	dummy_rte->rtekind = RTE_REMOTE_DUMMY;
	/* Use a dummy relname... */
	if (is_exec_direct)
		dummy_rte->relname = "__EXECUTE_DIRECT__";
	else
		dummy_rte->relname	   = "__REMOTE_FQS_QUERY__";
	dummy_rte->eref		   = makeAlias("__REMOTE_FQS_QUERY__", NIL);
	/* Rest will be zeroed out in makeNode() */

	query->rtable = lappend(query->rtable, dummy_rte);
	query_step->scan.scanrelid 	= list_length(query->rtable);
	query_step->scan.plan.targetlist = query->targetList;
	query_step->base_tlist = query->targetList;

	/*
	 * Append the range table entries collected from the sub-query-trees to the
	 * rtable of passed in Query so that it can be added to the PlannerStmt
	 * later. It's safe to change the rtable of query for
	 * 1. Any Var in the Query is going to refer only the original range table
	 * entries
	 * 2. We have decided to create a plan, so this Query won't be used again
	 * for planning.
	 * Do this after we append the dummy RTE for RemoteQuery plan just created,
	 * so that it remains attached to the passed in Query's range table. The
	 * walker just appends the range tables without copying it.
	 */
	query->rtable = list_concat(query->rtable, collected_rtable);

	/* Finally save a handle to this Query structure */
	query_step->remote_query = copyObject(query);

	return query_step;
}


/*
 * validate whether partition column of a table is being updated
 */
static void
validate_part_col_updatable(const Query *query)
{
	RangeTblEntry *rte;
	RelationLocInfo *rel_loc_info;
	ListCell *lc;

	/* Make sure there is one table at least */
	if (query->rtable == NULL)
		return;

	rte = (RangeTblEntry *) list_nth(query->rtable, query->resultRelation - 1);


	if (rte != NULL && rte->relkind != RELKIND_RELATION)
		/* Bad relation type */
		return;

	/* See if we have the partitioned case. */
	rel_loc_info = GetRelationLocInfo(rte->relid);

	/* Any column updation on local relations is fine */
	if (!rel_loc_info)
		return;

	/* Only relations distributed by value can be checked */
	if (IsRelationDistributedByValue(rel_loc_info))
	{
		/* It is a partitioned table, check partition column in targetList */
		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			/* Nothing to do for a junk entry */
			if (tle->resjunk)
				continue;

			/*
			 * The TargetEntry::resno is the same as the attribute number
			 * of the column being updated, if the attribute number of the
			 * column being updated and the attribute on which the table is
			 * distributed is same means this set clause entry is updating the
			 * distribution column of the target table.
			 */
			if (rel_loc_info->partAttrNum == tle->resno)
			{
				/*
				 * The TargetEntry::expr contains the RHS of the SET clause
				 * i.e. the expression that the target column should get
				 * updated to. If that expression is such that it is not
				 * changing the target column e.g. in case of a statement
				 * UPDATE tab set dist_col = dist_col;
				 * then this UPDATE should be allowed.
				 */
				if (IsA(tle->expr, Var))
				{
					Var *v = (Var *)tle->expr;
					if (v->varno == query->resultRelation &&
						v->varattno == tle->resno)
						return;
				}

				ereport(ERROR,
						(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
						(errmsg("Partition column can't be updated in current version"))));
			}
		}
	}
}

/*
 * AddRemoteQueryNode
 *
 * Add a Remote Query node to launch on Datanodes.
 * This can only be done for a query a Top Level to avoid
 * duplicated queries on Datanodes.
 */
List *
AddRemoteQueryNode(List *stmts, const char *queryString, RemoteQueryExecType remoteExecType, bool is_temp)
{
	List *result = stmts;

	/* If node is appplied on EXEC_ON_NONE, simply return the list unchanged */
	if (remoteExecType == EXEC_ON_NONE)
		return result;

	/* Only a remote Coordinator is allowed to send a query to backend nodes */
	if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
	{
		RemoteQuery *step = makeNode(RemoteQuery);
		step->combine_type = COMBINE_TYPE_SAME;
		step->sql_statement = (char *) queryString;
		step->exec_type = remoteExecType;
		step->is_temp = is_temp;
		result = lappend(result, step);
	}

	return result;
}

/*
 * pgxc_query_contains_temp_tables
 *
 * Check if there is any temporary object used in given list of queries.
 */
bool
pgxc_query_contains_temp_tables(List *queries)
{
	ListCell   *elt;

	foreach(elt, queries)
	{
		Query *query = (Query *) lfirst(elt);

		if (!query)
			continue;

		switch(query->commandType)
		{
			case CMD_SELECT:
			case CMD_UPDATE:
			case CMD_INSERT:
			case CMD_DELETE:
				if (contains_temp_tables(query->rtable))
					return true;
			default:
				break;
		}
	}

	return false;
}

/*
 * pgxc_query_contains_utility
 *
 * Check if there is any utility statement in given list of queries.
 */
bool
pgxc_query_contains_utility(List *queries)
{
	ListCell   *elt;

	foreach(elt, queries)
	{
		Query *query = (Query *) lfirst(elt);

		if (!query)
			continue;

		if (query->commandType == CMD_UTILITY)
			return true;
	}

	return false;
}


/*
 * create_remotesort_plan
 * Check if we can push down the ORDER BY clause to the datanode/s thereby
 * getting partially sorted results.
 * If this optimization is possible, the function changes the passed in Sort
 * plan and the underlying RemoteQuery plan.
 * Even if RemoteQuery has local quals to be applied, it doesn't matter,
 * since application of quals does not change the order in which the
 * filtered rows appear. Hence unlike remote grouping or limit planner, we don't
 * look at the RemoteQuery plan's local quals.
 */
Plan *
create_remotesort_plan(PlannerInfo *root, Plan *local_plan)
{
	Sort			*local_sort = NULL;
	RemoteQuery		*remote_scan = NULL;
	Plan			*temp_plan = NULL;
	Plan			*result_plan = NULL;
	Query			*remote_query = NULL;
	List			*rq_order_by = NULL;
	int				cntCol;
	ListCell		*lcell;
	Index			next_ressortgrpref;
	RangeTblEntry	*dummy_rte;

	/* If GUC does not specify this optimization, don't do it */
	if (!enable_remotesort)
		return local_plan;

	/* We can only handle Sort->RemoteQuery plan tree */
	if (local_plan && IsA(local_plan, Sort))
	{
		local_sort = (Sort *)local_plan;
		temp_plan = local_plan->lefttree;
	}

	if (temp_plan && IsA(temp_plan, RemoteQuery))
	{
		remote_scan = (RemoteQuery *) temp_plan;
		temp_plan = NULL;
	}

	if (!remote_scan || !local_sort)
		return local_plan;

	/* We expect the targetlists of the Sort and RemoteQuery plans to be same */
	if (!equal(local_sort->plan.targetlist, remote_scan->scan.plan.targetlist))
		return local_plan;

	remote_query = remote_scan->remote_query;
	/*
	 * If there is already sortClause set for the query to be sent to the
	 * datanode/s, we can't over-write it. No push down possible.
	 */
	if (!remote_query || remote_query->sortClause)
		return local_plan;

	/*
	 * Scan the targetlist of the query to be shipped to the datanode to find the
	 * maximum ressortgroupref, in case we need to create new one. By default
	 * it's the first one.
	 */
	next_ressortgrpref = 1;
	foreach (lcell, remote_query->targetList)
	{
		TargetEntry *tle = lfirst(lcell);
		/*
		 * If there is a valid ressortgroupref in TLE and it's greater than
		 * next ressortgrpref intended to be used, update the later one.
		 */
		if (tle->ressortgroupref > 0 &&
			next_ressortgrpref <= tle->ressortgroupref)
			next_ressortgrpref = tle->ressortgroupref + 1;
	}
	/*
	 * If all of the sort clauses are shippable, we can push down ORDER BY
	 * clause. For every sort key, check if the corresponding expression is
	 * being shipped to the datanode. The expressions being shipped to the
	 * datanode can be found in RemoteQuery::base_tlist. Also make sure that the
	 * sortgroupref numbers are correct.
	 */
	for (cntCol = 0; cntCol < local_sort->numCols; cntCol++)
	{
		TargetEntry 	*remote_base_tle;/* TLE in the remote_scan's base_tlist */
		TargetEntry 	*remote_tle;	/* TLE in the remote_scan's targetlist */
		SortGroupClause	*orderby_entry;
		TargetEntry		*remote_query_tle; /* TLE in targetlist of the query to
											* be sent to the datanodes */

		remote_tle = get_tle_by_resno(remote_scan->scan.plan.targetlist,
											local_sort->sortColIdx[cntCol]);
		/* This should not happen, but safer to protect against bugs */
		if (!remote_tle)
		{
			rq_order_by = NULL;
			break;
		}

		remote_base_tle = tlist_member((Node *)remote_tle->expr,
										remote_scan->base_tlist);
		/*
		 * If we didn't find the sorting expression in base_tlist, can't ship
		 * ORDER BY.
		 */
		if (!remote_base_tle)
		{
			rq_order_by = NULL;
			break;
		}

		remote_query_tle = get_tle_by_resno(remote_query->targetList,
											remote_base_tle->resno);
		/*
		 * This should never happen, base_tlist should be exact replica of the
		 * targetlist to be sent to the Datanode except for varnos. But be
		 * safer. Also, if the targetlist entry to be sent to the Datanode is
		 * resjunk, while deparsing the query we are going to use the resno in
		 * ORDER BY clause, we can not ship the ORDER BY.
		 * PGXCTODO: in the case of resjunk, is it possible to just reset
		 * resjunk in the query's targetlist and base_tlist?
		 */
		if (!remote_query_tle ||
			remote_query_tle->ressortgroupref != remote_base_tle->ressortgroupref ||
			(remote_query_tle->resjunk))
		{
			rq_order_by = NULL;
			break;
		}

		/*
		 * The expression in the target entry is going to be used for sorting,
		 * so it should have a valid ressortref. If it doesn't have one, give it
		 * the one we calculated. Also set it in all the TLEs. In case the
		 * targetlists in RemoteQuery need to be adjusted, we will have
		 * the same ressortgroupref everywhere, which will get copied.
		 */
		if (remote_query_tle->ressortgroupref <= 0)
		{
			remote_query_tle->ressortgroupref = next_ressortgrpref;
			remote_tle->ressortgroupref = next_ressortgrpref;
			remote_base_tle->ressortgroupref = next_ressortgrpref;
			next_ressortgrpref = next_ressortgrpref + 1;
		}

		orderby_entry = makeNode(SortGroupClause);
		orderby_entry->tleSortGroupRef = remote_query_tle->ressortgroupref;
		orderby_entry->sortop = local_sort->sortOperators[cntCol];
		orderby_entry->nulls_first = local_sort->nullsFirst[cntCol];
		rq_order_by = lappend(rq_order_by, orderby_entry);
	}

	/*
	 * The sorting expressions are not found in the remote targetlist, ORDER BY
	 * can not be pushed down.
	 */
	if (!rq_order_by)
		return local_plan;
	remote_query->sortClause = rq_order_by;
	/*
	 * If there is only a single node involved in execution of RemoteQuery, we
	 * don't need the covering Sort plan. No sorting required at the
	 * coordinator.
	 */
	if (list_length(remote_scan->exec_nodes->nodeList) == 1)
		result_plan = (Plan *)remote_scan;
	else
	{
		local_sort->srt_start_merge = true;
		result_plan = (Plan *)local_sort;
	}

	/* Use index into targetlist for ORDER BY clause instead of expressions */
	remote_scan->rq_sortgroup_colno = true;
	/* We changed the Query to be sent to datanode/s. Build the statement */
	pgxc_rqplan_build_statement(remote_scan);
	/* Change the dummy RTE added to show the new place of reduction */
	dummy_rte = rt_fetch(remote_scan->scan.scanrelid, root->parse->rtable);
	dummy_rte->relname = "__REMOTE_SORT_QUERY__";
	dummy_rte->eref = makeAlias("__REMOTE_SORT_QUERY__", NIL);

	/* The plan to return should be from the passed in tree */
	Assert(result_plan == (Plan *)remote_scan ||
			result_plan == (Plan *)local_sort);

	return result_plan;
}

/*
 * create_remotelimit_plan
 * Check if we can incorporate the LIMIT clause into the RemoteQuery node if the
 * node under the Limit node is a RemoteQuery node. If yes then do so.
 * If there is only one Datanode involved in the execution of RemoteQuery, we
 * don't need the covering Limit node, both limitcount and limitoffset can be
 * pushed to the RemoteQuery node.
 * If there are multiple Datanodes involved in the execution of RemoteQuery, we
 * have following rules
 * 1. If there is only limitcount, push that into RemoteQuery node
 * 2. If there are both limitcount and limitoffset, push limitcount +
 * limitoffset into RemoteQuery node.
 *
 * If either of limitcount or limitoffset expressions are unshippable, we can
 * not push either of them to the RemoteQuery node.
 * If there is no possibility of pushing clauses, the input plan is returned
 * unmodified, otherwise, the RemoteQuery node under Limit is modified.
 */
Plan *
create_remotelimit_plan(PlannerInfo *root, Plan *local_plan)
{
	Limit		*local_limit = NULL;
	RemoteQuery	*remote_scan = NULL;
	Plan		*result_plan = local_plan; /* By default we return the local_plan */
	Query 		*remote_query;
	Node		*rq_limitOffset = NULL;
	Node		*rq_limitCount = NULL;
	RangeTblEntry	*dummy_rte;
	Query		*query = root->parse;	/* Query being planned */
	Plan		*temp_plan = NULL;

	/* If GUC forbids this optimization, return */
	if (!enable_remotelimit)
		return local_plan;

	/* The function can handle only Limit as local plan */
	if (IsA(local_plan, Limit))
	{
		local_limit = (Limit *)local_plan;
		temp_plan = local_plan->lefttree;
	}

	/*
	 * If the underlying plan is not RemoteQuery, there are other operations
	 * being done locally on Coordinator. LIMIT or OFFSET clauses can be applied
	 * only after these operations have been performed. Hence can not push LIMIT
	 * or OFFSET to Datanodes.
	 */
	if (temp_plan && IsA(temp_plan, RemoteQuery))
	{
		remote_scan = (RemoteQuery *)temp_plan;
		temp_plan = NULL;
	}

	/* If we didn't find the desired plan tree, return local plan as is */
	if (!local_limit || !remote_scan)
		return local_plan;

	/*
	 * If we have some local quals to be applied in RemoteQuery node, we can't
	 * push the LIMIT and OFFSET clauses, since application of quals would
	 * reduce the number of rows that can be projected further.
	 */
	if (remote_scan->scan.plan.qual)
		return local_plan;

	/*
	 * While checking shippability for LIMIT and OFFSET clauses, we don't expect
	 * any aggregates in there
	 */
	if (!pgxc_is_expr_shippable((Expr *)local_limit->limitOffset, NULL) ||
		!pgxc_is_expr_shippable((Expr *)local_limit->limitCount, NULL))
			return local_plan;

	/* Calculate the LIMIT and OFFSET values to be sent to the Datanodes */
	if (remote_scan->exec_nodes &&
		list_length(remote_scan->exec_nodes->nodeList) == 1)
	{
		/*
		 * If there is only a single node involved in execution of the RemoteQuery
		 * node, we don't need covering Limit node on Coordinator LIMIT and OFFSET
		 * clauses can be pushed to the Datanode. Copy the LIMIT and OFFSET
		 * expressions.
		 */
		rq_limitCount = copyObject(local_limit->limitCount);
		rq_limitOffset = copyObject(local_limit->limitOffset);
		/* we don't need the covering Limit node, return the RemoteQuery node */
		result_plan = (Plan *)remote_scan;
	}
	else if (local_limit->limitCount)
	{
		/*
		 * The underlying RemoteQuery node needs more than one Datanode for its
		 * execution. We need the covering local Limit plan for combining the
		 * results. If there is no LIMIT clause but only OFFSET clause, we don't
		 * push anything.
		 * Assumption: We do not know the number of rows available from each Datanode.
		 * Case 1: There is no OFFSET clause
		 * In worst case, we can have all qualifying rows coming from a single
		 * Datanode. Hence we have to set the same limit as specified by LIMIT
		 * clause for each Datanode.
		 * Case 2: There is OFFSET clause
		 * In worst case, there can be OFFSET + LIMIT number of rows evenly
		 * distributed across all the Datanodes. In such case, we need to fetch
		 * all rows from all the Datanodes. Hence we can not set any OFFSET in
		 * the query. The other extreme is all OFFSET + LIMIT rows are located
		 * only on a single node. Hence we need to set limit of the query to be
		 * sent to the Datanode to be LIMIT + OFFSET.
		 */
		if (!local_limit->limitOffset)
			rq_limitCount = copyObject(local_limit->limitCount);
		else
		{
			/* Generate an expression LIMIT + OFFSET, use dummy parse state */
			/*
			 * PGXCTODO: do we need to use pg_catalog.+ as the operator name
			 * here?
			 */
			rq_limitCount = (Node *)make_op(make_parsestate(NULL),
									list_make1(makeString((char *)"+")),
									copyObject(local_limit->limitCount),
									copyObject(local_limit->limitOffset),
									-1);
		}
		rq_limitOffset = NULL;
		result_plan = local_plan;
	}
	else
	{
		/*
		 * Underlying RemoteQuery needs more than one Datanode, and does not
		 * have LIMIT clause. No optimization. Return the input plan as is.
		 */
		result_plan = local_plan;
		rq_limitCount = NULL;
		rq_limitOffset = NULL;
	}

	/*
	 * If we have valid OFFSET and LIMIT to be set for the RemoteQuery, then
	 * only modify the Query in RemoteQuery node.
	 */
	if (rq_limitOffset || rq_limitCount)
	{
		remote_query = remote_scan->remote_query;
		Assert(remote_query);
		/*
		 * None should have set the OFFSET and LIMIT clauses in the Query of
		 * RemoteQuery node
		 */
		Assert(!remote_query->limitOffset && !remote_query->limitCount);
		remote_query->limitOffset = rq_limitOffset;
		remote_query->limitCount = rq_limitCount;

		/* Change the dummy RTE added to show the new place of reduction */
		dummy_rte = rt_fetch(remote_scan->scan.scanrelid, query->rtable);
		dummy_rte->relname = "__REMOTE_LIMIT_QUERY__";
		dummy_rte->eref = makeAlias("__REMOTE_LIMIT_QUERY__", NIL);

		/*
		 * We must have modified the Query to be sent to the Datanode. Build the
		 * statement out of it.
		 */
		pgxc_rqplan_build_statement(remote_scan);
	}
	return result_plan;
}

/*
 * pgxc_collect_RTE
 * Collect range tables in all the Query sub-trees in given Query tree. The function
 * returns all range tables appended one after the other in the order in which
 * the subqueries are visited. The entries in range table of passed in Query
 * do not appear in the collectd result.
 * The walker doesn't create a copy of range tables of sub-trees, but appends
 * them one after the other as they are. Any change to range table entries or
 * the linkages would be reflected in the originating Query as well.
 */
static List *
pgxc_collect_RTE(Query *query)
{
	collect_RTE_context crte_context;
	crte_context.crte_rtable = NIL;
	query_tree_walker(query, pgxc_collect_RTE_walker, &crte_context, 0);
	return crte_context.crte_rtable;
}

static bool
pgxc_collect_RTE_walker(Node *node, collect_RTE_context *crte_context)
{
	if (!node)
		return false;

	if (IsA(node, Query))
	{
		Query *query = (Query *)node;

		/*
		 * create a copy of query's range table, so that it can be
		 * linked with other RTEs in the collector's context.
		 */
		crte_context->crte_rtable = list_concat(crte_context->crte_rtable,
												list_copy(query->rtable));
		return query_tree_walker(query, pgxc_collect_RTE_walker, crte_context,
									0);
	}

	return expression_tree_walker(node, pgxc_collect_RTE_walker, crte_context);
}
