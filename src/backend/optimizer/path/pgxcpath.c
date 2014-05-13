/*-------------------------------------------------------------------------
 *
 * rquerypath.c
 *	  Routines to find possible remote query paths for various relations and
 *	  their costs.
 *
 * Portions Copyright (c) 2012 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/path/rquerypath.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/pathnode.h"
#include "optimizer/pgxcship.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "pgxc/pgxc.h"
#include "optimizer/pgxcplan.h"
#include "tcop/tcopprot.h"

static RemoteQueryPath *pgxc_find_remotequery_path(RelOptInfo *rel);
static RemoteQueryPath *create_remotequery_path(PlannerInfo *root, RelOptInfo *rel,
								ExecNodes *exec_nodes, ParamPathInfo *param_info,
								RemoteQueryPath *leftpath,
								RemoteQueryPath *rightpath, JoinType jointype,
								List *join_restrictlist);
/*
 * create_remotequery_path
 *	  Creates a path for given RelOptInfo (for base rel or a join rel) so that
 *	  the results corresponding to this RelOptInfo are obtained by querying
 *	  datanode/s. When RelOptInfo represents a JOIN, we leftpath and rightpath
 *	  represents the RemoteQuery paths for left and right relations resp,
 *	  jointype gives the type of JOIN and join_restrictlist gives the
 *	  restrictinfo list for the JOIN. For a base relation, these should be
 *	  NULL.
 *	  ExecNodes is the set of datanodes to which the query should be sent to.
 *	  This function also marks the path with shippability of the quals.
 *	  If any of the relations involved in this path is a temporary relation,
 *	  record that fact.
 *	  Note about ParamPathInfo argument: This argument is non-NULL if the
 *	  current RemoteQuery path needs to be parameterized. Later while
 *	  crafting the plan node such parameters will be converted into nested loop
 *	  parameters. As of now (1st Nov. 2013) such parameters are not shippable.
 *	  Also, we do not create RemoteQuery path for a join involving child
 *	  RemoteQuery paths with unshippable targetlists or unshippable clauses.
 *	  Thus when we encounter paths with ParamPathInfo, we will not create
 *	  RemoteQuery paths above those paths. Which also means that we will not
 *	  create queries with LATERAL joins. Optimizer might change a LATERAL join
 *	  into non-LATERAL join by pulling clauses up (which is mostly the case).
 *	  Hence it is likely that a prima-facie LATERAL join would get shipped
 *	  because it was turned into non-LATERAL join.
 *	  PGXC_TODO: Find a way to ship the queries with LATERAL references (if they
 *	  survive the optimizer).
 */
static RemoteQueryPath *
create_remotequery_path(PlannerInfo *root, RelOptInfo *rel, ExecNodes *exec_nodes,
						ParamPathInfo *param_info,
						RemoteQueryPath *leftpath, RemoteQueryPath *rightpath,
						JoinType jointype, List *join_restrictlist)
{
	RemoteQueryPath	*rqpath = makeNode(RemoteQueryPath);
	bool			unshippable_quals = false;

	if (rel->reloptkind == RELOPT_JOINREL && (!leftpath || !rightpath))
		elog(ERROR, "a join rel requires both the left path and right path");

	rqpath->path.pathtype = T_RemoteQuery;
	rqpath->path.parent = rel;
	rqpath->path.param_info = param_info;
	rqpath->path.pathkeys = NIL;	/* result is always unordered */
	rqpath->rqpath_en = exec_nodes;
	rqpath->leftpath = leftpath;
	rqpath->rightpath = rightpath;
	rqpath->jointype = jointype;
	rqpath->join_restrictlist = join_restrictlist;

	switch (rel->reloptkind)
	{
		case RELOPT_BASEREL:
		case RELOPT_OTHER_MEMBER_REL:
		{
			RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
			if (rte->rtekind != RTE_RELATION)
				elog(ERROR, "can not create remote path for ranges of type %d",
							rte->rtekind);
			rqpath->rqhas_temp_rel = IsTempTable(rte->relid);
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(rel->baserestrictinfo, false),
														NULL);
		}
		break;

		case RELOPT_JOINREL:
		{
			rqpath->rqhas_temp_rel = leftpath->rqhas_temp_rel ||
									rightpath->rqhas_temp_rel;
			unshippable_quals = !pgxc_is_expr_shippable((Expr *)extract_actual_clauses(join_restrictlist, false),
														NULL);
		}
		break;

		default:
			elog(ERROR, "can not create remote path for relation of type %d",
							rel->reloptkind);
	}
	rqpath->rqhas_unshippable_qual = unshippable_quals;
	rqpath->rqhas_unshippable_tlist = !pgxc_is_expr_shippable((Expr *)rel->reltargetlist,
																NULL);

	/* PGXCTODO - set cost properly */
	cost_remotequery(rqpath, root, rel);

	return rqpath;
}

/*
 * create_plainrel_rqpath
 * Create a RemoteQuery path for a plain relation residing on datanode/s and add
 * it to the pathlist in corresponding RelOptInfo. The function returns true, if
 * it creates a remote query path and adds it, otherwise it returns false.
 * The caller can decide whether to add the scan paths depending upon the return
 * value.
 */
extern bool
create_plainrel_rqpath(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
						Relids required_outer)
{
	List			*quals;
	ExecNodes		*exec_nodes;
	ParamPathInfo	*param_info;

	/*
	 * If we are on the Coordinator, we always want to use
	 * the remote query path unless relation is local to coordinator or the
	 * query is to entirely executed on coordinator.
	 */
	if (!IS_PGXC_COORDINATOR || IsConnFromCoord() || root->parse->is_local)
		return false;

	quals = extract_actual_clauses(rel->baserestrictinfo, false);
	exec_nodes = GetRelationNodesByQuals(rte->relid, rel->relid,
														(Node *)quals,
														RELATION_ACCESS_READ);
	if (!exec_nodes)
		return false;

	if (IsExecNodesDistributedByValue(exec_nodes))
	{
		Var	*dist_var = pgxc_get_dist_var(rel->relid, rte, rel->reltargetlist);
		exec_nodes->en_dist_vars = list_make1(dist_var);
	}

	param_info = get_baserel_parampathinfo(root, rel, required_outer);

	/* We don't have subpaths for a plain base relation */
	add_path(rel, (Path *)create_remotequery_path(root, rel, exec_nodes, param_info,
													NULL, NULL, 0, NULL));
	return true;
}

/*
 * pgxc_find_remotequery_path
 * Search the path list for the rel for existence of a RemoteQuery path, return
 * if one found, NULL otherwise. There should be only one RemoteQuery path for
 * each rel, but we don't check for this.
 */
static RemoteQueryPath *
pgxc_find_remotequery_path(RelOptInfo *rel)
{
	ListCell *cell;

	foreach (cell, rel->pathlist)
	{
		Path *path = (Path *)lfirst(cell);
		if (IsA(path, RemoteQueryPath))
			return (RemoteQueryPath *)path;
	}
	return NULL;
}

/*
 * pgxc_ship_remotejoin
 * If there are RemoteQuery paths for the rels being joined, check if the join
 * is shippable to the datanodes, and if so, create a remotequery path for this
 * JOIN.
 */
extern void
create_joinrel_rqpath(PlannerInfo *root, RelOptInfo *joinrel,
						RelOptInfo *outerrel, RelOptInfo *innerrel,
						List *restrictlist, JoinType jointype,
						SpecialJoinInfo *sjinfo, Relids param_source_rels)
{
	RemoteQueryPath	*innerpath;
	RemoteQueryPath	*outerpath;
	ExecNodes		*inner_en;
	ExecNodes		*outer_en;
	ExecNodes		*join_en;
	List			*join_quals = NIL;
	List			*other_quals = NIL;
	ParamPathInfo	*param_info;
	Relids			required_outer;

	/* If GUC does not allow remote join optimization, so be it */
	if (!enable_remotejoin)
		return;

	innerpath = pgxc_find_remotequery_path(innerrel);
	outerpath = pgxc_find_remotequery_path(outerrel);
	/*
	 * If one of the relation does not have RemoteQuery path, the join can not
	 * be shipped to the datanodes.
	 * If one of the relation has an unshippable qual, it needs to be evaluated
	 * before joining the two relations. Hence this JOIN is not shippable.
	 * PGXC_TODO: In case of INNER join above condition can be relaxed by
	 * attaching the unshippable qual to the join itself, and thus shipping join
	 * but evaluating the qual on join result. But we don't attempt it for now
	 */
	if (!innerpath || !outerpath ||
		innerpath->rqhas_unshippable_qual || outerpath->rqhas_unshippable_qual)
		return;

	inner_en = innerpath->rqpath_en;
	outer_en = outerpath->rqpath_en;

	if (!inner_en || !outer_en)
		elog(ERROR, "No node list provided for remote query path");

	/*
	 * Check to see if proposed path is still parameterized, and reject if the
	 * parameterization wouldn't be sensible.
	 */
	required_outer = calc_non_nestloop_required_outer((Path *)outerpath,
													  (Path *)innerpath);
	if (required_outer &&
		!bms_overlap(required_outer, param_source_rels))
	{
		/* Waste no memory when we reject a path here */
		bms_free(required_outer);
		return;
	}
	/*
	 * Collect quals from restrictions so as to check the shippability of a JOIN
	 * between distributed relations.
	 */
	extract_actual_join_clauses(restrictlist, &join_quals, &other_quals);
	/*
	 * If the joining qual is not shippable and it's an OUTER JOIN, we can not
	 * ship the JOIN, since that would impact JOIN result.
	 */
	if (jointype != JOIN_INNER &&
		!pgxc_is_expr_shippable((Expr *)join_quals, NULL))
		return;
	/*
	 * For INNER JOIN there is no distinction between JOIN and non-JOIN clauses,
	 * so let the JOIN reduction algorithm take all of them into consideration
	 * to decide whether a JOIN is reducible or not based on quals (if
	 * required).
	 */
	if (jointype == JOIN_INNER)
		join_quals = list_concat(join_quals, other_quals);

	/*
	 * If the nodelists on both the sides of JOIN can be merged, the JOIN is
	 * shippable.
	 */
	join_en = pgxc_is_join_shippable(inner_en, outer_en,
										innerpath->rqhas_unshippable_tlist,
										outerpath->rqhas_unshippable_tlist,
										jointype, (Node *)join_quals);
	param_info = get_joinrel_parampathinfo(root, joinrel, (Path *)outerpath,
											(Path *)innerpath, sjinfo,
											required_outer, &restrictlist);
	if (join_en)
		add_path(joinrel, (Path *)create_remotequery_path(root, joinrel, join_en,
															param_info,
													outerpath, innerpath, jointype,
													restrictlist));
	return;
}
