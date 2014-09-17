/*-------------------------------------------------------------------------
 *
 * placeholder.c
 *	  PlaceHolderVar and PlaceHolderInfo manipulation routines
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/placeholder.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/pathnode.h"
#include "optimizer/placeholder.h"
#include "optimizer/planmain.h"
#include "optimizer/var.h"
#include "utils/lsyscache.h"

/* Local functions */
static void find_placeholders_recurse(PlannerInfo *root, Node *jtnode);
static void find_placeholders_in_expr(PlannerInfo *root, Node *expr);


/*
 * make_placeholder_expr
 *		Make a PlaceHolderVar for the given expression.
 *
 * phrels is the syntactic location (as a set of baserels) to attribute
 * to the expression.
 */
PlaceHolderVar *
make_placeholder_expr(PlannerInfo *root, Expr *expr, Relids phrels)
{
	PlaceHolderVar *phv = makeNode(PlaceHolderVar);

	phv->phexpr = expr;
	phv->phrels = phrels;
	phv->phid = ++(root->glob->lastPHId);
	phv->phlevelsup = 0;

	return phv;
}

/*
 * find_placeholder_info
 *		Fetch the PlaceHolderInfo for the given PHV
 *
 * If the PlaceHolderInfo doesn't exist yet, create it if create_new_ph is
 * true, else throw an error.
 *
 * This is separate from make_placeholder_expr because subquery pullup has
 * to make PlaceHolderVars for expressions that might not be used at all in
 * the upper query, or might not remain after const-expression simplification.
 * We build PlaceHolderInfos only for PHVs that are still present in the
 * simplified query passed to query_planner().
 *
 * Note: this should only be called after query_planner() has started.  Also,
 * create_new_ph must not be TRUE after deconstruct_jointree begins, because
 * make_outerjoininfo assumes that we already know about all placeholders.
 */
PlaceHolderInfo *
find_placeholder_info(PlannerInfo *root, PlaceHolderVar *phv,
					  bool create_new_ph)
{
	PlaceHolderInfo *phinfo;
	Relids		rels_used;
	ListCell   *lc;

	/* if this ever isn't true, we'd need to be able to look in parent lists */
	Assert(phv->phlevelsup == 0);

	foreach(lc, root->placeholder_list)
	{
		phinfo = (PlaceHolderInfo *) lfirst(lc);
		if (phinfo->phid == phv->phid)
			return phinfo;
	}

	/* Not found, so create it */
	if (!create_new_ph)
		elog(ERROR, "too late to create a new PlaceHolderInfo");

	phinfo = makeNode(PlaceHolderInfo);

	phinfo->phid = phv->phid;
	phinfo->ph_var = copyObject(phv);

	/*
	 * Any referenced rels that are outside the PHV's syntactic scope are
	 * LATERAL references, which should be included in ph_lateral but not in
	 * ph_eval_at.  If no referenced rels are within the syntactic scope,
	 * force evaluation at the syntactic location.
	 */
	rels_used = pull_varnos((Node *) phv->phexpr);
	phinfo->ph_lateral = bms_difference(rels_used, phv->phrels);
	if (bms_is_empty(phinfo->ph_lateral))
		phinfo->ph_lateral = NULL;		/* make it exactly NULL if empty */
	phinfo->ph_eval_at = bms_int_members(rels_used, phv->phrels);
	/* If no contained vars, force evaluation at syntactic location */
	if (bms_is_empty(phinfo->ph_eval_at))
	{
		phinfo->ph_eval_at = bms_copy(phv->phrels);
		Assert(!bms_is_empty(phinfo->ph_eval_at));
	}
	/* ph_eval_at may change later, see update_placeholder_eval_levels */
	phinfo->ph_needed = NULL;	/* initially it's unused */
	/* for the moment, estimate width using just the datatype info */
	phinfo->ph_width = get_typavgwidth(exprType((Node *) phv->phexpr),
									   exprTypmod((Node *) phv->phexpr));

	root->placeholder_list = lappend(root->placeholder_list, phinfo);

	/*
	 * The PHV's contained expression may contain other, lower-level PHVs.  We
	 * now know we need to get those into the PlaceHolderInfo list, too, so we
	 * may as well do that immediately.
	 */
	find_placeholders_in_expr(root, (Node *) phinfo->ph_var->phexpr);

	return phinfo;
}

/*
 * find_placeholders_in_jointree
 *		Search the jointree for PlaceHolderVars, and build PlaceHolderInfos
 *
 * We don't need to look at the targetlist because build_base_rel_tlists()
 * will already have made entries for any PHVs in the tlist.
 *
 * This is called before we begin deconstruct_jointree.  Once we begin
 * deconstruct_jointree, all active placeholders must be present in
 * root->placeholder_list, because make_outerjoininfo and
 * update_placeholder_eval_levels require this info to be available
 * while we crawl up the join tree.
 */
void
find_placeholders_in_jointree(PlannerInfo *root)
{
	/* We need do nothing if the query contains no PlaceHolderVars */
	if (root->glob->lastPHId != 0)
	{
		/* Start recursion at top of jointree */
		Assert(root->parse->jointree != NULL &&
			   IsA(root->parse->jointree, FromExpr));
		find_placeholders_recurse(root, (Node *) root->parse->jointree);
	}
}

/*
 * find_placeholders_recurse
 *	  One recursion level of find_placeholders_in_jointree.
 *
 * jtnode is the current jointree node to examine.
 */
static void
find_placeholders_recurse(PlannerInfo *root, Node *jtnode)
{
	if (jtnode == NULL)
		return;
	if (IsA(jtnode, RangeTblRef))
	{
		/* No quals to deal with here */
	}
	else if (IsA(jtnode, FromExpr))
	{
		FromExpr   *f = (FromExpr *) jtnode;
		ListCell   *l;

		/*
		 * First, recurse to handle child joins.
		 */
		foreach(l, f->fromlist)
		{
			find_placeholders_recurse(root, lfirst(l));
		}

		/*
		 * Now process the top-level quals.
		 */
		find_placeholders_in_expr(root, f->quals);
	}
	else if (IsA(jtnode, JoinExpr))
	{
		JoinExpr   *j = (JoinExpr *) jtnode;

		/*
		 * First, recurse to handle child joins.
		 */
		find_placeholders_recurse(root, j->larg);
		find_placeholders_recurse(root, j->rarg);

		/* Process the qual clauses */
		find_placeholders_in_expr(root, j->quals);
	}
	else
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(jtnode));
}

/*
 * find_placeholders_in_expr
 *		Find all PlaceHolderVars in the given expression, and create
 *		PlaceHolderInfo entries for them.
 */
static void
find_placeholders_in_expr(PlannerInfo *root, Node *expr)
{
	List	   *vars;
	ListCell   *vl;

	/*
	 * pull_var_clause does more than we need here, but it'll do and it's
	 * convenient to use.
	 */
	vars = pull_var_clause(expr,
						   PVC_RECURSE_AGGREGATES,
						   PVC_INCLUDE_PLACEHOLDERS);
	foreach(vl, vars)
	{
		PlaceHolderVar *phv = (PlaceHolderVar *) lfirst(vl);

		/* Ignore any plain Vars */
		if (!IsA(phv, PlaceHolderVar))
			continue;

		/* Create a PlaceHolderInfo entry if there's not one already */
		(void) find_placeholder_info(root, phv, true);
	}
	list_free(vars);
}

/*
 * update_placeholder_eval_levels
 *		Adjust the target evaluation levels for placeholders
 *
 * The initial eval_at level set by find_placeholder_info was the set of
 * rels used in the placeholder's expression (or the whole subselect below
 * the placeholder's syntactic location, if the expr is variable-free).
 * If the query contains any outer joins that can null any of those rels,
 * we must delay evaluation to above those joins.
 *
 * We repeat this operation each time we add another outer join to
 * root->join_info_list.  It's somewhat annoying to have to do that, but
 * since we don't have very much information on the placeholders' locations,
 * it's hard to avoid.  Each placeholder's eval_at level must be correct
 * by the time it starts to figure in outer-join delay decisions for higher
 * outer joins.
 *
 * In future we might want to put additional policy/heuristics here to
 * try to determine an optimal evaluation level.  The current rules will
 * result in evaluation at the lowest possible level.  However, pushing a
 * placeholder eval up the tree is likely to further constrain evaluation
 * order for outer joins, so it could easily be counterproductive; and we
 * don't have enough information at this point to make an intelligent choice.
 */
void
update_placeholder_eval_levels(PlannerInfo *root, SpecialJoinInfo *new_sjinfo)
{
	ListCell   *lc1;

	foreach(lc1, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc1);
		Relids		syn_level = phinfo->ph_var->phrels;
		Relids		eval_at;
		bool		found_some;
		ListCell   *lc2;

		/*
		 * We don't need to do any work on this placeholder unless the
		 * newly-added outer join is syntactically beneath its location.
		 */
		if (!bms_is_subset(new_sjinfo->syn_lefthand, syn_level) ||
			!bms_is_subset(new_sjinfo->syn_righthand, syn_level))
			continue;

		/*
		 * Check for delays due to lower outer joins.  This is the same logic
		 * as in check_outerjoin_delay in initsplan.c, except that we don't
		 * have anything to do with the delay_upper_joins flags; delay of
		 * upper outer joins will be handled later, based on the eval_at
		 * values we compute now.
		 */
		eval_at = phinfo->ph_eval_at;

		do
		{
			found_some = false;
			foreach(lc2, root->join_info_list)
			{
				SpecialJoinInfo *sjinfo = (SpecialJoinInfo *) lfirst(lc2);

				/* disregard joins not within the PHV's sub-select */
				if (!bms_is_subset(sjinfo->syn_lefthand, syn_level) ||
					!bms_is_subset(sjinfo->syn_righthand, syn_level))
					continue;

				/* do we reference any nullable rels of this OJ? */
				if (bms_overlap(eval_at, sjinfo->min_righthand) ||
					(sjinfo->jointype == JOIN_FULL &&
					 bms_overlap(eval_at, sjinfo->min_lefthand)))
				{
					/* yes; have we included all its rels in eval_at? */
					if (!bms_is_subset(sjinfo->min_lefthand, eval_at) ||
						!bms_is_subset(sjinfo->min_righthand, eval_at))
					{
						/* no, so add them in */
						eval_at = bms_add_members(eval_at,
												  sjinfo->min_lefthand);
						eval_at = bms_add_members(eval_at,
												  sjinfo->min_righthand);
						/* we'll need another iteration */
						found_some = true;
					}
				}
			}
		} while (found_some);

		/* Can't move the PHV's eval_at level to above its syntactic level */
		Assert(bms_is_subset(eval_at, syn_level));

		phinfo->ph_eval_at = eval_at;
	}
}

/*
 * fix_placeholder_input_needed_levels
 *		Adjust the "needed at" levels for placeholder inputs
 *
 * This is called after we've finished determining the eval_at levels for
 * all placeholders.  We need to make sure that all vars and placeholders
 * needed to evaluate each placeholder will be available at the scan or join
 * level where the evaluation will be done.  (It might seem that scan-level
 * evaluations aren't interesting, but that's not so: a LATERAL reference
 * within a placeholder's expression needs to cause the referenced var or
 * placeholder to be marked as needed in the scan where it's evaluated.)
 * Note that this loop can have side-effects on the ph_needed sets of other
 * PlaceHolderInfos; that's okay because we don't examine ph_needed here, so
 * there are no ordering issues to worry about.
 */
void
fix_placeholder_input_needed_levels(PlannerInfo *root)
{
	ListCell   *lc;

	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);
		List	   *vars = pull_var_clause((Node *) phinfo->ph_var->phexpr,
										   PVC_RECURSE_AGGREGATES,
										   PVC_INCLUDE_PLACEHOLDERS);

		add_vars_to_targetlist(root, vars, phinfo->ph_eval_at, false);
		list_free(vars);
	}
}

/*
 * add_placeholders_to_base_rels
 *		Add any required PlaceHolderVars to base rels' targetlists, and
 *		update lateral_vars lists for lateral references contained in them.
 *
 * If any placeholder can be computed at a base rel and is needed above it,
 * add it to that rel's targetlist, and add any lateral references it requires
 * to the rel's lateral_vars list.  This might look like it could be merged
 * with fix_placeholder_input_needed_levels, but it must be separate because
 * join removal happens in between, and can change the ph_eval_at sets.  There
 * is essentially the same logic in add_placeholders_to_joinrel, but we can't
 * do that part until joinrels are formed.
 */
void
add_placeholders_to_base_rels(PlannerInfo *root)
{
	ListCell   *lc;

	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);
		Relids		eval_at = phinfo->ph_eval_at;

		if (bms_membership(eval_at) == BMS_SINGLETON)
		{
			int			varno = bms_singleton_member(eval_at);
			RelOptInfo *rel = find_base_rel(root, varno);

			/* add it to reltargetlist if needed above the rel scan level */
			if (bms_nonempty_difference(phinfo->ph_needed, eval_at))
				rel->reltargetlist = lappend(rel->reltargetlist,
											 copyObject(phinfo->ph_var));
			/* if there are lateral refs in it, add them to lateral_vars */
			if (phinfo->ph_lateral != NULL)
			{
				List	   *vars = pull_var_clause((Node *) phinfo->ph_var->phexpr,
												   PVC_RECURSE_AGGREGATES,
												   PVC_INCLUDE_PLACEHOLDERS);
				ListCell   *lc2;

				foreach(lc2, vars)
				{
					Node	   *node = (Node *) lfirst(lc2);

					if (IsA(node, Var))
					{
						Var		   *var = (Var *) node;

						if (var->varno != varno)
							rel->lateral_vars = lappend(rel->lateral_vars,
														var);
					}
					else if (IsA(node, PlaceHolderVar))
					{
						PlaceHolderVar *other_phv = (PlaceHolderVar *) node;
						PlaceHolderInfo *other_phi;

						other_phi = find_placeholder_info(root, other_phv,
														  false);
						if (!bms_is_subset(other_phi->ph_eval_at, eval_at))
							rel->lateral_vars = lappend(rel->lateral_vars,
														other_phv);
					}
					else
						Assert(false);
				}

				list_free(vars);
			}
		}
	}
}

/*
 * add_placeholders_to_joinrel
 *		Add any required PlaceHolderVars to a join rel's targetlist.
 *
 * A join rel should emit a PlaceHolderVar if (a) the PHV is needed above
 * this join level and (b) the PHV can be computed at or below this level.
 * At this time we do not need to distinguish whether the PHV will be
 * computed here or copied up from below.
 */
void
add_placeholders_to_joinrel(PlannerInfo *root, RelOptInfo *joinrel)
{
	Relids		relids = joinrel->relids;
	ListCell   *lc;

	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = (PlaceHolderInfo *) lfirst(lc);

		/* Is it still needed above this joinrel? */
		if (bms_nonempty_difference(phinfo->ph_needed, relids))
		{
			/* Is it computable here? */
			if (bms_is_subset(phinfo->ph_eval_at, relids))
			{
				/* Yup, add it to the output */
				joinrel->reltargetlist = lappend(joinrel->reltargetlist,
												 phinfo->ph_var);
				joinrel->width += phinfo->ph_width;
			}
		}
	}
}
