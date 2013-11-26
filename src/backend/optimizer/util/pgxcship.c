/*-------------------------------------------------------------------------
 *
 * pgxcship.c
 *		Routines to evaluate expression shippability to remote nodes
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/optimizer/util/pgxcship.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits_fn.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#ifdef PGXC
#include "catalog/pg_trigger.h"
#endif
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/trigger.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "parser/parse_coerce.h"
#include "parser/parse_type.h"
#include "pgxc/pgxcnode.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/*
 * Shippability_context
 * This context structure is used by the Fast Query Shipping walker, to gather
 * information during analysing query for Fast Query Shipping.
 */
typedef struct
{
	bool		sc_for_expr;		/* if false, the we are checking shippability
									 * of the Query, otherwise, we are checking
									 * shippability of a stand-alone expression.
									 */
	Bitmapset	*sc_shippability;	/* The conditions for (un)shippability of the
									 * query.
									 */
	Query		*sc_query;			/* the query being analysed for FQS */
	int			sc_query_level;		/* level of the query */
	int			sc_max_varlevelsup;	/* maximum upper level referred to by any
									 * variable reference in the query. If this
									 * value is greater than 0, the query is not
									 * shippable, if shipped alone.
									 */
	ExecNodes	*sc_exec_nodes;		/* nodes where the query should be executed */
	ExecNodes	*sc_subquery_en;	/* ExecNodes produced by merging the ExecNodes
									 * for individual subqueries. This gets
									 * ultimately merged with sc_exec_nodes.
									 */
	bool		sc_groupby_has_distcol;	/* GROUP BY clause has distribution column */
} Shippability_context;

/*
 * ShippabilityStat
 * List of reasons why a query/expression is not shippable to remote nodes.
 */
typedef enum
{
	SS_UNSHIPPABLE_EXPR = 0,	/* it has unshippable expression */
	SS_NEED_SINGLENODE,			/* Has expressions which can be evaluated when
								 * there is only a single node involved.
								 * Athought aggregates too fit in this class, we
								 * have a separate status to report aggregates,
								 * see below.
								 */
	SS_NEEDS_COORD,				/* the query needs Coordinator */
	SS_VARLEVEL,				/* one of its subqueries has a VAR
								 * referencing an upper level query
								 * relation
								 */
	SS_NO_NODES,				/* no suitable nodes can be found to ship
								 * the query
								 */
	SS_UNSUPPORTED_EXPR,		/* it has expressions currently unsupported
								 * by FQS, but such expressions might be
								 * supported by FQS in future
								 */
	SS_HAS_AGG_EXPR,			/* it has aggregate expressions */
	SS_UNSHIPPABLE_TYPE,		/* the type of expression is unshippable */
	SS_UNSHIPPABLE_TRIGGER		/* the type of trigger is unshippable */
} ShippabilityStat;

/* Manipulation of shippability reason */
static bool pgxc_test_shippability_reason(Shippability_context *context,
										  ShippabilityStat reason);
static void pgxc_set_shippability_reason(Shippability_context *context,
										 ShippabilityStat reason);
static void pgxc_reset_shippability_reason(Shippability_context *context,
										   ShippabilityStat reason);

/* Evaluation of shippability */
static bool pgxc_shippability_walker(Node *node, Shippability_context *sc_context);
static void pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context);

/* Fast-query shipping (FQS) functions */
static ExecNodes *pgxc_FQS_get_relation_nodes(RangeTblEntry *rte,
											  Index varno,
											  Query *query);
static ExecNodes *pgxc_FQS_find_datanodes(Shippability_context *sc_context);
static bool pgxc_query_needs_coord(Query *query);
static bool pgxc_query_contains_only_pg_catalog(List *rtable);
static bool pgxc_is_var_distrib_column(Var *var, List *rtable);
static bool pgxc_distinct_has_distcol(Query *query, ExecNodes *exec_nodes);
static ExecNodes *pgxc_FQS_find_datanodes_recurse(Node *node,
											Shippability_context *sc_context);
static ExecNodes *pgxc_FQS_datanodes_for_rtr(Index varno,
											Shippability_context *sc_context);
static void pgxc_replace_dist_vars_subquery(Query *query, ExecNodes *exec_nodes,
												Index varno);
static bool pgxc_is_trigger_shippable(Trigger *trigger);

/*
 * Set the given reason in Shippability_context indicating why the query can not be
 * shipped directly to remote nodes.
 */
static void
pgxc_set_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	context->sc_shippability = bms_add_member(context->sc_shippability, reason);
}

/*
 * pgxc_reset_shippability_reason
 * Reset reason why the query cannot be shipped to remote nodes
 */
static void
pgxc_reset_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	context->sc_shippability = bms_del_member(context->sc_shippability, reason);
	return;
}


/*
 * See if a given reason is why the query can not be shipped directly
 * to the remote nodes.
 */
static bool
pgxc_test_shippability_reason(Shippability_context *context, ShippabilityStat reason)
{
	return bms_is_member(reason, context->sc_shippability);
}


/*
 * pgxc_set_exprtype_shippability
 * Set the expression type shippability. For now composite types
 * derived from view definitions are not shippable.
 */
static void
pgxc_set_exprtype_shippability(Oid exprtype, Shippability_context *sc_context)
{
	char	typerelkind;

	typerelkind = get_rel_relkind(typeidTypeRelid(exprtype));

	if (typerelkind == RELKIND_SEQUENCE ||
		typerelkind == RELKIND_VIEW		||
		typerelkind == RELKIND_FOREIGN_TABLE)
		pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_TYPE);
}

/*
 * pgxc_FQS_datanodes_for_rtr
 * For a given RangeTblRef find the datanodes where corresponding data is
 * located.
 */
static ExecNodes *
pgxc_FQS_datanodes_for_rtr(Index varno, Shippability_context *sc_context)
{
	Query			*query = sc_context->sc_query;
	RangeTblEntry	*rte = rt_fetch(varno, query->rtable);
	switch (rte->rtekind)
	{
		case RTE_RELATION:
		{
			/* For anything, other than a table, we can't find the datanodes */
			if (rte->relkind != RELKIND_RELATION)
				return NULL;
			/*
			 * In case of inheritance, child tables can have completely different
			 * Datanode distribution than parent. To handle inheritance we need
			 * to merge the Datanodes of the children table as well. The inheritance
			 * is resolved during planning, so we may not have the RTEs of the
			 * children here. Also, the exact method of merging Datanodes of the
			 * children is not known yet. So, when inheritance is requested, query
			 * can not be shipped.
			 * See prologue of has_subclass, we might miss on the optimization
			 * because has_subclass can return true even if there aren't any
			 * subclasses, but it's ok.
			 */
			if (rte->inh && has_subclass(rte->relid))
				return NULL;

			return pgxc_FQS_get_relation_nodes(rte, varno, query);
		}
		break;

		case RTE_SUBQUERY:
		{
			ExecNodes *exec_nodes;
			/*
			 * Subquery in RangeTbleEntry is not scanned while scanning the
			 * parent query, since we don't scan the parent query's rtable.
			 */
			exec_nodes = pgxc_is_query_shippable(rte->subquery,
											sc_context->sc_query_level + 1);
			/*
			 * If the query result is distributed by HASH or MODULO, we need to
			 * map the Vars on which its distributed to the columns in the
			 * result.
			 */
			if (exec_nodes && IsExecNodesDistributedByValue(exec_nodes))
				pgxc_replace_dist_vars_subquery(rte->subquery, exec_nodes, varno);
			return exec_nodes;
		}
		break;
		/* For any other type of RTE, we return NULL for now */
		case RTE_JOIN:
		case RTE_CTE:
		case RTE_FUNCTION:
		case RTE_VALUES:
		default:
			return NULL;
	}
}

/*
 * pgxc_FQS_find_datanodes_recurse
 * Recursively find whether the sub-tree of From Expr rooted under given node is
 * pushable and if yes where.
 */
static ExecNodes *
pgxc_FQS_find_datanodes_recurse(Node *node, Shippability_context *sc_context)
{
	Query	*query = sc_context->sc_query;

	if (!node)
		return NULL;

	switch(nodeTag(node))
	{
		case T_FromExpr:
		{
			FromExpr	*from_expr = (FromExpr *)node;
			ListCell	*lcell;
			bool		first;
			ExecNodes	*result_en;

			/*
			 * For INSERT commands, we won't have any entries in the from list.
			 * Get the datanodes using the resultRelation index.
			 */
			if (query->commandType != CMD_SELECT && !from_expr->fromlist)
				return pgxc_FQS_datanodes_for_rtr(query->resultRelation,
														sc_context);

			/*
			 * All the entries in the From list are considered to be INNER
			 * joined with the quals as the JOIN condition. Get the datanodes
			 * for the first entry in the From list. For every subsequent entry
			 * determine whether the join between the relation in that entry and
			 * the cumulative JOIN of previous entries can be pushed down to the
			 * datanodes and the corresponding set of datanodes where the join
			 * can be pushed down.
			 */
			first = true;
			result_en = NULL;
			foreach (lcell, from_expr->fromlist)
			{
				Node	*fromlist_entry = lfirst(lcell);
				ExecNodes	*tmp_en;
				ExecNodes *en = pgxc_FQS_find_datanodes_recurse(fromlist_entry,
																sc_context);
				/*
				 * If any entry in fromlist is not shippable, jointree is not
				 * shippable
				 */
				if (!en)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				/* FQS does't ship a DML with more than one relation involved */
				if (!first && query->commandType != CMD_SELECT)
				{
					FreeExecNodes(&result_en);
					return NULL;
				}

				if (first)
				{
					first = false;
					result_en = en;
					continue;
				}

				tmp_en = result_en;
				/*
				 * Check whether the JOIN is pushable to the datanodes and
				 * find the datanodes where the JOIN can be pushed to. In FQS
				 * the query is shippable if only all the expressions are
				 * shippable. Hence assume that the targetlists of the joining
				 * relations are shippable.
				 */
				result_en = pgxc_is_join_shippable(result_en, en, JOIN_INNER,
													false, false,
											from_expr->quals);
				FreeExecNodes(&tmp_en);
			}

			return result_en;
		}
			break;

		case T_RangeTblRef:
		{
			RangeTblRef *rtr = (RangeTblRef *)node;
			return pgxc_FQS_datanodes_for_rtr(rtr->rtindex, sc_context);
		}
			break;

		case T_JoinExpr:
		{
			JoinExpr *join_expr = (JoinExpr *)node;
			ExecNodes *len;
			ExecNodes *ren;
			ExecNodes *result_en;

			/* FQS does't ship a DML with more than one relation involved */
			if (query->commandType != CMD_SELECT)
				return NULL;

			len = pgxc_FQS_find_datanodes_recurse(join_expr->larg, sc_context);
			ren = pgxc_FQS_find_datanodes_recurse(join_expr->rarg, sc_context);
			/* If either side of JOIN is unshippable, JOIN is unshippable */
			if (!len || !ren)
			{
				FreeExecNodes(&len);
				FreeExecNodes(&ren);
				return NULL;
			}
			/*
			 * Check whether the JOIN is pushable or not, and find the datanodes
			 * where the JOIN can be pushed to. In FQS the query is shippable if
			 * only all the expressions are shippable. Hence assume that the
			 * targetlists of the joining relations are shippable.
			 */
			result_en = pgxc_is_join_shippable(ren, len, false, false,
												join_expr->jointype,
												join_expr->quals);
			FreeExecNodes(&len);
			FreeExecNodes(&ren);
			return result_en;
		}
			break;

		default:
			return NULL;
			break;
	}
	/* Keep compiler happy */
	return NULL;
}

/*
 * pgxc_FQS_find_datanodes
 * Find the list of nodes where to ship query.
 */
static ExecNodes *
pgxc_FQS_find_datanodes(Shippability_context *sc_context)
{
	ExecNodes	*exec_nodes;
	Query		*query = sc_context->sc_query;

	/*
	 * For SELECT, the datanodes required to execute the query is obtained from
	 * the join tree of the query
	 */
	exec_nodes = pgxc_FQS_find_datanodes_recurse((Node *)query->jointree,
													sc_context);
	/* If we found the datanodes to ship, use them */
	if (exec_nodes && exec_nodes->nodeList)
	{
		/*
		 * If this is the highest level query in the query tree and
		 * relations involved in the query are such that ultimate JOIN is
		 * replicated JOIN, choose only one of them.
		 * If we do this for lower level queries in query tree, we might loose
		 * chance because common nodes are left out.
		 */
		if (IsExecNodesReplicated(exec_nodes) &&
			(exec_nodes->accesstype == RELATION_ACCESS_READ_FOR_UPDATE ||
			              exec_nodes->accesstype == RELATION_ACCESS_READ) &&
			sc_context->sc_query_level == 0)

		{
			List *tmp_list = exec_nodes->nodeList;
			exec_nodes->nodeList = GetPreferredReplicationNode(exec_nodes->nodeList);
			list_free(tmp_list);
		}
		return exec_nodes;
	}
	/*
	 * If we found the expression which can decide which can be used to decide
	 * where to ship the query, use that
	 */
	else if (exec_nodes && exec_nodes->en_expr)
		return exec_nodes;
	/* No way to figure out datanodes to ship the query to */
	return NULL;
}


/*
 * pgxc_FQS_get_relation_nodes
 * Return ExecNodes structure so as to decide which node the query should
 * execute on. If it is possible to set the node list directly, set it.
 * Otherwise set the appropriate distribution column expression or relid in
 * ExecNodes structure.
 */
static ExecNodes *
pgxc_FQS_get_relation_nodes(RangeTblEntry *rte, Index varno, Query *query)
{
	CmdType command_type = query->commandType;
	bool for_update = query->rowMarks ? true : false;
	ExecNodes	*rel_exec_nodes;
	RelationAccessType rel_access = RELATION_ACCESS_READ;
	RelationLocInfo *rel_loc_info;

	Assert(rte == rt_fetch(varno, (query->rtable)));

	switch (command_type)
	{
		case CMD_SELECT:
			if (for_update)
				rel_access = RELATION_ACCESS_READ_FOR_UPDATE;
			else
				rel_access = RELATION_ACCESS_READ;
			break;

		case CMD_UPDATE:
		case CMD_DELETE:
			rel_access = RELATION_ACCESS_UPDATE;
			break;

		case CMD_INSERT:
			rel_access = RELATION_ACCESS_INSERT;
			break;

		default:
			/* should not happen, but */
			elog(ERROR, "Unrecognised command type %d", command_type);
			break;
	}

	rel_loc_info = GetRelationLocInfo(rte->relid);
	/* If we don't know about the distribution of relation, bail out */
	if (!rel_loc_info)
		return NULL;

	/*
	 * Find out the datanodes to execute this query on.
	 * PGXC_FQS_TODO: for now, we apply node reduction only when there is only
	 * one relation involved in the query. If there are multiple distributed
	 * tables in the query and we apply node reduction here, we may fail to ship
	 * the entire join. We should apply node reduction transitively.
	 */
	if (list_length(query->rtable) == 1)
		rel_exec_nodes = GetRelationNodesByQuals(rte->relid, varno,
												 query->jointree->quals, rel_access);
	else
		rel_exec_nodes = GetRelationNodes(rel_loc_info, (Datum) 0,
										  true, InvalidOid, rel_access);

	if (!rel_exec_nodes)
		return NULL;

	if (IsExecNodesDistributedByValue(rel_exec_nodes))
	{
		Var	*dist_var = pgxc_get_dist_var(varno, rte, query->targetList);
		rel_exec_nodes->en_dist_vars = list_make1(dist_var);
	}

	if (rel_access == RELATION_ACCESS_INSERT &&
			 IsRelationDistributedByValue(rel_loc_info))
	{
		ListCell *lc;
		TargetEntry *tle;
		/*
		 * If the INSERT is happening on a table distributed by value of a
		 * column, find out the
		 * expression for distribution column in the targetlist, and stick in
		 * in ExecNodes, and clear the nodelist. Execution will find
		 * out where to insert the row.
		 */
		/* It is a partitioned table, get value by looking in targetList */
		foreach(lc, query->targetList)
		{
			tle = (TargetEntry *) lfirst(lc);

			if (tle->resjunk)
				continue;
			if (strcmp(tle->resname, GetRelationDistribColumn(rel_loc_info)) == 0)
				break;
		}
		/* Not found, bail out */
		if (!lc)
			return NULL;

		Assert(tle);
		/* We found the TargetEntry for the partition column */
		list_free(rel_exec_nodes->primarynodelist);
		rel_exec_nodes->primarynodelist = NULL;
		list_free(rel_exec_nodes->nodeList);
		rel_exec_nodes->nodeList = NULL;
		rel_exec_nodes->en_expr = tle->expr;
		rel_exec_nodes->en_relid = rel_loc_info->relid;
	}
	return rel_exec_nodes;
}

bool
pgxc_query_has_distcolgrouping(Query *query, ExecNodes *exec_nodes)
{
	ListCell	*lcell;

	if (!exec_nodes)
		return false;

	foreach (lcell, query->groupClause)
	{
		SortGroupClause 	*sgc = lfirst(lcell);
		Node				*sgc_expr;
		if (!IsA(sgc, SortGroupClause))
			continue;
		sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var) &&
			pgxc_is_var_distrib_column((Var *)sgc_expr,
										exec_nodes->en_dist_vars))
			return true;
	}
	return false;
}

static bool
pgxc_distinct_has_distcol(Query *query, ExecNodes *exec_nodes)
{
	ListCell	*lcell;

	if (!exec_nodes)
		return false;

	foreach (lcell, query->distinctClause)
	{
		SortGroupClause 	*sgc = lfirst(lcell);
		Node				*sgc_expr;
		if (!IsA(sgc, SortGroupClause))
			continue;
		sgc_expr = get_sortgroupclause_expr(sgc, query->targetList);
		if (IsA(sgc_expr, Var) &&
			pgxc_is_var_distrib_column((Var *)sgc_expr,
										exec_nodes->en_dist_vars))
			return true;
	}
	return false;
}

/*
 * pgxc_shippability_walker
 * walks the query/expression tree routed at the node passed in, gathering
 * information which will help decide whether the query to which this node
 * belongs is shippable to the Datanodes.
 *
 * The function should try to walk the entire tree analysing each subquery for
 * shippability. If a subquery is shippable but not the whole query, we would be
 * able to create a RemoteQuery node for that subquery, shipping it to the
 * Datanode.
 *
 * Return value of this function is governed by the same rules as
 * expression_tree_walker(), see prologue of that function for details.
 */
static bool
pgxc_shippability_walker(Node *node, Shippability_context *sc_context)
{
	if (node == NULL)
		return false;

	/* Below is the list of nodes that can appear in a query, examine each
	 * kind of node and find out under what conditions query with this node can
	 * be shippable. For each node, update the context (add fields if
	 * necessary) so that decision whether to FQS the query or not can be made.
	 * Every node which has a result is checked to see if the result type of that
	 * expression is shippable.
	 */
	switch(nodeTag(node))
	{
		/* Constants are always shippable */
		case T_Const:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

			/*
			 * For placeholder nodes the shippability of the node, depends upon the
			 * expression which they refer to. It will be checked separately, when
			 * that expression is encountered.
			 */
		case T_CaseTestExpr:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

			/*
			 * record_in() function throws error, thus requesting a result in the
			 * form of anonymous record from datanode gets into error. Hence, if the
			 * top expression of a target entry is ROW(), it's not shippable.
			 */
		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *)node;
			if (tle->expr)
			{
				char typtype = get_typtype(exprType((Node *)tle->expr));
				if (!typtype || typtype == TYPTYPE_PSEUDO)
					pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			}
		}
		break;

		case T_SortGroupClause:
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			break;

		case T_CoerceViaIO:
		{
			CoerceViaIO		*cvio = (CoerceViaIO *)node;
			Oid				input_type = exprType((Node *)cvio->arg);
			Oid				output_type = cvio->resulttype;
			CoercionContext	cc;

			cc = cvio->coerceformat == COERCE_IMPLICIT_CAST ? COERCION_IMPLICIT :
				COERCION_EXPLICIT;
			/*
			 * Internally we use IO coercion for types which do not have casting
			 * defined for them e.g. cstring::date. If such casts are sent to
			 * the datanode, those won't be accepted. Hence such casts are
			 * unshippable. Since it will be shown as an explicit cast.
			 */
			if (!can_coerce_type(1, &input_type, &output_type, cc))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;
		/*
		 * Nodes, which are shippable if the tree rooted under these nodes is
		 * shippable
		 */
		case T_CoerceToDomainValue:
			/*
			 * PGXCTODO: mostly, CoerceToDomainValue node appears in DDLs,
			 * do we handle DDLs here?
			 */
		case T_FieldSelect:
		case T_NamedArgExpr:
		case T_RelabelType:
		case T_BoolExpr:
			/*
			 * PGXCTODO: we might need to take into account the kind of boolean
			 * operator we have in the quals and see if the corresponding
			 * function is immutable.
			 */
		case T_ArrayCoerceExpr:
		case T_ConvertRowtypeExpr:
		case T_CaseExpr:
		case T_ArrayExpr:
		case T_RowExpr:
		case T_CollateExpr:
		case T_CoalesceExpr:
		case T_XmlExpr:
		case T_NullTest:
		case T_BooleanTest:
		case T_CoerceToDomain:
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

		case T_List:
		case T_RangeTblRef:
			break;

		case T_ArrayRef:
			/*
			 * When multiple values of of an array are updated at once
			 * FQS planner cannot yet handle SQL representation correctly.
			 * So disable FQS in this case and let standard planner manage it.
			 */
		case T_FieldStore:
			/*
			 * PostgreSQL deparsing logic does not handle the FieldStore
			 * for more than one fields (see processIndirection()). So, let's
			 * handle it through standard planner, where whole row will be
			 * constructed.
			 */
		case T_SetToDefault:
			/*
			 * PGXCTODO: we should actually check whether the default value to
			 * be substituted is shippable to the Datanode. Some cases like
			 * nextval() of a sequence can not be shipped to the Datanode, hence
			 * for now default values can not be shipped to the Datanodes
			 */
			pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
			break;

		case T_Var:
		{
			Var	*var = (Var *)node;
			/*
			 * if a subquery references an upper level variable, that query is
			 * not shippable, if shipped alone.
			 */
			if (var->varlevelsup > sc_context->sc_max_varlevelsup)
				sc_context->sc_max_varlevelsup = var->varlevelsup;
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_Param:
		{
			Param *param = (Param *)node;
			/* PGXCTODO: Can we handle internally generated parameters? */
			if (param->paramkind != PARAM_EXTERN)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_CurrentOfExpr:
		{
			/*
			 * Ideally we should not see CurrentOf expression here, it
			 * should have been replaced by the CTID = ? expression. But
			 * still, no harm in shipping it as is.
			 */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_Aggref:
		{
			Aggref *aggref = (Aggref *)node;
			/*
			 * An aggregate is completely shippable to the Datanode, if the
			 * whole group resides on that Datanode. This will be clear when
			 * we see the GROUP BY clause.
			 * agglevelsup is minimum of variable's varlevelsup, so we will
			 * set the sc_max_varlevelsup when we reach the appropriate
			 * VARs in the tree.
			 */
			pgxc_set_shippability_reason(sc_context, SS_HAS_AGG_EXPR);
			/*
			 * If a stand-alone expression to be shipped, is an
			 * 1. aggregate with ORDER BY, DISTINCT directives, it needs all
			 * the qualifying rows
			 * 2. aggregate without collection function
			 * 3. (PGXCTODO:)aggregate with polymorphic transition type, the
			 *    the transition type needs to be resolved to correctly interpret
			 *    the transition results from Datanodes.
			 * Hence, such an expression can not be shipped to the datanodes.
			 */
			if (aggref->aggorder ||
				aggref->aggdistinct ||
				aggref->agglevelsup ||
				!aggref->agghas_collectfn ||
				IsPolymorphicType(aggref->aggtrantype))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_FuncExpr:
		{
			FuncExpr	*funcexpr = (FuncExpr *)node;
			/*
			 * PGXC_FQS_TODO: it's too restrictive not to ship non-immutable
			 * functions to the Datanode. We need a better way to see what
			 * can be shipped to the Datanode and what can not be.
			 */
			if (!pgxc_is_func_shippable(funcexpr->funcid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			/*
			 * If this is a stand alone expression and the function returns a
			 * set of rows, we need to handle it along with the final result of
			 * other expressions. So, it can not be shippable.
			 */
			if (funcexpr->funcretset && sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
		case T_NullIfExpr:		/* struct-equivalent to OpExpr */
		{
			/*
			 * All of these three are structurally equivalent to OpExpr, so
			 * cast the node to OpExpr and check if the operator function is
			 * immutable. See PGXC_FQS_TODO item for FuncExpr.
			 */
			OpExpr *op_expr = (OpExpr *)node;
			Oid		opfuncid = OidIsValid(op_expr->opfuncid) ?
				op_expr->opfuncid : get_opcode(op_expr->opno);
			if (!OidIsValid(opfuncid) ||
				!pgxc_is_func_shippable(opfuncid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_ScalarArrayOpExpr:
		{
			/*
			 * Check if the operator function is shippable to the Datanode
			 * PGXC_FQS_TODO: see immutability note for FuncExpr above
			 */
			ScalarArrayOpExpr *sao_expr = (ScalarArrayOpExpr *)node;
			Oid		opfuncid = OidIsValid(sao_expr->opfuncid) ?
				sao_expr->opfuncid : get_opcode(sao_expr->opno);
			if (!OidIsValid(opfuncid) ||
				!pgxc_is_func_shippable(opfuncid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_RowCompareExpr:
		case T_MinMaxExpr:
		{
			/*
			 * PGXCTODO should we be checking the comparision operator
			 * functions as well, as we did for OpExpr OR that check is
			 * unnecessary. Operator functions are always shippable?
			 * Otherwise this node should be treated similar to other
			 * "shell" nodes.
			 */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_Query:
		{
			Query *query = (Query *)node;

			/* PGXCTODO : If the query has a returning list, it is not shippable as of now */
			if (query->returningList)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/* A stand-alone expression containing Query is not shippable */
			if (sc_context->sc_for_expr)
			{
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
				break;
			}
			/*
			 * We are checking shippability of whole query, go ahead. The query
			 * in the context should be same as the query being checked
			 */
			Assert(query == sc_context->sc_query);

			/* CREATE TABLE AS is not supported in FQS */
			if (query->commandType == CMD_UTILITY &&
				IsA(query->utilityStmt, CreateTableAsStmt))
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			if (query->hasRecursive)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			/*
			 * If the query needs Coordinator for evaluation or the query can be
			 * completed on Coordinator itself, we don't ship it to the Datanode
			 */
			if (pgxc_query_needs_coord(query))
				pgxc_set_shippability_reason(sc_context, SS_NEEDS_COORD);

			/* PGXCTODO: It should be possible to look at the Query and find out
			 * whether it can be completely evaluated on the Datanode just like SELECT
			 * queries. But we need to be careful while finding out the Datanodes to
			 * execute the query on, esp. for the result relations. If one happens to
			 * remove/change this restriction, make sure you change
			 * pgxc_FQS_get_relation_nodes appropriately.
			 * For now DMLs with single rtable entry are candidates for FQS
			 */
			if (query->commandType != CMD_SELECT && list_length(query->rtable) > 1)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * In following conditions query is shippable when there is only one
			 * Datanode involved
			 * 1. the query has aggregagtes without grouping by distribution
			 *    column
			 * 2. the query has window functions
			 * 3. the query has ORDER BY clause
			 * 4. the query has Distinct clause without distribution column in
			 *    distinct clause
			 * 5. the query has limit and offset clause
			 */
			if (query->hasWindowFuncs || query->sortClause ||
				query->limitOffset || query->limitCount)
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * walk the entire query tree to analyse the query. We will walk the
			 * range table, when examining the FROM clause. No need to do it
			 * here
			 */
			if (query_tree_walker(query, pgxc_shippability_walker,
									sc_context, QTW_IGNORE_RANGE_TABLE ))
				return true;

			/*
			 * PGXC_FQS_TODO:
			 * There is a subquery in this query, which references Vars in the upper
			 * query. For now stop shipping such queries. We should get rid of this
			 * condition.
			 */
			if (sc_context->sc_max_varlevelsup != 0)
				pgxc_set_shippability_reason(sc_context, SS_VARLEVEL);

			/*
			 * Check shippability of triggers on this query. Don't consider
			 * TRUNCATE triggers; it's a utility statement and triggers are
			 * handled explicitly in ExecuteTruncate()
			 */
			if (query->commandType == CMD_UPDATE ||
				query->commandType == CMD_INSERT ||
				query->commandType == CMD_DELETE)
			{
				RangeTblEntry *rte = (RangeTblEntry *)
					list_nth(query->rtable, query->resultRelation - 1);

				if (!pgxc_check_triggers_shippability(rte->relid,
													  query->commandType))
					pgxc_set_shippability_reason(sc_context,
												 SS_UNSHIPPABLE_TRIGGER);

				/*
				 * PGXCTODO: For the time being Postgres-XC does not support
				 * global constraints, but once it does it will be necessary
				 * to add here evaluation of the shippability of indexes and
				 * constraints of the relation used for INSERT/UPDATE/DELETE.
				 */
			}

			/*
			 * Walk the join tree of the query and find the
			 * Datanodes needed for evaluating this query
			 */
			sc_context->sc_exec_nodes = pgxc_FQS_find_datanodes(sc_context);

			/*
			 * Presence of aggregates or having clause, implies grouping. In
			 * such cases, the query won't be shippable unless 1. there is only
			 * a single node involved 2. GROUP BY clause has distribution column
			 * in it. In the later case aggregates for a given group are entirely
			 * computable on a single datanode, because all the rows
			 * participating in particular group reside on that datanode.
			 * The distribution column can be of any relation
			 * participating in the query. All the rows of that relation with
			 * the same value of distribution column reside on same node.
			 */
			if ((query->hasAggs || query->havingQual) &&
				sc_context->sc_exec_nodes &&
				!pgxc_query_has_distcolgrouping(query, sc_context->sc_exec_nodes))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * If distribution column of any relation is present in the distinct
			 * clause, values for that column across nodes will differ, thus two
			 * nodes won't be able to produce same result row. Hence in such
			 * case, we can execute the queries on many nodes managing to have
			 * distinct result.
			 */
			if (query->distinctClause &&
				sc_context->sc_exec_nodes &&
				!pgxc_distinct_has_distcol(query, sc_context->sc_exec_nodes))
				pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

		}
		break;

		case T_FromExpr:
		{
			/* We don't expect FromExpr in a stand-alone expression */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * We will examine the jointree of query separately to determine the
			 * set of datanodes where to execute the query.
			 * If this is an INSERT query with quals, resulting from say
			 * conditional rule, we can not handle those in FQS, since there is
			 * not SQL representation for such quals.
			 */
			if (sc_context->sc_query->commandType == CMD_INSERT &&
				((FromExpr *)node)->quals)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

		}
		break;

		case T_WindowFunc:
		{
			WindowFunc *winf = (WindowFunc *)node;
			/*
			 * A window function can be evaluated on a Datanode if there is
			 * only one Datanode involved.
			 */
			pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * A window function is not shippable as part of a stand-alone
			 * expression. If the window function is non-immutable, it can not
			 * be shipped to the datanodes.
			 */
			if (sc_context->sc_for_expr ||
				!pgxc_is_func_shippable(winf->winfnoid))
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);

			pgxc_set_exprtype_shippability(exprType(node), sc_context);
		}
		break;

		case T_WindowClause:
		{
			/*
			 * A window function can be evaluated on a Datanode if there is
			 * only one Datanode involved.
			 */
			pgxc_set_shippability_reason(sc_context, SS_NEED_SINGLENODE);

			/*
			 * A window function is not shippable as part of a stand-alone
			 * expression
			 */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSHIPPABLE_EXPR);
		}
		break;

		case T_JoinExpr:
			/* We don't expect JoinExpr in a stand-alone expression */
			if (sc_context->sc_for_expr)
				pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);

			/*
			 * The shippability of join will be deduced while
			 * examining the jointree of the query. Nothing to do here
			 */
			break;

		case T_SubLink:
		{
			/*
			 * We need to walk the tree in sublink to check for its
			 * shippability. We need to call pgxc_is_query_shippable() on Query
			 * instead of this function so that every subquery gets a different
			 * context for itself. We should avoid the default expression walker
			 * getting called on the subquery. At the same time we don't want to
			 * miss any other member (current or future) of this structure, from
			 * being scanned. So, copy the SubLink structure with subselect
			 * being NULL and call expression_tree_walker on the copied
			 * structure.
			 */
			SubLink		sublink = *(SubLink *)node;
			ExecNodes	*sublink_en;
			/*
			 * Walk the query and find the nodes where the query should be
			 * executed and node distribution. Merge this with the existing
			 * node list obtained for other subqueries. If merging fails, we
			 * can not ship the whole query.
			 */
			if (IsA(sublink.subselect, Query))
				sublink_en = pgxc_is_query_shippable((Query *)(sublink.subselect),
													 sc_context->sc_query_level);
			else
				sublink_en = NULL;

			/* PGXCTODO free the old sc_subquery_en. */
			/* If we already know that this query does not have a set of nodes
			 * to evaluate on, don't bother to merge again.
			 */
			if (!pgxc_test_shippability_reason(sc_context, SS_NO_NODES))
			{
				/*
				 * If this is the first time we are finding out the nodes for
				 * SubLink, we don't have anything to merge, just assign.
				 */
				if (!sc_context->sc_subquery_en)
					sc_context->sc_subquery_en = sublink_en;
				/*
				 * Merge if only the accumulated SubLink ExecNodes and the
				 * ExecNodes for this subquery are both replicated.
				 */
				else if (sublink_en && IsExecNodesReplicated(sublink_en) &&
							IsExecNodesReplicated(sc_context->sc_subquery_en))
				{
					sc_context->sc_subquery_en = pgxc_merge_exec_nodes(sublink_en,
																   sc_context->sc_subquery_en);
				}
				else
					sc_context->sc_subquery_en = NULL;

				/*
				 * If we didn't find a cumulative ExecNodes, set shippability
				 * reason, so that we don't bother merging future sublinks.
				 */
				if (!sc_context->sc_subquery_en)
					pgxc_set_shippability_reason(sc_context, SS_NO_NODES);
			}
			else
				Assert(!sc_context->sc_subquery_en);

			/* Check if the type of sublink result is shippable */
			pgxc_set_exprtype_shippability(exprType(node), sc_context);

			/* Wipe out subselect as explained above and walk the copied tree */
			sublink.subselect = NULL;
			return expression_tree_walker((Node *)&sublink, pgxc_shippability_walker,
											sc_context);
		}
		break;

		case T_SubPlan:
		case T_AlternativeSubPlan:
		case T_CommonTableExpr:
		case T_SetOperationStmt:
		case T_PlaceHolderVar:
		case T_AppendRelInfo:
		case T_PlaceHolderInfo:
		{
			/* PGXCTODO: till we exhaust this list */
			pgxc_set_shippability_reason(sc_context, SS_UNSUPPORTED_EXPR);
			/*
			 * These expressions are not supported for shippability entirely, so
			 * there is no need to walk trees underneath those. If we do so, we
			 * might walk the trees with wrong context there.
			 */
			return false;
		}
		break;

		default:
			elog(ERROR, "unrecognized node type: %d",
				 (int) nodeTag(node));
			break;
	}

	return expression_tree_walker(node, pgxc_shippability_walker, (void *)sc_context);
}


/*
 * pgxc_query_needs_coord
 * Check if the query needs Coordinator for evaluation or it can be completely
 * evaluated on Coordinator. Return true if so, otherwise return false.
 */
static bool
pgxc_query_needs_coord(Query *query)
{
	/*
	 * If the query is an EXEC DIRECT on the same Coordinator where it's fired,
	 * it should not be shipped
	 */
	if (query->is_local)
		return true;
	/*
	 * If the query involves just the catalog tables, and is not an EXEC DIRECT
	 * statement, it can be evaluated completely on the Coordinator. No need to
	 * involve Datanodes.
	 */
	if (pgxc_query_contains_only_pg_catalog(query->rtable))
		return true;

	return false;
}

/*
 * pgxc_is_var_distrib_column
 * If the passed in Var node is in the list of distribution columns passed in,
 * return true; otherwise return false.
 */
static bool
pgxc_is_var_distrib_column(Var *var, List *dist_vars)
{
	return list_member(dist_vars, var);
}

/*
 * Returns whether or not the rtable (and its subqueries)
 * only contain pg_catalog entries.
 */
static bool
pgxc_query_contains_only_pg_catalog(List *rtable)
{
	ListCell *item;

	/* May be complicated. Before giving up, just check for pg_catalog usage */
	foreach(item, rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(item);

		if (rte->rtekind == RTE_RELATION)
		{
			if (get_rel_namespace(rte->relid) != PG_CATALOG_NAMESPACE)
				return false;
		}
		else if (rte->rtekind == RTE_SUBQUERY &&
				 !pgxc_query_contains_only_pg_catalog(rte->subquery->rtable))
			return false;
	}
	return true;
}


/*
 * pgxc_is_query_shippable
 * This function calls the query walker to analyse the query to gather
 * information like  Constraints under which the query can be shippable, nodes
 * on which the query is going to be executed etc.
 * Based on the information gathered, it decides whether the query can be
 * executed on Datanodes directly without involving Coordinator.
 * If the query is shippable this routine also returns the nodes where the query
 * should be shipped. If the query is not shippable, it returns NULL.
 */
ExecNodes *
pgxc_is_query_shippable(Query *query, int query_level)
{
	Shippability_context sc_context;
	ExecNodes	*exec_nodes;
	bool		canShip = true;
	Bitmapset	*shippability;

	memset(&sc_context, 0, sizeof(sc_context));
	/* let's assume that by default query is shippable */
	sc_context.sc_query = query;
	sc_context.sc_query_level = query_level;
	sc_context.sc_for_expr = false;

	/*
	 * We might have already decided not to ship the query to the Datanodes, but
	 * still walk it anyway to find out if there are any subqueries which can be
	 * shipped.
	 */
	pgxc_shippability_walker((Node *)query, &sc_context);

	exec_nodes = sc_context.sc_exec_nodes;
	/*
	 * The shippability context contains two ExecNodes, one for the subLinks
	 * involved in the Query and other for the relation involved in FromClause.
	 * They are computed at different times while scanning the query. Merge both
	 * of them if they are both replicated. If query doesn't have SubLinks, we
	 * don't need to consider corresponding ExecNodes.
	 * PGXC_FQS_TODO:
	 * Merge the subquery ExecNodes if both of them are replicated.
	 * The logic to merge node lists with other distribution
	 * strategy is not clear yet.
	 */
	if (query->hasSubLinks)
	{
		if (exec_nodes && IsExecNodesReplicated(exec_nodes) &&
			sc_context.sc_subquery_en &&
			IsExecNodesReplicated(sc_context.sc_subquery_en))
			exec_nodes = pgxc_merge_exec_nodes(exec_nodes,
											   sc_context.sc_subquery_en);
		else
			exec_nodes = NULL;
	}

	/*
	 * Look at the information gathered by the walker in Shippability_context and that
	 * in the Query structure to decide whether we should ship this query
	 * directly to the Datanode or not
	 */

	/*
	 * If the planner was not able to find the Datanodes to the execute the
	 * query, the query is not completely shippable. So, return NULL
	 */
	if (!exec_nodes)
		return NULL;

	/* Copy the shippability reasons. We modify the copy for easier handling.
	 * The original can be saved away */
	shippability = bms_copy(sc_context.sc_shippability);

	/*
	 * If the query has an expression which renders the shippability to single
	 * node, and query needs to be shipped to more than one node, it can not be
	 * shipped.
	 */
	if (bms_is_member(SS_NEED_SINGLENODE, shippability))
	{
		/*
		 * if nodeList has no nodes, it ExecNodes will have other means to know
		 * the nodes where to execute like distribution column expression. We
		 * can't tell how many nodes the query will be executed on, hence treat
		 * that as multiple nodes.
		 * If query has result replicated across nodes, it's as good as having a
		 * single node.
		 */
		if (list_length(exec_nodes->nodeList) != 1 &&
			!IsExecNodesReplicated(exec_nodes))
			canShip = false;

		/* We handled the reason here, reset it */
		shippability = bms_del_member(shippability, SS_NEED_SINGLENODE);
	}

	/*
	 * If HAS_AGG_EXPR is set but NEED_SINGLENODE is not set, it means the
	 * aggregates are entirely shippable, so don't worry about it.
	 */
	shippability = bms_del_member(shippability, SS_HAS_AGG_EXPR);

	/* Can not ship the query for some reason */
	if (!bms_is_empty(shippability))
		canShip = false;

	/* Always keep this at the end before checking canShip and return */
	if (!canShip && exec_nodes)
		FreeExecNodes(&exec_nodes);
	/* If query is to be shipped, we should know where to execute the query */
	Assert (!canShip || exec_nodes);

	bms_free(shippability);
	shippability = NULL;

	return exec_nodes;
}


/*
 * pgxc_is_expr_shippable
 * Check whether the given expression can be shipped to datanodes.
 *
 * Note on has_aggs
 * The aggregate expressions are not shippable if they can not be completely
 * evaluated on a single datanode. But this function does not have enough
 * context to determine the set of datanodes where the expression will be
 * evaluated. Hence, the caller of this function can handle aggregate
 * expressions, it passes a non-NULL value for has_aggs. This function returns
 * whether the expression has any aggregates or not through this argument. If a
 * caller passes NULL value for has_aggs, this function assumes that the caller
 * can not handle the aggregates and deems the expression has unshippable.
 */
bool
pgxc_is_expr_shippable(Expr *node, bool *has_aggs)
{
	Shippability_context sc_context;

	/* Create the FQS context */
	memset(&sc_context, 0, sizeof(sc_context));
	sc_context.sc_query = NULL;
	sc_context.sc_query_level = 0;
	sc_context.sc_for_expr = true;

	/* Walk the expression to check its shippability */
	pgxc_shippability_walker((Node *)node, &sc_context);

	/*
	 * If caller is interested in knowing, whether the expression has aggregates
	 * let the caller know about it. The caller is capable of handling such
	 * expressions. Otherwise assume such an expression as not shippable.
	 */
	if (has_aggs)
		*has_aggs = pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);
	else if (pgxc_test_shippability_reason(&sc_context, SS_HAS_AGG_EXPR))
		return false;
	/* Done with aggregate expression shippability. Delete the status */
	pgxc_reset_shippability_reason(&sc_context, SS_HAS_AGG_EXPR);

	/* If there are reasons why the expression is unshippable, return false */
	if (!bms_is_empty(sc_context.sc_shippability))
		return false;

	/* If nothing wrong found, the expression is shippable */
	return true;
}


/*
 * pgxc_is_func_shippable
 * Determine if a function is shippable
 */
bool
pgxc_is_func_shippable(Oid funcid)
{
	/*
	 * For the time being a function is thought as shippable
	 * only if it is immutable.
	 */
	return func_volatile(funcid) == PROVOLATILE_IMMUTABLE;
}


/*
 * pgxc_find_dist_equijoin_qual
 * Check equijoin conditions on given relations
 */
Expr *
pgxc_find_dist_equijoin_qual(List *dist_vars1, List *dist_vars2, Node *quals)
{
	List		*lquals;
	ListCell	*qcell;

	/* If no quals, no equijoin */
	if (!quals)
		return false;

	if (!IsA(quals, List))
		lquals = make_ands_implicit((Expr *)quals);
	else
		lquals = (List *)quals;

	foreach(qcell, lquals)
	{
		Expr *qual_expr = (Expr *)lfirst(qcell);
		OpExpr *op;
		Var *lvar;
		Var *rvar;

		if (!IsA(qual_expr, OpExpr))
			continue;
		op = (OpExpr *)qual_expr;
		/* If not a binary operator, it can not be '='. */
		if (list_length(op->args) != 2)
			continue;

		/*
		 * Check if both operands are Vars, if not check next expression */
		if (IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var))
		{
			lvar = (Var *)linitial(op->args);
			rvar = (Var *)lsecond(op->args);
		}
		else
			continue;

		/*
		 * If the data types of both the columns are not same, continue. Hash
		 * and Modulo of a the same bytes will be same if the data types are
		 * same. So, only when the data types of the columns are same, we can
		 * ship a distributed JOIN to the Datanodes
		 */
		if (exprType((Node *)lvar) != exprType((Node *)rvar))
			continue;

		/* Do Vars in the equi-join represent distribution columns? */
		if (!((pgxc_is_var_distrib_column(lvar, dist_vars1) &&
				pgxc_is_var_distrib_column(rvar, dist_vars2)) ||
			 (pgxc_is_var_distrib_column(lvar, dist_vars2) &&
				pgxc_is_var_distrib_column(rvar, dist_vars1))))
			continue;

		/*
		 * If the operator is not an assignment operator, check next
		 * constraint. An operator is an assignment operator if it's
		 * mergejoinable or hashjoinable. Beware that not every assignment
		 * operator is mergejoinable or hashjoinable, so we might leave some
		 * oportunity. But then we have to rely on the opname which may not
		 * be something we know to be equality operator as well.
		 */
		if (!op_mergejoinable(op->opno, exprType((Node *)lvar)) &&
			!op_hashjoinable(op->opno, exprType((Node *)lvar)))
			continue;
		/* Found equi-join condition on distribution columns */
		return qual_expr;
	}
	return NULL;
}

/*
 * pgxc_merge_exec_nodes
 * The routine combines the two exec_nodes passed such that the resultant
 * exec_node corresponds to the JOIN of respective relations.
 * If both exec_nodes can not be merged, it returns NULL.
 */
ExecNodes *
pgxc_merge_exec_nodes(ExecNodes *en1, ExecNodes *en2)
{
	ExecNodes	*merged_en = makeNode(ExecNodes);
	ExecNodes	*tmp_en;

	/* If either of exec_nodes are NULL, return the copy of other one */
	if (!en1)
	{
		tmp_en = copyObject(en2);
		return tmp_en;
	}
	if (!en2)
	{
		tmp_en = copyObject(en1);
		return tmp_en;
	}

	/* Following cases are not handled in this routine */
	/* PGXC_FQS_TODO how should we handle table usage type? */
	if (en1->primarynodelist || en2->primarynodelist ||
		en1->en_expr || en2->en_expr ||
		OidIsValid(en1->en_relid) || OidIsValid(en2->en_relid) ||
		en1->accesstype != RELATION_ACCESS_READ || en2->accesstype != RELATION_ACCESS_READ)
		return NULL;

	if (IsExecNodesReplicated(en1) &&
		IsExecNodesReplicated(en2))
	{
		/*
		 * Replicated/replicated join case
		 * Check that replicated relation is not disjoint
		 * with initial relation which is also replicated.
		 * If there is a common portion of the node list between
		 * the two relations, other rtables have to be checked on
		 * this restricted list.
		 */
		merged_en->nodeList = list_intersection_int(en1->nodeList,
													en2->nodeList);
		merged_en->baselocatortype = LOCATOR_TYPE_REPLICATED;
		if (!merged_en->nodeList)
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	if (IsExecNodesReplicated(en1) &&
		IsExecNodesColumnDistributed(en2))
	{
		List	*diff_nodelist = NULL;
		/*
		 * Replicated/distributed join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en2->nodeList, en1->nodeList);
		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
		 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
		{
			merged_en->nodeList = list_copy(en2->nodeList);
			merged_en->baselocatortype = en2->baselocatortype;
			merged_en->en_dist_vars = en2->en_dist_vars;
		}
		return merged_en;
	}

	if (IsExecNodesColumnDistributed(en1) &&
		IsExecNodesReplicated(en2))
	{
		List *diff_nodelist = NULL;
		/*
		 * Distributed/replicated join case.
		 * Node list of distributed table has to be included
		 * in node list of replicated table.
		 */
		diff_nodelist = list_difference_int(en1->nodeList, en2->nodeList);

		/*
		 * If the difference list is not empty, this means that node list of
		 * distributed table is not completely mapped by node list of replicated
			 * table, so go through standard planner.
		 */
		if (diff_nodelist)
			FreeExecNodes(&merged_en);
		else
		{
			merged_en->nodeList = list_copy(en1->nodeList);
			merged_en->baselocatortype = en1->baselocatortype;
			merged_en->en_dist_vars = en1->en_dist_vars;
		}
		return merged_en;
	}

	if (IsExecNodesColumnDistributed(en1) &&
		IsExecNodesColumnDistributed(en2))
	{
		/*
		 * Distributed/distributed case
		 * If the caller has suggested that this is an equi-join between two
		 * distributed results, check that they have the same nodes in the distribution
		 * node list. The caller is expected to fully decide whether to merge
		 * the nodes or not.
		 */
		if (!list_difference_int(en1->nodeList, en2->nodeList) &&
			!list_difference_int(en2->nodeList, en1->nodeList))
		{
			merged_en->nodeList = list_copy(en1->nodeList);
			if (en1->baselocatortype == en2->baselocatortype)
			{
				merged_en->baselocatortype = en1->baselocatortype;
				merged_en->en_dist_vars = list_concat(list_copy(en1->en_dist_vars),
												list_copy(en2->en_dist_vars));
			}
			else
				merged_en->baselocatortype = LOCATOR_TYPE_DISTRIBUTED;
		}
		else
			FreeExecNodes(&merged_en);
		return merged_en;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("Postgres-XC does not support this distribution type yet"),
			 errdetail("The feature is not currently supported")));

	/* Keep compiler happy */
	return NULL;
}


/*
 * pgxc_check_index_shippability
 * Check shippability of index described by given conditions. This generic
 * function can be called even if the index is not yet defined.
 */
bool
pgxc_check_index_shippability(RelationLocInfo *relLocInfo,
							  bool is_primary,
							  bool is_unique,
							  bool is_exclusion,
							  List *indexAttrs,
							  List *indexExprs)
{
	bool		result = true;
	ListCell   *lc;

	/*
	 * Leave if no locator information, in this case shippability has no
	 * meaning.
	 */
	if (!relLocInfo)
		return result;

	/*
	 * Scan the expressions used in index and check the shippability of each
	 * of them. If only one is not-shippable, the index is considered as non
	 * shippable. It is important to check the shippability of the expressions
	 * before refining scan on the index columns and distribution type of
	 * parent relation.
	 */
	foreach(lc, indexExprs)
	{
		if (!pgxc_is_expr_shippable((Expr *) lfirst(lc), NULL))
		{
			/* One of the expressions is not shippable, so leave */
			result = false;
			goto finish;
		}
	}

	/*
	 * Check if relation is distributed on a single node, in this case
	 * the constraint can be shipped in all the cases.
	 */
	if (list_length(relLocInfo->nodeList) == 1)
		return result;

	/*
	 * Check the case of EXCLUSION index.
	 * EXCLUSION constraints are shippable only for replicated relations as
	 * such constraints need that one tuple is checked on all the others, and
	 * if this tuple is correctly excluded of the others, the constraint is
	 * verified.
	 */
	if (is_exclusion)
	{
		if (!IsRelationReplicated(relLocInfo))
		{
			result = false;
			goto finish;
		}
	}

	/*
	 * Check the case of PRIMARY KEY INDEX and UNIQUE index.
	 * Those constraints are shippable if the parent relation is replicated
	 * or if the column
	 */
	if (is_unique ||
		is_primary)
	{
		/*
		 * Perform different checks depending on distribution type of parent
		 * relation.
		 */
		switch(relLocInfo->locatorType)
		{
			case LOCATOR_TYPE_REPLICATED:
				/* In the replicated case this index is shippable */
				result = true;
				break;

			case LOCATOR_TYPE_RROBIN:
				/*
				 * Index on roundrobin parent table cannot be safely shipped
				 * because of the random behavior of data balancing.
				 */
				result = false;
				break;

			case LOCATOR_TYPE_HASH:
			case LOCATOR_TYPE_MODULO:
				/*
				 * Unique indexes on Hash and Modulo tables are shippable if the
				 * index expression contains all the distribution expressions of
				 * its parent relation.
				 *
				 * Here is a short example with concatenate that cannot be
				 * shipped:
				 * CREATE TABLE aa (a text, b text) DISTRIBUTE BY HASH(a);
				 * CREATE UNIQUE INDEX aap ON aa((a || b));
				 * INSERT INTO aa VALUES ('a', 'abb');
				 * INSERT INTO aa VALUES ('aab', b); -- no error ??!
				 * The output uniqueness is not guaranteed as both INSERT will
				 * go to different nodes. For such simple reasons unique
				 * indexes on distributed tables are not shippable.
				 * Shippability is not even ensured if all the expressions
				 * used as Var are only distributed columns as the hash output of
				 * their value combination does not ensure that query will
				 * be directed to the correct remote node. Uniqueness is not even
				 * protected if the index expression contains only the distribution
				 * column like for that with a cluster of 2 Datanodes:
				 * CREATE TABLE aa (a int) DISTRIBUTE BY HASH(a);
				 * CREATE UNIQUE INDEX aap ON (abs(a));
				 * INSERT INTO aa (2); -- to Datanode 1
				 * INSERT INTO aa (-2); -- to Datanode 2, breaks uniqueness
				 *
				 * PGXCTODO: for the time being distribution key can only be
				 * defined on a single column, so this will need to be changed
				 * onde a relation distribution will be able to be defined based
				 * on an expression of multiple columns.
				 */

				/* Index contains expressions, it cannot be shipped safely */
				if (indexExprs != NIL)
				{
					result = false;
					break;
				}

				/* Nothing to do if no attributes */
				if (indexAttrs == NIL)
					break;

				/*
				 * Check that distribution column is included in the list of
				 * index columns.
				 */
				if (!list_member_int(indexAttrs, relLocInfo->partAttrNum))
				{
					/*
					 * Distribution column is not in index column list
					 * So index can be enforced remotely.
					 */
					result = false;
					break;
				}

				/*
				 * by being here we are now sure that the index can be enforced
				 * remotely as the distribution column is included in index.
				 */
				break;

			/* Those types are not supported yet */
			case LOCATOR_TYPE_RANGE:
			case LOCATOR_TYPE_NONE:
			case LOCATOR_TYPE_DISTRIBUTED:
			case LOCATOR_TYPE_CUSTOM:
			default:
				/* Should not come here */
				Assert(0);
		}
	}

finish:
	return result;
}


/*
 * pgxc_check_fk_shippabilily
 * Check the shippability of a parent and a child relation based on the
 * distribution of each and the columns that are used to reference to
 * parent and child relation. This can be used for inheritance or foreign
 * key shippability evaluation.
 */
bool
pgxc_check_fk_shippability(RelationLocInfo *parentLocInfo,
						   RelationLocInfo *childLocInfo,
						   List *parentRefs,
						   List *childRefs)
{
	bool result = true;

	Assert(list_length(parentRefs) == list_length(childRefs));

	/*
	 * If either child or parent have no relation data, shippability makes
	 * no sense.
	 */
	if (!parentLocInfo || !childLocInfo)
		return result;

	/* In the case of a child referencing to itself, constraint is shippable */
	if (IsLocatorInfoEqual(parentLocInfo, childLocInfo))
		return result;

	/* Now begin the evaluation */
	switch (parentLocInfo->locatorType)
	{
		case LOCATOR_TYPE_REPLICATED:
			/*
			 * If the parent relation is replicated, the child relation can
			 * always refer to it on all the nodes.
			 */
			result = true;
			break;

		case LOCATOR_TYPE_RROBIN:
			/*
			 * If the parent relation is based on roundrobin, the child
			 * relation cannot be enforced on remote nodes before of the
			 * random behavior of data balancing.
			 */
			result = false;
			break;

		case LOCATOR_TYPE_HASH:
		case LOCATOR_TYPE_MODULO:
			/*
			 * If parent table is distributed, the child table can reference
			 * to its parent safely if the following conditions are satisfied:
			 * - parent and child are both hash-based, or both modulo-based
			 * - parent reference columns contain the distribution column
			 *   of the parent relation
			 * - child reference columns contain the distribution column
			 *   of the child relation
			 * - both child and parent map the same nodes for data location
			 */

			/* A replicated child cannot refer to a distributed parent */
			if (IsRelationReplicated(childLocInfo))
			{
				result = false;
				break;
			}

			/*
			 * Parent and child need to have the same distribution type:
			 * hash or modulo.
			 */
			if (parentLocInfo->locatorType != childLocInfo->locatorType)
			{
				result = false;
				break;
			}

			/*
			 * Parent and child need to have their data located exactly
			 * on the same list of nodes.
			 */
			if (list_difference_int(childLocInfo->nodeList, parentLocInfo->nodeList) ||
				list_difference_int(parentLocInfo->nodeList, childLocInfo->nodeList))
			{
				result = false;
				break;
			}

			/*
			 * Check that child and parents are referenced using their
			 * distribution column.
			 */
			if (!list_member_int(childRefs, childLocInfo->partAttrNum) ||
				!list_member_int(parentRefs, parentLocInfo->partAttrNum))
			{
				result = false;
				break;
			}

			/* By being here, parent-child constraint can be shipped correctly */
			break;

		case LOCATOR_TYPE_RANGE:
		case LOCATOR_TYPE_NONE:
		case LOCATOR_TYPE_DISTRIBUTED:
		case LOCATOR_TYPE_CUSTOM:
		default:
			/* Should not come here */
			Assert(0);
	}

	return result;
}

/*
 * pgxc_replace_dist_vars_subquery
 * The function looks up the members of ExecNodes::en_dist_var in the
 * query->targetList. If found, they are re-stamped with the given varno and
 * resno of the TargetEntry found and added to the new distribution var list
 * being created. This function is useful to re-stamp the distribution columns
 * of a subquery.
 */
static void
pgxc_replace_dist_vars_subquery(Query *query, ExecNodes *exec_nodes, Index varno)
{
	ListCell	*lcell;
	List		*new_dist_vars = NIL;

	foreach(lcell, exec_nodes->en_dist_vars)
	{
		Var *var = lfirst(lcell);
		TargetEntry *tle;

		Assert(IsA(var, Var));

		tle = tlist_member((Node *)var, query->targetList);
		if (tle)
		{
			Var *new_dist_var = makeVar(varno, tle->resno,
										exprType((Node *)tle->expr),
										exprTypmod((Node *)tle->expr),
										exprCollation((Node *)tle->expr), 0);
			new_dist_vars = lappend(new_dist_vars, new_dist_var);
		}
	}
	list_free(exec_nodes->en_dist_vars);
	exec_nodes->en_dist_vars = new_dist_vars;
}

/*
 * pgxc_get_dist_var
 * Given the varno, corresponding range table entry and targetlist, get the Var
 * node for distribution column, present in the targetlist as the root of
 * expression if there is one; otherwise return one created.
 *
 * If it's a replicated table or table local to the coordinator, or
 * any relation other than distributed table it returns NULL.
 *
 * varno: is the index of the range table entry in the query range table, to be
 *    		set as Var::varno
 * rte:   range table entry corresponding to varno. There is no way to verify
 * 			where the correspondence is true.
 * tlist: target list or just the list of expression where to find the Var
 * 			corresponding to the distribution column
 */
Var *
pgxc_get_dist_var(Index varno, RangeTblEntry *rte, List *tlist)
{
	RelationLocInfo *rel_loc_info = GetRelationLocInfo(rte->relid);
	ListCell		*lcell;
	Var				*dist_var;
	Oid				dist_var_type;
	int32			dist_var_typmod;
	Oid				dist_var_collid;

	if (!rel_loc_info || !IsRelationDistributedByValue(rel_loc_info))
		return NULL;

	/* find the TLE corresponding to the distribution column it. */
	foreach (lcell, tlist)
	{
		TargetEntry *tle = lfirst(lcell);
		Var *var;
		if (tle && IsA(tle, TargetEntry))
			var = (Var *)tle->expr;
		else
			var = (Var *)tle;

		if (var && IsA(var, Var) && (var->varno == varno) &&
			(var->varattno == rel_loc_info->partAttrNum))
			return copyObject(var);
	}

	/*
	 * Bare distribution column is not found in the targetlist, craft a Var for
	 * it and return.
	 */
	get_rte_attribute_type(rte, rel_loc_info->partAttrNum, &dist_var_type,
							&dist_var_typmod, &dist_var_collid);
	dist_var = makeVar(varno, rel_loc_info->partAttrNum, dist_var_type,
						dist_var_typmod, dist_var_collid, 0);
	return dist_var;
}

/*
 * pgxc_is_join_shippable
 * The shippability of JOIN is decided in following steps
 * 1. Are the JOIN conditions shippable?
 * 	For INNER JOIN it's possible to apply some of the conditions at the
 * 	Datanodes and others at coordinator. But for other JOINs, JOIN conditions
 * 	decide which tuples on the OUTER side are appended with NULL columns from
 * 	INNER side, we need all the join conditions to be shippable for the join to
 * 	be shippable.
 * 2. Do the JOIN conditions have quals that will make it shippable?
 * 	When both sides of JOIN are replicated, irrespective of the quals the JOIN
 * 	is shippable.
 * 	INNER joins between replicated and distributed relation are shippable
 * 	irrespective of the quals. OUTER join between replicated and distributed
 * 	relation is shippable if distributed relation is the outer relation.
 * 	All joins between hash/modulo distributed relations are shippable if they
 * 	have equi-join on the distributed column, such that distribution columns
 * 	have same datatype and same distribution strategy.
 * 3. Are datanodes where the joining relations exist, compatible?
 * 	Joins between replicated relations are shippable if both relations share a
 * 	datanode. Joins between distributed relations are shippable if both
 * 	relations are distributed on same set of Datanodes. Join between replicated
 * 	and distributed relations is shippable is replicated relation is replicated
 * 	on all nodes where distributed relation is distributed.
 * 4. Are targetlists of both sides shippable?
 *  For OUTER Joins if there is at least one unshippable entry in the targetlist
 *  of the relation which contributes NULL columns in the join result, the join
 *  is not shippable. In such cases, the unshippable expression is projected at
 *  the coordinator, thus causing a non-NULL value to appear instead of NULL
 *  value in the result.
 *
 * The first step is to be applied by the caller of this function.
 */
ExecNodes *
pgxc_is_join_shippable(ExecNodes *inner_en, ExecNodes *outer_en,
						bool inner_unshippable_tlist, bool outer_unshippable_tlist,
						JoinType jointype, Node *join_quals)
{
	bool	merge_nodes = false;

	/*
	 * If either of inner_en or outer_en is NULL, return NULL. We can't ship the
	 * join when either of the sides do not have datanodes to ship to.
	 */
	if (!outer_en || !inner_en)
		return NULL;
	/*
	 * We only support reduction of INNER, LEFT [OUTER] and FULL [OUTER] joins.
	 * RIGHT [OUTER] join is converted to LEFT [OUTER] join during join tree
	 * deconstruction.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT && jointype != JOIN_FULL)
		return NULL;

	/*
	 * For left outer join, if the inner relation (for which null columns are
	 * added if there is a row unmatched from outer join), has unshippable
	 * targetlist entry, we can not ship the join. This is because, the unshippable
	 * targetlist entry needs to be calculated before it can be added to the
	 * JOIN result, either as NULL or non-NULL.
	 * Similarly for FULL OUTER Join, none of the sides should have unshippable
	 * targetlist expression.
	 */
	if (jointype == JOIN_LEFT && inner_unshippable_tlist)
		return NULL;
	if (jointype == JOIN_FULL && (inner_unshippable_tlist ||
									outer_unshippable_tlist))
		return NULL;

	/* If both sides are replicated or have single node each, we ship any kind of JOIN */
	if ((IsExecNodesReplicated(inner_en) && IsExecNodesReplicated(outer_en)) ||
		 (list_length(inner_en->nodeList) == 1 &&
			list_length(outer_en->nodeList) == 1))
		merge_nodes = true;

	/* If both sides are distributed, ... */
	else if (IsExecNodesColumnDistributed(inner_en) &&
				IsExecNodesColumnDistributed(outer_en))
	{
		/*
		 * If two sides are distributed in the same manner by a value, with an
		 * equi-join on the distribution column and that condition
		 * is shippable, ship the join if node lists from both sides can be
		 * merged.
		 */
		if (inner_en->baselocatortype == outer_en->baselocatortype &&
			IsExecNodesDistributedByValue(inner_en))
		{
			Expr *equi_join_expr = pgxc_find_dist_equijoin_qual(inner_en->en_dist_vars,
																outer_en->en_dist_vars,
																join_quals);
			if (equi_join_expr && pgxc_is_expr_shippable(equi_join_expr, NULL))
				merge_nodes = true;
		}
	}
	/*
	 * If outer side is distributed and inner side is replicated, we can ship
	 * LEFT OUTER and INNER join.
	 */
	else if (IsExecNodesColumnDistributed(outer_en) &&
				IsExecNodesReplicated(inner_en) &&
				(jointype == JOIN_INNER || jointype == JOIN_LEFT))
			merge_nodes = true;
	/*
	 * If outer side is replicated and inner side is distributed, we can ship
	 * only for INNER join.
	 */
	else if (IsExecNodesReplicated(outer_en) &&
				IsExecNodesColumnDistributed(inner_en) &&
				jointype == JOIN_INNER)
		merge_nodes = true;
	/*
	 * If the ExecNodes of inner and outer nodes can be merged, the JOIN is
	 * shippable
	 */
	if (merge_nodes)
		return pgxc_merge_exec_nodes(inner_en, outer_en);
	else
		return NULL;
}


/*
 * pgxc_check_triggers_shippability:
 * Return true if none of the triggers prevents the query from being FQSed.
 */
bool
pgxc_check_triggers_shippability(Oid relid, int commandType)
{
	int16 trigevent = pgxc_get_trigevent(commandType);
	Relation	rel = relation_open(relid, AccessShareLock);
	bool		found_nonshippable;

	/*
	 * If we don't find a non-shippable row trigger, then the statement is
	 * shippable as far as triggers are concerned. For FQSed query, statement
	 * triggers are separately invoked on coordinator.
	 */
	found_nonshippable = pgxc_find_nonshippable_row_trig(rel, trigevent, 0, true);

	relation_close(rel, AccessShareLock);
	return !found_nonshippable;
}




/* pgxc_find_nonshippable_row_trig:
 * Search for a non-shippable ROW trigger of a particular type.
 *
 * If ignore_timing is true, just the event_type is used to find a match, so
 * once the event matches, the search returns true regardless of whether it is a
 * before or after row trigger.
 *
 * If ignore_timing is false, return true if we find one or more non-shippable
 * row triggers that match the exact combination of event and timing.
 *
 * We have to do this way because the bitmask used for timing does
 * not have unique bit positions for different values. For e.g. for AFTER timing
 * type, the bit position 0x2 has value 0, and for BEFORE type the same
 * bit position has value 1, so it is impossible to use these bits to suggest
 * ignoring the timing. (ROW and STATEMENT values also share the same 0x1 bit
 * but we only want ROW triggers so it does not matter here). Hence an extra
 * flag ignore_timing to indicate that we want to ignore the timing
 * and only consider event type. The caller may just pass 0 for timing.
 * NOTE: To indicate that timing is to be ignored, we can device our own
 * "invalid" timing value in which all of the timing bits are set to 1
 * (i.e. the exact TRIGGER_TYPE_TIMING_MASK value), but that will make the
 * function calls unreadable.
 */
bool
pgxc_find_nonshippable_row_trig(Relation rel, int16 tgtype_event,
							  int16 tgtype_timing, bool ignore_timing)
{
	TriggerDesc *trigdesc = rel->trigdesc;
	int			 i;

	/*
	 * This function is used for finding matching row triggers only; should not
	 * be called for TRUNCATE command.
	 */
	Assert(!TRIGGER_FOR_TRUNCATE(tgtype_event));

	/* Have triggers in the first place ? */
	if (!trigdesc)
		return false;

	/*
	 * Quick check by just scanning the trigger descriptor, before
	 * actually peeking into each of the individual triggers.
	 */
	if (!pgxc_has_trigger_for_event(tgtype_event, trigdesc))
		return false;

	for (i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger    *trigger = &trigdesc->triggers[i];
		int16		tgtype = trigger->tgtype;

		/* We are looking for row triggers only */
		if (!TRIGGER_FOR_ROW(tgtype))
			continue;

		/*
		 * If we are asked to find triggers of *any* level or timing, just match
		 * the event type to determine whether we should ignore this trigger.
		 */
		if (ignore_timing)
		{
			if ((TRIGGER_FOR_INSERT(tgtype_event) && !TRIGGER_FOR_INSERT(tgtype)) ||
				(TRIGGER_FOR_UPDATE(tgtype_event) && !TRIGGER_FOR_UPDATE(tgtype)) ||
				(TRIGGER_FOR_DELETE(tgtype_event) && !TRIGGER_FOR_DELETE(tgtype)))
				continue;
		}
		else
		{
			/*
			 * Otherwise, do an exact match with the given combination of event
			 * and timing.
			 */
			if (!TRIGGER_TYPE_MATCHES(tgtype, TRIGGER_TYPE_ROW,
								  tgtype_timing, tgtype_event))
			continue;
		}

		/*
		 * We now know that we cannot ignore this trigger, so check its
		 * shippability.
		 */
		if (!pgxc_is_trigger_shippable(trigger))
			return true;
	}

	return false;
}



/*
 * pgxc_is_trigger_shippable:
 * Check if trigger is shippable to a remote node. This function would be
 * called both on coordinator as well as datanode. We want this function
 * to be workable on datanode because we want to skip non-shippable triggers
 * on datanode.
 */
static bool
pgxc_is_trigger_shippable(Trigger *trigger)
{
	bool		res = true;

	/*
	 * If trigger is based on a constraint or is internal, enforce its launch
	 * whatever the node type where we are for the time being.
	 * PGXCTODO: we need to remove this condition once constraints are better
	 * implemented within Postgres-XC as a constraint can be locally
	 * evaluated on remote nodes depending on the distribution type of the table
	 * on which it is defined or on its parent/child distribution types.
	 */
	if (trigger->tgisinternal)
		return true;

	/*
	 * INSTEAD OF triggers can only be defined on views, which are defined
	 * only on Coordinators, so they cannot be shipped.
	 */
	if (TRIGGER_FOR_INSTEAD(trigger->tgtype))
		res = false;

	/* Finally check if function called is shippable */
	if (!pgxc_is_func_shippable(trigger->tgfoid))
		res = false;

	return res;
}

