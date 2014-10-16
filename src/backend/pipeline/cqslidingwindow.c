/*-------------------------------------------------------------------------
 *
 * cqslidingwindow.c
 *	  Support for analyzing sliding window queries
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cslidingwindow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqslidingwindow.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

typedef struct CQAnalyzeContext
{
	ParseState *pstate;
	List *cols;
	Node *matchExpr;
	int location;
} CQAnalyzeContext;

/*
 * find_sliding_window_expr
 *
 * Walk the parse tree of the `whereClause` in the SelectStmt of
 * a CQ and set context->matchExpr to be the minimal expression
 * that must match for a tuple to be considered in the sliding window.
 *
 * This function picks out the part of the where clause
 * that depends on clock_timestamp().
 *
 * XXX(usmanm): For now we only allow a clock_timestamp()
 * expression to exist in a conjunction at the top level of the
 * whereClause expression tree. This lets us avoid complicated
 * situations like sliding windows with holes etc. which turns
 * into a tricky combinatorial problem.
 */
static bool
find_sliding_window_expr(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *funccall = (FuncCall *) node;
		char *name = NameListToString(funccall->funcname);
		if (pg_strcasecmp(name, CLOCK_TIMESTAMP) == 0)
		{
			context->location = funccall->location;
			return true;
		}
	}
	else if (IsA(node, A_Expr))
	{
		A_Expr *a_expr = (A_Expr *) node;
		bool l_res, r_res;
		Node *l_expr, *r_expr;

		context->matchExpr = NULL;

		l_res = find_sliding_window_expr(a_expr->lexpr, context);
		if (context->matchExpr != NULL)
			l_expr = context->matchExpr;
		else
			l_expr = a_expr->lexpr;

		context->matchExpr = NULL;

		r_res = find_sliding_window_expr(a_expr->rexpr, context);
		if (context->matchExpr)
			r_expr = context->matchExpr;
		else
			r_expr = a_expr->rexpr;

		switch (a_expr->kind)
		{
		case AEXPR_OP:
			if (l_res || r_res)
			{
				context->matchExpr = (Node *) makeA_Expr(AEXPR_OP, a_expr->name, l_expr, r_expr, -1);
				return true;
			}
			return false;
		case AEXPR_AND:
			if (l_res && r_res)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("clock_timestamp may only appear once in a WHERE clause"),
								parser_errposition(context->pstate, context->location)));
			else if (l_res)
				context->matchExpr = l_expr;
			else if (r_res)
				context->matchExpr = r_expr;
			return (l_res || r_res);
		case AEXPR_NOT:
			if (r_res)
				context->matchExpr = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, r_expr, -1);
			return r_res;
		case AEXPR_OR:
		case AEXPR_OP_ANY:
		case AEXPR_OP_ALL:
		case AEXPR_DISTINCT:
		case AEXPR_NULLIF:
		case AEXPR_OF:
		case AEXPR_IN:
		default:
			if (l_res || r_res)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("clock_timestamp may only appear as a top-level predicate"),
								parser_errposition(context->pstate, context->location)));
			break;
		}
	}

	return raw_expression_tree_walker(node, find_sliding_window_expr, (void *) context);
}

/*
 * has_clock_timestamp
 */
static bool
has_clock_timestamp(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *funccall = (FuncCall *) node;
		char *name = NameListToString(funccall->funcname);
		if (pg_strcasecmp(name, CLOCK_TIMESTAMP) == 0)
		{
			return true;
		}
	}

	return raw_expression_tree_walker(node, has_clock_timestamp, context);
}

/*
 * find_col_refs
 *
 * Recursively search this node and find all ColRefs in it.
 */
static bool
find_col_refs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
		{
			context->cols = lappend(context->cols, node);
			return false;
		}

	}
	else if (IsA(node, ColumnRef))
	{
		context->cols = lappend(context->cols, node);
	}

	return raw_expression_tree_walker(node, find_col_refs, (void *) context);
}

/*
 * does_colref_match_res_target
 *
 * Does this ColRef match the ResTarget? This is used
 * so that TypeCasted ColRefs are also considered matches.
 */
static bool
does_colref_match_res_target(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL &&
				strcmp(res->name, FigureColname((Node *) cref)) == 0)
			return true;
	}

	if (IsA(node, ColumnRef))
	{
		ColumnRef *test_cref = (ColumnRef *) node;
		ListCell *lc1;
		ListCell *lc2;
		bool match = true;

		if (list_length(test_cref->fields) != list_length(cref->fields))
			return false;

		forboth(lc1, test_cref->fields, lc2, cref->fields)
		{
			if (strcmp(strVal(lfirst(lc1)), strVal(lfirst(lc2))) != 0)
			{
				match = false;
				break;
			}
		}

		return match;
	}

	/*
	 * Even if a FuncCall has cref as an argument, its value
	 * can be different from the column, so we need to project
	 * the column.
	 */
	if (IsA(node, FuncCall))
		return false;

	return raw_expression_tree_walker(node, does_colref_match_res_target, (void *) cref);
}

/*
 * GetSlidingWindowMatchExpr
 *
 * Return the squashed whereClause that is used by the
 * CQ VIEW or the GC to disqualify tuples which don't fall
 * in the sliding window.
 */
Node *
GetSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate)
{
	CQAnalyzeContext context;
	context.pstate = pstate;
	context.matchExpr = NULL;

	if (stmt->whereClause == NULL)
	{
		return NULL;
	}

	/* find the subset of the whereClause that we must match valid tuples */
	find_sliding_window_expr(stmt->whereClause, &context);

	return context.matchExpr;
}

/*
 * ValidateSlidingWindowExpr
 */
void
ValidateSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate)
{
	GetSlidingWindowExpr(stmt, pstate);
}

/*
 * IsSlidingWindowSelectStmt
 *
 * Does the SelectStmt define a sliding window CQ? Returns true
 * iff some part of the whereClause depends on clock_timestamp().
 */
bool
IsSlidingWindowSelectStmt(SelectStmt *stmt)
{
	return has_clock_timestamp(stmt->whereClause, NULL);
}

/*
 * IsSlidingWindowContinuousView
 */
bool
IsSlidingWindowContinuousView(RangeVar *cvname)
{
	char *sql = GetQueryString(cvname->relname, true);
	List *parsetree_list = pg_parse_query(sql);
	SelectStmt	*select_stmt;

	Assert(list_length(parsetree_list) == 1);

	select_stmt = (SelectStmt *) linitial(parsetree_list);
	return IsSlidingWindowSelectStmt(select_stmt);
}

/*
 * is_agg_func
 *
 * Does the node represent an aggregate function?
 *
 * TODO(usmanm): Turn this into a walker.
 */
static bool
is_agg_func(Node *node)
{
	HeapTuple	ftup;
	Form_pg_proc pform;
	bool is_agg = false;
	FuncCandidateList clist;
	FuncCall *fn;

	if (!IsA(node, FuncCall))
		return false;

	fn = (FuncCall *) node;

	clist = FuncnameGetCandidates(fn->funcname, list_length(fn->args), NIL, false, false, true);

	while (clist != NULL)
	{
		if (!OidIsValid(clist->oid))
			break;

		ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(clist->oid));
		if (!HeapTupleIsValid(ftup))
			break;

		pform = (Form_pg_proc) GETSTRUCT(ftup);
		is_agg = pform->proisagg;

		if (is_agg)
			break;

		clist = clist->next;
	}

	ReleaseSysCache(ftup);

	return is_agg;
}

/*
 * add_res_targets_for_missing_cols
 *
 * Add ResTargets for any ColRef that is missing in the
 * targetList of the SelectStmt.
 */
static void
add_res_targets_for_missing_cols(SelectStmt *stmt, List *cols)
{
	ListCell *clc;
	ListCell *tlc;
	ResTarget *res;

	foreach(clc, cols)
	{
		Node *node = (Node *) lfirst(clc);
		ColumnRef *cref;
		int location;
		bool found = false;

		if (IsA(node, TypeCast))
		{
			TypeCast *tc = (TypeCast *) node;
			cref = (ColumnRef *) tc->arg;
			location = tc->location;
		}
		else
		{
			cref = (ColumnRef *) lfirst(clc);
			location = cref->location;
		}

		foreach(tlc, stmt->targetList)
		{
			res = (ResTarget *) lfirst(tlc);
			found = does_colref_match_res_target((Node *) res, cref);

			if (found)
				break;
		}

		if (!found)
		{
			res = makeNode(ResTarget);
			res->name = NULL;
			res->indirection = NIL;
			res->val = node;
			res->location = location;
			stmt->targetList = lappend(stmt->targetList, res);
		}
	}
}

static bool
replace_colrefs_with_colnames(Node *node, void *colname)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		ColumnRef *cref = (ColumnRef *) node;
		if (colname == NULL)
			colname = FigureColname(node);
		cref->fields = list_make1(makeString(colname));
		return false;
	}

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		replace_colrefs_with_colnames(res->val, res->name);
		return false;
	}

	return raw_expression_tree_walker(node, replace_colrefs_with_colnames, colname);
}

/*
 * TransformSWSelectStmtForCQWorker
 */
SelectStmt *
TransformSWSelectStmtForCQWorker(SelectStmt *stmt)
{
	CQAnalyzeContext context;
		List *targetList = NIL;
		ListCell *clc;
		ListCell *tlc;
		ResTarget *res;
		Node *swExpr = GetSlidingWindowExpr(stmt, NULL);

		if (swExpr == NULL)
			return stmt;

		/*
		 * Find all ColRefs in matchExpr expression and add them to the
		 * targetList if they're missing.
		 */
		context.cols = NIL;
		find_col_refs(swExpr, &context);
		add_res_targets_for_missing_cols(stmt, context.cols);

		/*
		 * Find all ColRefs needed to calculate aggregates for this sliding
		 * window and add them to the targetList if they're missing.
		 *
		 * This step must happen after the projection of missing columns from
		 * the groupClause and matchExpr.
		 */
		foreach(tlc, stmt->targetList)
		{
			res = (ResTarget *) lfirst(tlc);

			if (is_agg_func(res->val))
			{
				context.cols = NIL;
				find_col_refs((Node *) res->val, &context);

				foreach(clc, context.cols)
				{
					Node *node = lfirst(clc);

					res = makeNode(ResTarget);
					res->name = NULL;
					res->indirection = NIL;
					res->val = (Node *) node;

					if (IsA(node, ColumnRef))
					{
						res->location = ((ColumnRef *) node)->location;
					}
					else
					{
						res->location = ((TypeCast *) node)->location;
					}

					targetList = lappend(targetList, res);
				}

				stmt->groupClause = NIL;
			}
			else
				targetList = lappend(targetList, res);
		}

		stmt->targetList = targetList;

		return stmt;
}

/*
 * TransformSWSelectStmtForCQView
 */
SelectStmt *
TransformSWSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel)
{
	Node *swExpr = GetSlidingWindowExpr(stmt, NULL);
	if (swExpr != NULL)
	{
		stmt->whereClause = swExpr;
		stmt->fromClause = list_make1(cqrel);
		replace_colrefs_with_colnames((Node *) stmt, NULL);
	}
	return stmt;
}
