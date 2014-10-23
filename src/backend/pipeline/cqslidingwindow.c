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
#include "commands/pipelinecmds.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqslidingwindow.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

/*
 * find_clock_timestamp_expr
 *
 * This function picks out the part of the where clause
 * that depends on clock_timestamp()--this expression defines
 * a sliding window in PipelineDB land.
 *
 * XXX(usmanm): For now we only allow a clock_timestamp()
 * expression to exist in a conjunction at the top level of the
 * whereClause expression tree. This lets us avoid complicated
 * situations like sliding windows with holes etc. which turns
 * into a tricky combinatorial problem.
 */
static bool
find_clock_timestamp_expr(Node *node, CQAnalyzeContext *context)
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

		l_res = find_clock_timestamp_expr(a_expr->lexpr, context);
		if (context->matchExpr != NULL)
			l_expr = context->matchExpr;
		else
			l_expr = a_expr->lexpr;

		context->matchExpr = NULL;

		r_res = find_clock_timestamp_expr(a_expr->rexpr, context);
		if (context->matchExpr)
			r_expr = context->matchExpr;
		else
			r_expr = a_expr->rexpr;

		if (l_res && r_res)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("clock_timestamp may only appear once in a WHERE clause"),
							parser_errposition(context->pstate, context->location)));

		switch (a_expr->kind)
		{
		case AEXPR_OP:
			if (l_res || r_res)
				context->matchExpr = (Node *) makeA_Expr(AEXPR_OP, a_expr->name, l_expr, r_expr, -1);
			return l_res || r_res;
		case AEXPR_AND:
			if (l_res)
				context->matchExpr = l_expr;
			else if (r_res)
				context->matchExpr = r_expr;
			return l_res || r_res;
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

	return raw_expression_tree_walker(node, find_clock_timestamp_expr, (void *) context);
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
 * get_sliding_window_expr
 */
static Node *
get_sliding_window_expr(SelectStmt *stmt, ParseState *pstate)
{
	CQAnalyzeContext context;
	context.pstate = pstate;
	context.matchExpr = NULL;

	if (stmt->whereClause == NULL)
	{
		return NULL;
	}

	/* find the subset of the whereClause that we must match valid tuples */
	find_clock_timestamp_expr(stmt->whereClause, &context);

	return context.matchExpr;
}

/*
 * validate_clock_timestamp_expr
 */
static void
validate_clock_timestamp_expr(SelectStmt *stmt, Node *node, ParseState *pstate)
{
	CQAnalyzeContext context;
	A_Expr *a_expr;
	context.cols = NIL;
	context.pstate = pstate;

	if (node == NULL)
		return;

	Assert(IsA(node, A_Expr));
	a_expr = (A_Expr *) node;

	FindColumnRefsWithTypeCasts(node, &context);
	if (list_length(context.cols) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("clock_timestamp can only appear as in an expression containing a single column reference"),
						parser_errposition(pstate, a_expr->location)));


}

/*
 * ValidateSlidingWindowExpr
 */
void
ValidateSlidingWindowExpr(SelectStmt *stmt, ParseState *pstate)
{
	Node *swExpr = get_sliding_window_expr(stmt, pstate);
	validate_clock_timestamp_expr(stmt, swExpr, pstate);
}

/*
 * IsSlidingWindowSelectStmt
 *
 * Does the SelectStmt define a sliding window CQ? Returns true
 * iff some part of the whereClause depends on clock_timestamp().
 *
 * XXX(usmanm): This doesn't validate the clock_timestamp expression
 * on purpose because this function is only ever used on CQs that
 * have been CREATEd which implies they are valid.
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
 * Does the node contain an aggregate function?
 */
static bool
has_agg_func(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *fn = (FuncCall *) node;
		HeapTuple ftup;
		Form_pg_proc pform;
		bool is_agg = false;
		FuncCandidateList clist;

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
			ReleaseSysCache(ftup);

			if (is_agg)
				break;

			clist = clist->next;
		}

		return is_agg;
	}

	return raw_expression_tree_walker(node, has_agg_func, context);
}

/*
 * replace_colrefs_with_colnames
 */
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
 * get_step_size
 */
static bool
get_step_size(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (context->stepSize != NULL)
		return false;

	if (IsA(node, ColumnRef))
		return true;

	if (IsA(node, FuncCall))
	{
		FuncCall *fcall = (FuncCall *) node;
		Node *truncArg;
		ColumnRef *cref;
		char *colName = FigureColname(linitial(context->cols));

		if (pg_strcasecmp(strVal(linitial(fcall->funcname)), "date_trunc") != 0)
			return false;

		truncArg = linitial(fcall->args);

		if (!IsA(truncArg, A_Const))
			return false;

		if (!raw_expression_tree_walker(node, get_step_size, (void *) context))
			return false;

		context->stepSize = strVal(&((A_Const *) truncArg)->val);

		/* Swap out the FuncCall with the column its wrapping */
		cref = (ColumnRef *) node;
		cref->type = T_ColumnRef;
		cref->location = -1;
		cref->fields = list_make1(makeString(colName));

		/*
		 * XXX(usmanm): This is safe to do because sizeof(FuncCall)
		 * > sizeof(ColumnRef) which means we should have enough
		 * memory change in place node from a FuncCall to a ColumnRef.
		 */
		memcpy(node, cref, sizeof(ColumnRef));

		Assert(IsA(node, ColumnRef));

		return false;
	}

	return raw_expression_tree_walker(node, get_step_size, (void *) context);
}

/*
 * has_agg_or_group_by
 */
static bool
has_agg_or_group_by(SelectStmt *stmt)
{
	bool hasAgg;
	ListCell *tlc;
	ResTarget *res;

	/* Do we have any aggregates? */
	foreach(tlc, stmt->targetList)
	{
		res = (ResTarget *) lfirst(tlc);
		if (has_agg_func(res->val, NULL))
		{
			hasAgg = true;
			break;
		}
	}

	return (hasAgg || stmt->groupClause != NIL);
}

/*
 * TransformSWSelectStmtForCQWorker
 */
SelectStmt *
TransformSWSelectStmtForCQWorker(SelectStmt *stmt, 	CQAnalyzeContext *context)
{
	Node *swExpr = get_sliding_window_expr(stmt, NULL);
	Node *cmpCRef;

	if (swExpr == NULL)
		return stmt;

	context->cols = NIL;
	FindColumnRefsWithTypeCasts(swExpr, context);
	cmpCRef = (Node *) linitial(context->cols);

	/*
	 * This re-writes the query to be run by worker nodes.
	 * See the example re-writes below:
	 *
	 * SELECT user::int, COUNT(*) FROM click_stream
	 * WHERE date_trunc('minute', arrival_timestamp) > clock_timestamp() - interval '15 minute'
	 * GROUP BY user;
	 * =>
	 * SELECT user::int, COUNT(*), date_trunc('hour', arrival_timestamp) AS _0 FROM click_stream
	 * WHERE _0 > clock_timestamp() - interval '15 minute'
	 * GROUP BY user, _0;
	 *
	 * SELECT COUNT(*) FROM sream
	 * WHERE arrival_timestamp > clock_timestamp() - interval '15 minute';
	 * =>
	 * SELECT COUNT(*), date_trunc('second', arrival_timestamp) AS _0 FROM stream
	 * WHERE _0 > clock_timestamp() - interval '15 minute';
	 */
	if (has_agg_or_group_by(stmt))
	{
		/*
		 * The SELECT has a GROUP BY or some aggregate
		 * function. We reduce the materialization table
		 * data size by tweaking the sliding window *step-size*
		 * to be more coarse grained. What that allows us to do
		 * is store aggregates for small intervals and dynamically
		 * combine them on the read path, rather than storing all the
		 * events and computing the entiring query over them.
		 */
		ColumnRef *cref = makeNode(ColumnRef);
		ResTarget *res = makeNode(ResTarget);
		FuncCall *func = makeNode(FuncCall);
		A_Const *aconst = makeNode(A_Const);
		char *colName = FigureColname(linitial(context->cols));
		char *stepSize;

		/* Get step size. Default to 'second'. */
		context->stepSize = NULL;
		get_step_size(swExpr, context);
		if (context->stepSize == NULL)
			stepSize = "second";
		else
			stepSize = context->stepSize;

		/*
		 * Add ResTarget for step size field.
		 * We reuse the colname of the ColumnRef for
		 * this field.
		 */
		aconst->val = *makeString(stepSize);
		aconst->location = -1;
		func->funcname = list_make1(makeString("date_trunc"));
		func->args = list_concat(list_make1(aconst), context->cols);
		func->location = -1;
		res->name = colName;
		res->indirection = NIL;
		res->val = (Node *) func;
		res->location = -1;
		stmt->targetList = lappend(stmt->targetList, res);

		/*
		 * Add GROUP BY for step field. This is the group
		 * we will merge/combine on the read path.
		 */
		cref->fields = list_make1(makeString(colName));
		cref->location = -1;
		stmt->groupClause = lappend(stmt->groupClause, cref);
	}
	else if (!IsColumnRefInTargetList(stmt, cmpCRef))
	{
		/*
		 * The CQ is requesting raw events in the sliding window.
		 * In this case, simply add the column that clock_timestamp()
		 * is being compared to to the targetList (if missing).
		 */
		ResTarget *res = makeNode(ResTarget);
		char *name = GetUniqueInternalColname(context);

		res->name = name;
		res->indirection = NIL;
		res->val = cmpCRef;
		res->location = -1;

		stmt->targetList = lappend(stmt->targetList, res);
	}

	return stmt;
}

/*
 * TransformSWSelectStmtForCQView
 */
SelectStmt *
TransformSWSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel, CQAnalyzeContext *context)
{
	Node *swExpr = get_sliding_window_expr(stmt, NULL);
	Node *cmpCRef;

	if (swExpr == NULL)
		return stmt;

	context->cols = NIL;
	FindColumnRefsWithTypeCasts(swExpr, context);
	cmpCRef = (Node *) linitial(context->cols);

	if (has_agg_or_group_by(stmt))
	{
		/*
		 * This will swap out any date_trunc calls in the
		 * sliding window expression with the column reference
		 * to that field.
		 */
		context->stepSize = NULL;
		get_step_size(swExpr, context);
	}
	else if (!IsColumnRefInTargetList(stmt, cmpCRef))
	{
		/*
		 * Find the name we picked for the column we're
		 * comparing clock_timestamp() to and replace
		 * it with a ColumnRef to that.
		 */
		char *name = GetUniqueInternalColname(context);
		/*
		 * XXX(usmanm): This is hackery. cmpCRef can be a
		 * TypeCast or a ColumnRef but since sizeof(TypeCast)
		 * > sizeof(ColumnRef), we can force cast cmpCRef
		 * to be a ColumnRef.
		 */
		ColumnRef *cref = (ColumnRef *) cmpCRef;
		cref->type = T_ColumnRef;
		cref->fields = list_make1(makeString(name));
		cref->location = -1;
	}

	stmt->whereClause = swExpr;
	replace_colrefs_with_colnames((Node *) stmt, NULL);
	stmt->fromClause = list_make1(cqrel);
	stmt->groupClause = NIL;
	return stmt;
}

/*
 * GetDeleteStmtForGC
 */
DeleteStmt *
GetDeleteStmtForGC(char *cvname, SelectStmt *stmt)
{
	CQAnalyzeContext context;
	DeleteStmt *delete_stmt;
	Node *swExpr = get_sliding_window_expr(stmt, NULL);

	if (swExpr == NULL)
		return NULL;

	context.cols = NIL;
	FindColumnRefsWithTypeCasts(swExpr, &context);
	Assert(list_length(context.cols) == 1);

	if (has_agg_or_group_by(stmt))
	{
		/*
		 * This will swap out any date_trunc calls in the
		 * sliding window expression with the column reference
		 * to that field.
		 *
		 * TODO(usmanm): Will this work in all agg/group by cases?
		 */
		context.stepSize = NULL;
		get_step_size(swExpr, &context);
	}

	delete_stmt = makeNode(DeleteStmt);
	delete_stmt->relation = makeRangeVar(NULL, GetCQMatRelationName(cvname), -1);
	delete_stmt->whereClause = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, swExpr, -1);

	return delete_stmt;
}
