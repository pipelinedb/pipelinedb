/*-------------------------------------------------------------------------
 *
 * cqwindow.c
 *	  Support for analyzing window queries
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqindow.c
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
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/analyze.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqwindow.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"
#define DATE_TRUNC "date_trunc"
#define DEFAULT_WINDOW_GRANULARITY "second"

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

		context->swExpr = NULL;

		l_res = find_clock_timestamp_expr(a_expr->lexpr, context);
		if (context->swExpr != NULL)
			l_expr = context->swExpr;
		else
			l_expr = a_expr->lexpr;

		context->swExpr = NULL;

		r_res = find_clock_timestamp_expr(a_expr->rexpr, context);
		if (context->swExpr)
			r_expr = context->swExpr;
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
				context->swExpr = (Node *) makeA_Expr(AEXPR_OP, a_expr->name, l_expr, r_expr, -1);
			return l_res || r_res;
		case AEXPR_AND:
			if (l_res)
				context->swExpr = l_expr;
			else if (r_res)
				context->swExpr = r_expr;
			return l_res || r_res;
		case AEXPR_NOT:
			if (r_res)
				context->swExpr = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, r_expr, -1);
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
get_sliding_window_expr(SelectStmt *stmt, CQAnalyzeContext *context)
{
	context->swExpr = NULL;

	/* find the subset of the whereClause that we must match valid tuples */
	find_clock_timestamp_expr(stmt->whereClause, context);

	return context->swExpr;
}

/*
 * validate_clock_timestamp_expr
 */
static void
validate_clock_timestamp_expr(SelectStmt *stmt, Node *expr, CQAnalyzeContext *context)
{
	A_Expr *a_expr;

	if (expr == NULL)
			return;

	context->cols = NIL;

	Assert(IsA(expr, A_Expr));
	a_expr = (A_Expr *) expr;

	FindColumnRefsWithTypeCasts(expr, context);
	if (list_length(context->cols) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("clock_timestamp can only appear in an expression containing a single column reference"),
						parser_errposition(context->pstate, a_expr->location)));

	/*
	 * TODO(usmanm): Ensure that context.cols[0] isn't being grouped on.
	 */
}

/*
 * ValidateSlidingWindowExpr
 */
void
ValidateSlidingWindowExpr(SelectStmt *stmt, CQAnalyzeContext *context)
{
	Node *swExpr = get_sliding_window_expr(stmt, context);
	validate_clock_timestamp_expr(stmt, swExpr, context);
}

/*
 * validate_windowdef
 */
static void
validate_windowdef(WindowDef *wdef, CQAnalyzeContext *context)
{
	if (list_length(wdef->orderClause) > 1)
	{
		/* Can't ORDER BY multiple columns. */
		elog(ERROR, "single order by only");
	}
	else
	{
		/* ORDER BY must be on a time-like column/expression */
		//SortBy *sort = (SortBy *) linitial(wdef->orderClause);
		/* TODO(usmanm): Check that output type of this column is time-like */
	}
}

/*
 * get_window_defs
 */
static void
get_window_defs(SelectStmt *stmt, CQAnalyzeContext *context)
{
	List *windows = NIL;
	ListCell *lc;

	/*
	 * Copy over all WINDOW clauses.
	 */
	foreach(lc, stmt->windowClause)
	{
		windows = lappend(windows, lfirst(lc));
	}

	context->funcCalls = NIL;
	CollectFuncs((Node *) stmt->targetList, context);

	/*
	 * Copy over all inline OVER clauses.
	 */
	foreach(lc, context->funcCalls)
	{
		FuncCall *fcall = (FuncCall *) lfirst(lc);
		/*
		 * Ignore over entries if they reference to *named*
		 * windows. Those windows should already be copied
		 * above.
		 */
		if (fcall->over == NULL || fcall->over->name != NULL)
			continue;
		windows = lappend(windows, fcall->over);
	}

	context->windows = windows;
}

/*
 * validate_windows
 */
void
ValidateWindows(SelectStmt *stmt, CQAnalyzeContext *context)
{
	ListCell *lc;

	get_window_defs(stmt, context);

	if (list_length(context->windows) > 1)
		elog(ERROR, "Only a single WINDOW is allowed");

	foreach(lc, context->windows)
	{
		WindowDef *wdef = (WindowDef *) lfirst(lc);
		validate_windowdef(wdef, context);
	}
}

/*
 * GetColumnRefInSlidingWindowExpr
 */
ColumnRef *
GetColumnRefInSlidingWindowExpr(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	Node *swExpr = get_sliding_window_expr(stmt, &context);
	Node *cref;

	context.cols = NIL;
	FindColumnRefsWithTypeCasts(swExpr, &context);
	cref = (Node *) linitial(context.cols);

	if (IsA(cref, TypeCast))
	{
		TypeCast *tc = (TypeCast *) cref;
		cref = tc->arg;
	}

	return (ColumnRef *) cref;
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
 * DoesViewAggregate
 *
 * TODO(usmanm): Change name of function.
 */
bool
DoesViewAggregate(SelectStmt *stmt, CQAnalyzeContext *context)
{
	bool hasAggsOrGroupBy = list_length(stmt->groupClause) > 0;
	bool hasWindow = false;
	ListCell *lc;

	context->swExpr = NULL;

	find_clock_timestamp_expr(stmt->whereClause, context);
	CollectAggFuncs((Node *) stmt->targetList, context);
	hasAggsOrGroupBy |= list_length(context->funcCalls) > 0;

	context->funcCalls = NIL;
	CollectFuncs((Node *) stmt->targetList, context);

	foreach(lc, context->funcCalls)
	{
		FuncCall *fcall = (FuncCall *) lfirst(lc);
		hasWindow |= fcall->over != NULL;
		if (hasWindow)
			break;
	}

	/*
	 * True iff has a WINDOW or has a sliding window expression in the WHERE clause
	 * and either has a GROUP BY or an aggregate function.
	 */
	return (hasWindow || (context->swExpr != NULL && hasAggsOrGroupBy));
}

/*
 * get_time_bucket_size
 */
static bool
get_time_bucket_size(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (context->stepSize != NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		/* Default to 'second'. */
		context->stepSize = DEFAULT_WINDOW_GRANULARITY;
		context->stepNode = node;
		return true;
	}

	if (IsA(node, FuncCall))
	{
		FuncCall *fcall = (FuncCall *) node;
		Node *truncArg;

		if (pg_strcasecmp(strVal(linitial(fcall->funcname)), DATE_TRUNC) != 0)
			return false;

		truncArg = linitial(fcall->args);

		if (!IsA(truncArg, A_Const))
			return false;

		if (!raw_expression_tree_walker(node, get_time_bucket_size, (void *) context))
			return false;

		context->stepSize = strVal(&((A_Const *) truncArg)->val);
		context->stepNode = node;

		return false;
	}

	return raw_expression_tree_walker(node, get_time_bucket_size, (void *) context);
}

/*
 * replace_column_ref
 */
static void
replace_column_ref(Node *node, ColumnRef *from, ColumnRef *to, CQAnalyzeContext *context)
{
	ListCell *lc;

	context->cols = NIL;
	FindColumnRefsWithTypeCasts(node, context);

	foreach(lc, context->cols)
	{
		Node *node = (Node *) lfirst(lc);
		ColumnRef *cref = GetColumnRef(node);
		if (!AreColumnRefsEqual((Node *) from, (Node *) cref))
			continue;

		memcpy(cref, to, sizeof(ColumnRef));
	}
}

/*
 * create_group_by_for_time_bucket_field
 */
static void
create_group_by_for_time_bucket_field(SelectStmt *workerstmt, SelectStmt *viewstmt, Node *timeNode, bool doesViewAggregate, CQAnalyzeContext *context)
{
	Node *timeCRefWithTC;

	context->cols = NIL;
	FindColumnRefsWithTypeCasts(timeNode, context);
	timeCRefWithTC = (Node *) linitial(context->cols);

	if (!IsA(timeCRefWithTC, TypeCast))
	{
		ListCell *lc;
		foreach(lc, context->types)
		{
			TypeCast *tc = (TypeCast *) lfirst(lc);
			if (AreColumnRefsEqual(tc->arg, timeCRefWithTC))
			{
				timeCRefWithTC = (Node *) tc;
				break;
			}
		}
	}

	if (doesViewAggregate)
	{
		/*
		 * This re-writes the query to be run by worker nodes.
		 * See the example re-writes below:
		 *
		 * SELECT user::int, COUNT(*) FROM click_stream
		 * WHERE date_trunc('minute', arrival_timestamp) > clock_timestamp() - interval '15 minute'
		 * GROUP BY user;
		 * =>
		 * SELECT user::int, COUNT(*), date_trunc('hour', arrival_timestamp) AS _0 FROM click_stream
		 * WHERE arrival_timestamp > clock_timestamp() - interval '15 minute'
		 * GROUP BY user, _0;
		 *
		 * SELECT COUNT(*) FROM sream
		 * WHERE arrival_timestamp > clock_timestamp() - interval '15 minute';
		 * =>
		 * SELECT COUNT(*), date_trunc('second', arrival_timestamp) AS _0 FROM stream
		 * WHERE arrival_timestamp > clock_timestamp() - interval '15 minute';
		 *
		 * The SELECT has a GROUP BY or some aggregate
		 * function. We reduce the materialization table
		 * data size by tweaking the sliding window *step-size*
		 * to be more coarse grained. What that allows us to do
		 * is store aggregates for small intervals and dynamically
		 * combine them on the read path, rather than storing all the
		 * events and computing the entire query over them.
		 */
		ColumnRef *cref = makeNode(ColumnRef);
		ResTarget *res = makeNode(ResTarget);
		FuncCall *func = makeNode(FuncCall);
		A_Const *aconst = makeNode(A_Const);
		char *name = GetUniqueInternalColname(context);
		ListCell *lc;
		ColumnRef *tcref = copyObject(GetColumnRef(timeCRefWithTC));

		/* Get step size. */
		context->stepSize = NULL;
		get_time_bucket_size(timeNode, context);

		/*
		 * Add ResTarget for time bucket field.
		 */
		aconst->val = *makeString(context->stepSize);
		aconst->location = -1;
		func->funcname = list_make1(makeString(DATE_TRUNC));
		func->args = list_make2(aconst, copyObject(timeCRefWithTC));
		func->location = -1;
		res->name = name;
		res->indirection = NIL;
		res->val = (Node *) func;
		res->location = -1;

		/*
		 * Add GROUP BY for step field. This is the group
		 * we will merge/combine on the read path.
		 */
		cref->fields = list_make1(makeString(name));
		cref->location = -1;
		workerstmt->groupClause = lappend(workerstmt->groupClause, cref);

		foreach(lc, workerstmt->targetList)
		{
			ResTarget *tres = (ResTarget *) lfirst(lc);

			char *name = FigureColname(tres->val);

			if (!ContainsColumnRef(tres->val, tcref))
				continue;

			/*
			 * Replace tcref with cref.
			 */
			replace_column_ref(tres->val, tcref, cref, context);

			if (IsAColumnRef(tres->val) && tres->name == NULL)
				tres->name = name;

			tres->val = copyObject(tres);
		}

		if (context->windows != NIL)
		{
			WindowDef *wdef = (WindowDef *) linitial(context->windows);

			foreach(lc, wdef->partitionClause)
			{
				Node *node = (Node *) lfirst(lc);
				if (!ContainsColumnRef(node, tcref))
					continue;

				replace_column_ref(node, tcref, cref, context);
			}
		}

		workerstmt->targetList = lappend(workerstmt->targetList, res);

		/*
		 * Replace date_trunc call in the stepNode
		 * with a ColumnRef to the ResTarget we created above.
		 * This is needed so that the VIEW can filter out disqualified tuples.
		 */
		memcpy(context->stepNode, cref, sizeof(ColumnRef));
	}
	else if (!IsColumnRefInTargetList(workerstmt, timeCRefWithTC))
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
		res->val = copyObject(timeCRefWithTC);
		res->location = -1;

		workerstmt->targetList = lappend(workerstmt->targetList, res);

		/*
		 * Replace the timeColumnRef with the new projected field's ColumnRef.
		 */
		memcpy(timeCRefWithTC, CreateColumnRefFromResTarget(res), sizeof(ColumnRef));
	}
}

/*
 * AddProjectionsAndGroupBysForWindows
 */
void
AddProjectionsAndGroupBysForWindows(SelectStmt *workerstmt, SelectStmt *viewstmt, bool doesViewAggregate, CQAnalyzeContext *context)
{
	Node *swExpr = get_sliding_window_expr(workerstmt, context);

	if (swExpr == NULL)
	{
		/*
		 * WINDOW Handling
		 */
		WindowDef *wdef;
		ListCell *lc;

		Assert(doesViewAggregate);

		get_window_defs(workerstmt, context);
		Assert(list_length(context->windows) > 1);

		wdef = (WindowDef *) linitial(context->windows);

		/*
		 * By default add an ORDER BY arrival_timestamp to the WINDOW.
		 */
		if (list_length(wdef->orderClause) == 0)
		{
			ColumnRef *cref = makeNode(ColumnRef);
			SortBy *sort = makeNode(SortBy);

			cref->fields = list_make1(makeString(ARRIVAL_TIMESTAMP));
			cref->location = -1;

			sort->node = (Node *) cref;
			sort->sortby_dir = SORTBY_DEFAULT;
			sort->sortby_nulls = SORTBY_NULLS_DEFAULT;
			sort->useOp = NIL;
			sort->location = -1;

			wdef->orderClause = list_make1(sort);
		}

		Assert(list_length(wdef->orderClause) == 1);

		/*
		 * Create a GROUP BY on the ORDER BY expr.
		 */
		foreach (lc, wdef->orderClause)
		{
			SortBy *sort = (SortBy *) lfirst(lc);
			create_group_by_for_time_bucket_field(workerstmt, viewstmt, sort->node, doesViewAggregate, context);
		}

		/*
		 * Create a GROUP BY for all expressions being
		 * PARTITION BY'd on.
		 */
		foreach(lc, wdef->partitionClause)
		{
			Node *node = lfirst(lc);
			ListCell *glc;
			bool skip = false;

			node = HoistNode(workerstmt, node, context);

			foreach(glc, workerstmt->groupClause)
			{
				if (AreColumnRefsEqual(node, (Node *) lfirst(glc)))
				{
					skip = true;
					break;
				}
			}

			if (!skip)
				workerstmt->groupClause = lappend(workerstmt->groupClause, node);
		}
	}
	else
	{
		/*
		 * Sliding Window Handling
		 */
		swExpr = copyObject(swExpr);
		create_group_by_for_time_bucket_field(workerstmt, viewstmt, swExpr, doesViewAggregate, context);

		/*
		 * Copy over the modified swExpr to the VIEW's SelectStmt.
		 */
		viewstmt->whereClause = swExpr;
	}
}

/*
 * TransformAggNodeForCQView
 */
void
TransformAggNodeForCQView(SelectStmt *viewselect, Node *node, ResTarget *aggres, bool doesViewAggregate)
{
	ColumnRef *cref;
	Assert(IsA(node, FuncCall));

	cref = CreateColumnRefFromResTarget(aggres);

	if (doesViewAggregate)
	{
		FuncCall *agg = (FuncCall *) node;
		agg->agg_star = false;
		agg->args = list_make1(cref);
	}
	else
		memcpy(node, cref, sizeof(ColumnRef));
}

/*
 * change_agg_arg_to_hidden
 */
static void
change_agg_arg_to_hidden(FuncCall *agg, Query *query, TupleDesc matdesc)
{
	ColumnRef *cref = linitial(agg->args);
	char *colName = FigureColname((Node *) cref);
	int i;
	ListCell *lc;

	foreach(lc, query->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (pg_strcasecmp(te->resname, colName) != 0)
			continue;

		Assert(IsA(te->expr, Aggref));

		if (!OidIsValid(GetCombineStateColumnType(te)))
			break;

		for (i = 0; i < matdesc->natts; i++)
		{
			if (pg_strcasecmp(NameStr(matdesc->attrs[i]->attname), colName) != 0)
				continue;
			cref->fields = list_make1(makeString(NameStr(matdesc->attrs[i + 1]->attname)));
			break;
		}
	}
}

/*
 * FixAggregatesForCQView
 */
void
FixAggArgForCQView(SelectStmt *viewselect, SelectStmt *workerselect, RangeVar *matrelation)
{
	ListCell *lc;
	CQAnalyzeContext context;
	Query *query;
	TupleDesc matdesc;

	if (!HasAggOrGroupBy(workerselect))
		return;

	query = parse_analyze(copyObject(workerselect), NULL, 0, 0);
	matdesc = RelationNameGetTupleDesc(matrelation->relname);

	foreach(lc, viewselect->targetList)
	{
		ResTarget *res = (ResTarget *) lfirst(lc);
		ListCell *alc;

		context.funcCalls = NIL;
		CollectAggFuncs((Node *) res, &context);

		if (list_length(context.funcCalls) == 0)
			continue;

		foreach(alc, context.funcCalls)
		{
			FuncCall *agg = (FuncCall *) lfirst(alc);

			if (pg_strcasecmp(NameListToString(agg->funcname), "count") == 0)
			{
				/* All COUNT(?) functions are re-written to SUM(count column) */
				agg->funcname = list_make1(makeString("sum"));
			}

			change_agg_arg_to_hidden(agg, query, matdesc);
		}
	}
}

/*
 * fix_sliding_window_expr
 */
static void
fix_sliding_window_expr(SelectStmt *stmt, Node *swExpr, CQAnalyzeContext *context)
{
	Node *cmpCRef;
	bool hasAggOrGrp = HasAggOrGroupBy(stmt);

	FindColumnRefsWithTypeCasts(swExpr, context);
	cmpCRef = (Node *) linitial(context->cols);

	if (hasAggOrGrp || !IsColumnRefInTargetList(stmt, cmpCRef))
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

		if (hasAggOrGrp)
		{
			/*
			 * This will swap out any date_trunc calls in the
			 * sliding window expression with the column reference
			 * to that field.
			 *
			 * TODO(usmanm): Will this work in all agg/group by cases?
			 */
			context->stepSize = NULL;
			get_time_bucket_size(swExpr, context);
		}
	}
}

/*
 * GetDeleteStmtForGC
 */
DeleteStmt *
GetDeleteStmtForGC(char *cvname, SelectStmt *stmt)
{
	CQAnalyzeContext context;
	DeleteStmt *delete_stmt;
	Node *swExpr = get_sliding_window_expr(stmt, &context);

	if (swExpr == NULL)
		return NULL;

	InitializeCQAnalyzeContext(stmt, NULL, &context);
	fix_sliding_window_expr(stmt, swExpr, &context);

	delete_stmt = makeNode(DeleteStmt);
	delete_stmt->relation = makeRangeVar(NULL, GetCQMatRelationName(cvname), -1);
	delete_stmt->whereClause = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, swExpr, -1);

	return delete_stmt;
}
