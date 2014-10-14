/*-------------------------------------------------------------------------
 *
 * cqanalyze.c
 *	  Support for analyzing continuous view statements, mainly to support
 *	  schema inference
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqanalyze.c
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
#include "pipeline/cqanalyze.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

typedef struct CQAnalyzeContext
{
	ParseState *pstate;
	List *types;
	List *cols;
	List *streams;
	List *tables;
	List *targets;
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
GetSlidingWindowMatchExpr(SelectStmt *stmt, ParseState *pstate)
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
 * GetCombineStateColumnType
 *
 * Retrieves the additional hidden columns that will be
 * required to store transition states for the given target entry
 */
Oid
GetCombineStateColumnType(TargetEntry *te)
{
	Oid result = InvalidOid;

	if (IsA(te->expr, Aggref))
	{
		Aggref *agg = (Aggref *) te->expr;

		result = GetCombineStateType(agg->aggfnoid);
	}

	return result;
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

/*
 * transform_select_for_sliding_window
 *
 * Performs the necessary transformations on the SelectStmt
 * so that it can be executed by CQ workers to generate
 * tuples for the underlying materialization table of the CQ.
 */
static void
transform_select_for_sliding_window(SelectStmt *stmt, Node *matchExpr)
{
	CQAnalyzeContext context;
	List *targetList = NIL;
	ListCell *clc;
	ListCell *tlc;
	ResTarget *res;

	/*
	 * Find all ColRefs in matchExpr expression and add them to the
	 * targetList if they're missing.
	 */
	context.cols = NIL;
	find_col_refs(matchExpr, &context);
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
 * GetSelectStmtForCQWorker
 *
 * Get the SelectStmt that should be executed by the
 * CQ worker on micro-batches.
 *
 * The targetList of this SelectStmt is also used to determine
 * the columns that must be created in the CQ's underlying
 * materialization table.
 */
SelectStmt *
GetSelectStmtForCQWorker(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	Node *matchExpr;

	stmt = (SelectStmt *) copyObject(stmt);

	/*
	 * Find all ColRefs in the groupClause and add them to the targetList
	 * if they're missing.
	 *
	 * These columns need to be projected by the worker so that the combiner
	 * can determine which row to merge the partial results with.
	 */
	context.cols = NIL;
	find_col_refs((Node *) stmt->groupClause, &context);
	add_res_targets_for_missing_cols(stmt, context.cols);

	/*
	 *
	 */
	matchExpr = GetSlidingWindowMatchExpr(stmt, NULL);
	if (matchExpr != NULL)
	{
		transform_select_for_sliding_window(stmt, matchExpr);
		return stmt;
	}

	/*
	 * TODO(usmanm/derekjn): We should add columns to store transition states here.
	 * These columns must have a unique name and need to be added with an explicit
	 * TypeCast for the analyzer to determine what the type of the column should be
	 * in the underlying materialization table.
	 */

	return stmt;
}

/*
 * GetSelectStmtForCQView
 *
 * Get the SelectStmt that should be passed to the VIEW we
 * create for this CQ.
 */
SelectStmt *
GetSelectStmtForCQView(SelectStmt *stmt, RangeVar *cqrel)
{
	Node		*match_expr;
	List		*origTargetList = stmt->targetList;
	ListCell	*lc;

	stmt = (SelectStmt *) copyObject(stmt);

	/*
	 * Is this a sliding window CQ? If so create a SELECT over
	 * the materialization table that filters based on the
	 * `matchExpr` rather than the original `whereClause`.
	 */
	match_expr = GetSlidingWindowMatchExpr(stmt, NULL);
	if (match_expr != NULL)
	{
		stmt->whereClause = match_expr;
		stmt->fromClause = list_make1(cqrel);
		replace_colrefs_with_colnames((Node *) stmt, NULL);
		return stmt;
	}

	/*
	 * Create a SelectStmt that only projects fields that
	 * the user expects in the continuous view.
	 */
	stmt = makeNode(SelectStmt);
	stmt->fromClause = list_make1(cqrel);
	stmt->targetList = NIL;

	/*
	 * Create a ResTarget to wrap a ColumnRef for each column
	 * name that we expect in the continuous view.
	 */
	foreach(lc, origTargetList)
	{
		ResTarget *origRes = lfirst(lc);
		ResTarget *res;
		ColumnRef *cref;
		char *colname;

		if (origRes->name != NULL)
			colname = origRes->name;
		else
			colname = FigureColname(origRes->val);

		cref = makeNode(ColumnRef);
		cref->fields = list_make1(makeString(colname));

		res = makeNode(ResTarget);
		res->name = NULL;
		res->indirection = NIL;
		res->val = (Node *) cref;
		res->location = origRes->location;

		stmt->targetList = lappend(stmt->targetList, res);
	}

	return stmt;
}

/*
 * find_colref_types
 *
 * Walk the parse tree and associate a single type with each inferred column
 */
static bool
find_colref_types(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
		{
			ListCell* lc;
			foreach(lc, context->types)
			{
				TypeCast *t = (TypeCast *) lfirst(lc);
				if (equal(tc->arg, t->arg) && !equal(tc, t))
				{
					/* a column can only be assigned one type */
					ColumnRef *cr = (ColumnRef *) tc->arg;
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
									errmsg("column has an ambiguous type because of a conflicting previous type cast"),
									parser_errposition(context->pstate, cr->location)));
				}
			}
			context->types = lappend(context->types, tc);
		}
	}
	else if (IsA(node, ColumnRef))
	{
		context->cols = lappend(context->cols, (ColumnRef *) node);
	}
	else if (IsA(node, ResTarget))
	{
		context->targets = lappend(context->targets, (ResTarget *) node);
	}

	return raw_expression_tree_walker(node, find_colref_types, (void *) context);
}

/*
 * add_streams
 *
 * Figure out which relations are streams that we'll need to infer types for
 */
static bool
add_streams(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeVar))
	{
		Oid reloid = RangeVarGetRelid((RangeVar *) node, NoLock, true);
		if (reloid != InvalidOid)
			context->tables = lappend(context->tables, node);
		else
			context->streams = lappend(context->streams, node);

		return false;
	}

	return raw_expression_tree_walker(node, add_streams, (void *) context);
}

/*
 * make_streamdesc
 *
 * Create a StreamDesc and build and attach a TupleDesc to it
 */
static Node *
make_streamdesc(RangeVar *rv, CQAnalyzeContext *context)
{
	List *attrs = NIL;
	ListCell *lc;
	StreamDesc *desc = makeNode(StreamDesc);
	TupleDesc tupdesc;
	bool onestream = false;
	AttrNumber attnum = 1;
	bool sawArrivalTime = false;

	desc->name = rv;

	if (list_length(context->streams) == 1 && list_length(context->tables) == 0)
		onestream = true;

	foreach(lc, context->types)
	{
		Oid oid;
		TypeCast *tc = (TypeCast *) lfirst(lc);
		ColumnRef *ref = (ColumnRef *) tc->arg;
		Form_pg_attribute attr;
		char *colname;
		if (onestream)
		{
			/* all of the inferred columns belong to this stream desc */
			RangeVar *s = linitial(context->streams);
			if (s->alias)
				colname = strVal(lsecond(ref->fields));
			else
				colname = strVal(linitial(ref->fields));
		}
		else if (list_length(ref->fields) == 2)
		{
			/* find the columns that belong to this stream desc */
			char *colrelname = strVal(linitial(ref->fields));
			char *relname = rv->alias ? rv->alias->aliasname : rv->relname;
			if (strcmp(relname, colrelname) != 0)
				continue;

			colname = strVal(lsecond(ref->fields));
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference is ambiguous"),
					 parser_errposition(context->pstate, ref->location)));
		}

		if (strcmp(colname, ARRIVAL_TIMESTAMP) == 0)
				sawArrivalTime = true;

		oid = LookupTypeNameOid(NULL, tc->typeName, false);
		attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attnum = attnum++;
		attr->atttypid = oid;

		namestrcpy(&attr->attname, colname);

		attrs = lappend(attrs, attr);
	}

	if (!sawArrivalTime)
	{
		Oid oid = LookupTypeNameOid(NULL, makeTypeName("timestamptz"), false);
		Form_pg_attribute attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attnum = attnum++;
		attr->atttypid = oid;
		namestrcpy(&attr->attname, ARRIVAL_TIMESTAMP);
		attrs = lappend(attrs, attr);
	}

	tupdesc = CreateTemplateTupleDesc(list_length(attrs), false);

	foreach(lc, attrs)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);

		/* PipelineDB XXX: should we be able to handle non-zero dimensions here? */
		TupleDescInitEntry(tupdesc, attr->attnum, NameStr(attr->attname),
				attr->atttypid, InvalidOid, 0);
	}

	desc->desc = tupdesc;

	return (Node *) desc;
}

/*
 * analyze_from_item
 *
 * Replaces RangeVar nodes that correspond to streams with StreamDesc nodes.
 *
 * Doing this as early as possible simplifies the rest of the analyze path.
 */
static Node*
analyze_from_item(Node *node, CQAnalyzeContext *context)
{
	if (IsA(node, RangeVar))
	{
		ListCell *lc;
		foreach(lc, context->streams)
		{
			RangeVar *rv = (RangeVar *) lfirst(lc);
			if (equal(rv, node))
				return make_streamdesc(rv, context);
		}

		/* not a stream */
		return node;
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *join = (JoinExpr *) node;
		join->larg = analyze_from_item(join->larg, context);
		join->rarg = analyze_from_item(join->rarg, context);

		return (Node *) join;
	}
	else
	{
		return node;
	}
}

/*
 * AnalyzeContinuousSelectStmt
 *
 * This is mainly to prepare a CV SELECT's FROM clause, which may involve streams
 */
void
AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **topselect)
{
	SelectStmt *stmt = *topselect;
	ListCell *lc;
	CQAnalyzeContext context;
	List *newfrom = NIL;

	if (stmt->sortClause != NULL)
	{
		elog(ERROR, "continuous view select can't have any sorting");
	}

	context.pstate = pstate;
	context.types = NIL;
	context.cols = NIL;
	context.streams = NIL;
	context.tables = NIL;
	context.targets = NIL;

	/* make sure that we can infer types for every column that appears anywhere in the statement */
	find_colref_types((Node *) stmt, &context);

	/* now indicate which relations are actually streams */
	add_streams((Node *) stmt->fromClause, &context);

	foreach(lc, context.cols)
	{
		ListCell *tlc;
		ListCell *slc;
		ColumnRef *cref = lfirst(lc);
		bool needstype = true;
		bool hastype = false;
		char *colname;

		/*
		 * Ensure that we have no '*' for a stream or relation target.
		 *
		 * We can't SELECT * from streams because we don't know the schema of
		 * streams ahead of time.
		 *
		 * XXX(usmanm): What if we have created a stream using the CREATE STREAM
		 * syntax? We should probably allow wildcards for such streams.
		 *
		 * XXX(usmanm): The decision to disallow wildcards for relations was taken
		 * because relations can be ALTERed in the future which would require us to
		 * ALTER the CQ's underlying materialization table and the VIEW. This can probably
		 * be accomplished by triggers, but lets just punt on this for now.
		 */
		if (IsA(lfirst(cref->fields->tail), A_Star))
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("can't select %s", NameListToString(cref->fields)),
					 errhint("Explicitly state the fields you want to select"),
					 parser_errposition(pstate, cref->location)));
		}

		if (list_length(cref->fields) == 2)
		{
			char *colrelname = strVal(linitial(cref->fields));

			colname = strVal(lsecond(cref->fields));
			needstype = false;

			foreach(slc, context.streams)
			{
				RangeVar *r = (RangeVar *) lfirst(slc);
				char *sname = r->alias ? r->alias->aliasname : r->relname;

				/* if it's a legit relation, the column doesn't need to have a type yet */
				if (strcmp(colrelname, sname) == 0)
				{
					needstype = true;
					break;
				}
			}
			if (!needstype)
				continue;
		}
		else
		{
			colname = strVal(linitial(cref->fields));
		}

		/* verify that we have a type for the column if it needs one */
		foreach(tlc, context.types)
		{
			TypeCast *tc = (TypeCast *) lfirst(tlc);
			if (equal(tc->arg, cref))
			{
				hastype = true;
				break;
			}
		}

		/* if the ColRef refers to a named ResTarget then it doesn't need an explicit type */
		foreach(tlc, context.targets)
		{
			ResTarget *rt = (ResTarget *) lfirst(tlc);
			if (rt->name != NULL && strcmp(strVal(linitial(cref->fields)), rt->name) == 0)
			{
				needstype = false;
				break;
			}
		}

		if (strcmp(colname, "arrival_timestamp") == 0)
			needstype = false;

		if (needstype && !hastype)
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference \"%s\" has an ambiguous type", NameListToString(cref->fields)),
					 errhint("Explicitly cast to the desired type. For example, %s::integer.", NameListToString(cref->fields)),
					 parser_errposition(pstate, cref->location)));
		}
	}

	foreach(lc, stmt->fromClause)
	{
		Node *n = (Node *) lfirst(lc);
		Node *newnode = analyze_from_item(n, &context);

		newfrom = lappend(newfrom, newnode);
	}

	stmt->fromClause = newfrom;

	/* This will validate the sliding window expression */
	GetSlidingWindowMatchExpr(stmt, pstate);
}

/*
 * TransformStreamEntry
 *
 * Transform a StreamDesc to a RangeTblEntry
 */
RangeTblEntry *
TransformStreamEntry(ParseState *pstate, StreamDesc *stream)
{
	RangeVar *relation = stream->name;
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	char *refname = relation->alias ? relation->alias->aliasname : relation->relname;

	rte->rtekind = RTE_RELATION;
	rte->alias = relation->alias;
	rte->inFromCl = true;
	rte->requiredPerms = ACL_SELECT;
	rte->checkAsUser = InvalidOid;		/* not set-uid by default, either */
	rte->selectedCols = NULL;
	rte->modifiedCols = NULL;
	rte->relname = refname;

	rte->eref = makeAlias(refname, NIL);
	rte->streamdesc = stream;

	if (pstate != NULL)
		pstate->p_rtable = lappend(pstate->p_rtable, rte);

	return rte;
}
