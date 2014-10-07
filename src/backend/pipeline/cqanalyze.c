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
} CQAnalyzeContext;

/*
 * find_invalidate_expr
 *
 * Walk the parse tree of the `whereClause` in the SelectStmt of
 * a CQ and set context->matchExpr to be the minimal expression
 * that must match for a tuple to be considered in the sliding window.
 *
 * This function tries to rip out all expressions not containing clock_timestamp().
 */
static bool
find_clock_time_expr(Node *node, CQAnalyzeContext *context)
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
	else if (IsA(node, A_Expr))
	{
		A_Expr *a_expr = (A_Expr *) node;
		bool l_res, r_res;
		Node *l_expr, *r_expr;

		context->matchExpr = NULL;

		l_res = find_clock_time_expr(a_expr->lexpr, context);
		if (context->matchExpr != NULL)
			l_expr = context->matchExpr;
		else
			l_expr = a_expr->lexpr;

		context->matchExpr = NULL;

		r_res = find_clock_time_expr(a_expr->rexpr, context);
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
				context->matchExpr = (Node *) makeA_Expr(AEXPR_AND, NIL, l_expr, r_expr, -1);
			else if (l_res)
				context->matchExpr = l_expr;
			else if (r_res)
				context->matchExpr = r_expr;
			return (l_res || r_res);
		case AEXPR_OR:
			if (l_res || r_res)
			{
				context->matchExpr = (Node *) makeA_Expr(AEXPR_OR, NIL, l_expr, r_expr, -1);
				return true;
			}
			return false;
		case AEXPR_NOT:
			if (r_res)
				context->matchExpr = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, r_expr, -1);
			return r_res;
		case AEXPR_OP_ANY:
		case AEXPR_OP_ALL:
		case AEXPR_DISTINCT:
		case AEXPR_NULLIF:
		case AEXPR_OF:
		case AEXPR_IN:
		default:
			/* TODO(usmanm): Implement these operators as well */
			elog(ERROR, "unsupported expression kind %d", a_expr->kind);
			break;
		}
	}

	return raw_expression_tree_walker(node, find_clock_time_expr, (void *) context);
}

/*
 * find_cols_to_store
 */
static bool
find_cols_to_store(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, find_cols_to_store, (void *) context);
}
/*
 * does_colref_match_res_target
 */
static bool
does_colref_match_res_target(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

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
 * invalidTupleSelectStmt
 */
Node *
getWindowMatchExpr(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	context.matchExpr = NULL;

	if (stmt->whereClause == NULL)
	{
		return NULL;
	}

	/* find the subset of the whereClause that we must match valid tuples */
	find_clock_time_expr(stmt->whereClause, &context);

	return context.matchExpr;
}

/*
 * is_agg_func
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

static List *
add_res_targets_for_missing_cols(List *targetList, List *cols, List *resTargetsToAdd)
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

		foreach(tlc, targetList)
		{
			res = (ResTarget *) lfirst(tlc);
			found = does_colref_match_res_target(res->val, cref);

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
			resTargetsToAdd = lappend(resTargetsToAdd, res);
		}
	}

	return resTargetsToAdd;
}

/*
 * transformSelectStmtForCQWorker
 *
 * If the SelectStmt doesn't define a sliding window query, this
 * is a no-op. Otherwise we add any ColRefs needed to recompute the
 * whereClause expression needed to test for inclusion in the window.
 * Furthermore, if a sliding window query has aggregates, we *flatten* the query
 * into a simple SELECT as the read path will do the aggregations.
 */
SelectStmt *
transformSelectStmtForCQWorker(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	List *resTargetsToAdd = NIL;
	List *targetList = NIL;
	ListCell *clc;
	ListCell *tlc;
	ResTarget *res;

	context.matchExpr = NULL;
	context.cols = NIL;

	if (stmt->whereClause == NULL)
		return stmt;

	/* find the subset of the whereClause that depends on clock_timestamp() */
	find_clock_time_expr(stmt->whereClause, &context);
	if (context.matchExpr == NULL)
		return stmt;

	/* find all ColRefs in the squashed whereClause expression. */
	find_cols_to_store((Node *) context.matchExpr, &context);
	resTargetsToAdd = add_res_targets_for_missing_cols(stmt->targetList, context.cols, resTargetsToAdd);

	/* find all ColRefs in the groupClause. */
	context.cols = NIL;
	find_cols_to_store((Node *) stmt->groupClause, &context);
	resTargetsToAdd = add_res_targets_for_missing_cols(stmt->targetList, context.cols, resTargetsToAdd);

	foreach(tlc, stmt->targetList)
	{
		res = (ResTarget *) lfirst(tlc);
		if (is_agg_func(res->val))
		{
			context.cols = NIL;
			find_cols_to_store((Node *) res->val, &context);

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

				resTargetsToAdd = lappend(resTargetsToAdd, res);
			}

			stmt->groupClause = NIL;
		}
		else
			targetList = lappend(targetList, res);
	}

	/*
	 * Add any columns that need to be kept around for GC or calculating
	 * aggregates for sliding windows.
	 * TODO(usmanm): Mark these columns as hidden/System Columns.
	 */
	targetList = list_concat(targetList, resTargetsToAdd);

	stmt->targetList = targetList;

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
 * transformFromStreamClause
 *
 * This is mainly to prepare a CV SELECT's FROM clause, which may involve streams
 */
void
analyzeContinuousSelectStmt(ParseState *pstate, SelectStmt **topselect)
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

		/* ensure that we have no '*' for a stream target */
		if (IsA(lfirst(cref->fields->tail), A_Star))
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("can't select %s", NameListToString(cref->fields)),
					 errhint("Explicitly state the fields you want to read from the stream"),
					 parser_errposition(pstate, cref->location)));
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
}

/*
 * transformStreamEntry --- transform a StreamDesc to a RangeTblEntry
 */
RangeTblEntry *
transformStreamEntry(ParseState *pstate, StreamDesc *stream)
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

Node *
transformToRangeSubselectIfWindowView(ParseState *pstate, RangeVar *rv)
{
	char			*sql;
	List			*parsetree_list;
	SelectStmt		*selectstmt;
	Node			*match_expr;
	RangeSubselect	*rss;
	Alias			*alias;

	if (pstate->p_sliding_select != NIL)
	{
		RangeVar *rv2 = linitial(pstate->p_sliding_select);
		if (equal(rv, rv2))
			return NULL;
	}

	sql = GetQueryStringOrNull(rv->relname, true);

	if (sql == NULL)
		return NULL;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);

	selectstmt = (SelectStmt *) linitial(parsetree_list);
	match_expr = getWindowMatchExpr(selectstmt);

	if (match_expr == NULL)
		return NULL;

	selectstmt->whereClause = match_expr;
	selectstmt->fromClause = list_make1(rv);

	alias = rv->alias;
	if (alias == NULL)
	{
		alias = makeNode(Alias);
		alias->aliasname = rv->relname;
	}

	rss = makeNode(RangeSubselect);
	rss->alias = alias;
	rss->subquery = (Node *) selectstmt;
	rss->lateral = false;

	return rss;
}
