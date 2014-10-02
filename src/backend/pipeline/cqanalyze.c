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

#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "utils/builtins.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

typedef struct CQAnalyzeContext
{
	ParseState *pstate;
	List *types;
	List *cols;
	List *streams;
	List *tables;
	List *targets;
} CQAnalyzeContext;

typedef struct CQValidTupleAnalyzeContext
{
	Node *matchExpr;
	List *cols;
} CQSlidingAnalyzeContext;

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
find_validation_expr(Node *node, CQSlidingAnalyzeContext *context)
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

		l_res = find_validation_expr(a_expr->lexpr, context);
		if (context->matchExpr != NULL)
			l_expr = context->matchExpr;
		else
			l_expr = a_expr->lexpr;

		context->matchExpr = NULL;

		r_res = find_validation_expr(a_expr->rexpr, context);
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

	return raw_expression_tree_walker(node, find_validation_expr, (void *) context);
}

/*
 * has_arrival_timestamp
 *
 * Walk the parse tree and return true iff ARRIVAL_TIMESTAMP is being
 * referenced.
 */
static bool
find_cols_to_store(Node *node, CQSlidingAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		context->cols = lappend(context->cols, node);
	}

	return raw_expression_tree_walker(node, find_cols_to_store, (void *) context);
}

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
	return raw_expression_tree_walker(node, does_colref_match_res_target, (void *) cref);
}

/*
 * invalidTupleSelectStmt
 */
DeleteStmt *
getGarbageTupleDeleteStmt(char *cv_name, SelectStmt *stmt)
{
	CQSlidingAnalyzeContext context;
	DeleteStmt *disqualified_delete_stmt = NULL;
	context.matchExpr = NULL;

	/* find the subset of the whereClause that we must match valid tuples */
	find_validation_expr(stmt->whereClause, &context);

	if (context.matchExpr)
	{
		disqualified_delete_stmt = makeNode(DeleteStmt);
		disqualified_delete_stmt->relation = makeRangeVar(NULL, cv_name, -1);
		disqualified_delete_stmt->whereClause = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, context.matchExpr, -1);
	}

	return disqualified_delete_stmt;
}

/*
 * getResTargetsForGarbageCollection
 *
 * Any ColRef that is used in an expression with "clock_timestamp()" in the WHERE clause
 * needs to be stored with the sliding event window.
 */
List *
getResTargetsForGarbageCollection(SelectStmt *stmt)
{
	CQSlidingAnalyzeContext context;
	List *gcResTargets = NIL;
	ListCell *clc;
	context.matchExpr = NULL;
	context.cols = NIL;

	if (!stmt->whereClause)
		return NIL;

	/* find the subset of the whereClause that depends on clock_timestamp() */
	find_validation_expr(stmt->whereClause, &context);

	if (!context.matchExpr)
		return NIL;

	/* find all ColRefs in the squashed whereClause expression. */
	find_cols_to_store((Node *) context.matchExpr, &context);

	foreach(clc, context.cols)
	{
		ColumnRef *cref = (ColumnRef *) lfirst(clc);
		ListCell *tlc;
		bool found = false;
		char *colname = FigureColname((Node *) cref);

		foreach(tlc, stmt->targetList)
		{
			ResTarget *res = (ResTarget *) lfirst(tlc);

			/* see if ColRef references an alias (<expr> AS <alias>). */
			if (res->name && strcmp(res->name, colname) == 0)
			{
				found = true;
				break;
			}

			if (does_colref_match_res_target(res->val, cref))
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			ResTarget *res = makeNode(ResTarget);
			res->name = NULL;
			res->indirection = NIL;
			res->val = (Node *) cref;
			res->location = cref->location;
			gcResTargets = lappend(gcResTargets, res);
		}
	}

	return gcResTargets;
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

	context.pstate = pstate;
	context.types = NIL;
	context.cols = NIL;
	context.streams = NIL;
	context.tables = NIL;
	context.targets = NIL;

	/* make sure that we can infer types for every column that appears anywhere in the statement */
	raw_expression_tree_walker((Node *) stmt, find_colref_types, (void *) &context);

	/* now indicate which relations are actually streams */
	raw_expression_tree_walker((Node *) stmt->fromClause, add_streams, (void *) &context);

	foreach(lc, context.cols)
	{
		ListCell *tlc;
		ListCell *slc;
		ColumnRef *cref = lfirst(lc);
		RangeVar *rv = NULL;
		bool needstype = true;
		bool hastype = false;

		if (list_length(cref->fields) == 2)
		{
			needstype = false;
			rv = makeRangeVar(NULL, strVal(linitial(cref->fields)), -1);
			foreach(slc, context.streams)
			{
				RangeVar *r = (RangeVar *) lfirst(slc);
				/* if it's a legit relation, the column doesn't need to have a type yet */
				if (equal(r, rv))
					needstype = true;
			}
			if (!needstype)
				continue;
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

		if (strcmp(strVal(linitial(cref->fields)), "arrival_timestamp") == 0)
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
