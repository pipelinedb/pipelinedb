/* Copyright (c) 2013-2015 PipelineDB */
	/*-------------------------------------------------------------------------
 *
 * cont_analyze.c
 *	  Support for parsing and analyzing continuous queries
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#define CLOCK_TIMESTAMP "clock_timestamp"

static bool
collect_column_names(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL)
			context->colnames = lappend(context->colnames, res->name);
	}
	else if (IsA(node, ColumnRef))
		context->colnames = lappend(context->colnames, FigureColname(node));

	return raw_expression_tree_walker(node, collect_column_names, (void *) context);
}

ContAnalyzeContext *
MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select)
{
	ContAnalyzeContext *context = palloc0(sizeof(ContAnalyzeContext));

	context->pstate = pstate;

	/*
	 * Collect any column names being used, so we don't clobber them when generating
	 * internal column names for the materialization table.
	 */
	collect_column_names((Node *) select, context);

	return context;
}

/*
 * collect_rels_and_streams
 */
bool
collect_rels_and_streams(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeVar))
	{
		Oid reloid = RangeVarGetRelid((RangeVar *) node, NoLock, true);
		if (reloid != InvalidOid)
			context->rels = lappend(context->rels, node);
		else
			context->streams = lappend(context->streams, node);

		return false;
	}

	return raw_expression_tree_walker(node, collect_rels_and_streams, (void *) context);
}

/*
 * collect_types_and_cols
 */
bool
collect_types_and_cols(Node *node, ContAnalyzeContext *context)
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
		context->cols = lappend(context->cols, node);

	return raw_expression_tree_walker(node, collect_types_and_cols, (void *) context);
}

static bool
contains_node(Node *node, Node *child)
{
	if (node == NULL || child == NULL)
		return false;

	if (equal(node, child))
		return true;

	return raw_expression_tree_walker(node, contains_node, (void *) child);
}

static bool
collect_agg_funcs(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *func = (FuncCall *) node;
		HeapTuple ftup;
		Form_pg_proc pform;
		bool is_agg = func->agg_within_group;
		FuncCandidateList clist;

		if (!func->agg_within_group)
		{
			clist = FuncnameGetCandidates(func->funcname, list_length(func->args), NIL, true, false, true);
			while (clist)
			{
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
		}

		if (is_agg)
			context->funcs = lappend(context->funcs, func);
	}

	return raw_expression_tree_walker(node, collect_agg_funcs, context);
}

static void
collect_windows(SelectStmt *stmt, ContAnalyzeContext *context)
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

	context->funcs = NIL;
	collect_agg_funcs((Node *) stmt->targetList, context);

	/*
	 * Copy over all inline OVER clauses.
	 */
	foreach(lc, context->funcs)
	{
		FuncCall *func = (FuncCall *) lfirst(lc);
		/*
		 * Ignore entries that reference names windows. Such
		 * windows should already be copied in the above loop.
		 */
		if (func->over == NULL || func->over->name != NULL)
			continue;
		windows = lappend(windows, func->over);
	}

	context->windows = windows;
}

/*
 * compare_attrs
 */
static int
compare_attrs(const void *a, const void *b)
{
	const Form_pg_attribute attr1 = *((const Form_pg_attribute* ) a);
	const Form_pg_attribute attr2 = *((const Form_pg_attribute* ) b);
	return pg_strcasecmp(NameStr(attr1->attname), NameStr(attr2->attname));
}

/*
 * make_streamdesc
 *
 * Create a StreamDesc and build and attach a TupleDesc to it
 */
static Node *
make_streamdesc(RangeVar *rv, ContAnalyzeContext *context)
{
	List *attrs = NIL;
	ListCell *lc;
	StreamDesc *desc = makeNode(StreamDesc);
	TupleDesc tupdesc;
	bool onestream = list_length(context->streams) == 1 && list_length(context->rels) == 0;
	AttrNumber attnum = 1;
	bool sawArrivalTime = false;
	Form_pg_attribute *attrsArray;
	int i;

	desc->name = rv;

	foreach(lc, context->types)
	{
		Oid oid;
		TypeCast *tc = (TypeCast *) lfirst(lc);
		ColumnRef *ref = (ColumnRef *) tc->arg;
		Form_pg_attribute attr;
		char *colname;
		bool alreadyExists = false;
		ListCell *lc;

		if (list_length(ref->fields) == 2)
		{
			/* find the columns that belong to this stream desc */
			char *colrelname = strVal(linitial(ref->fields));
			char *relname = rv->alias ? rv->alias->aliasname : rv->relname;
			if (pg_strcasecmp(relname, colrelname) != 0)
				continue;
		}

		if ((list_length(ref->fields) == 1) && !onestream)
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("column reference is ambiguous"),
					 parser_errposition(context->pstate, ref->location)));

		colname = FigureColname((Node *) ref);

		/* Dedup */
		foreach (lc, attrs)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
			if (pg_strcasecmp(colname, NameStr(attr->attname)) == 0)
			{
				alreadyExists = true;
				break;
			}
		}

		if (alreadyExists)
			continue;

		if (pg_strcasecmp(colname, ARRIVAL_TIMESTAMP) == 0)
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

	attrsArray = (Form_pg_attribute *) palloc(list_length(attrs) * sizeof(Form_pg_attribute));
	i = 0;

	foreach(lc, attrs)
	{
		attrsArray[i] = (Form_pg_attribute) lfirst(lc);
		i++;
	}

	qsort(attrsArray, list_length(attrs), sizeof(Form_pg_attribute), &compare_attrs);

	tupdesc = CreateTemplateTupleDesc(list_length(attrs), false);

	for (i = 0; i < list_length(attrs); i++)
	{
		Form_pg_attribute attr = attrsArray[i];

		/* PipelineDB XXX: should we be able to handle non-zero dimensions here? */
		TupleDescInitEntry(tupdesc, attr->attnum, NameStr(attr->attname),
				attr->atttypid, InvalidOid, 0);
	}

	pfree(attrsArray);
	desc->desc = tupdesc;

	return (Node *) desc;
}

/*
 * replace_stream_rangevar_with_streamdesc
 *
 * Doing this as early as possible simplifies the rest of the analyze path.
 */
static Node*
replace_stream_rangevar_with_streamdesc(Node *node, ContAnalyzeContext *context)
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
		join->larg = replace_stream_rangevar_with_streamdesc(join->larg, context);
		join->rarg = replace_stream_rangevar_with_streamdesc(join->rarg, context);

		return (Node *) join;
	}
	else
		return node;
}

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
find_clock_timestamp_expr(Node *node, ContAnalyzeContext *context)
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

		context->expr = NULL;

		l_res = find_clock_timestamp_expr(a_expr->lexpr, context);
		if (context->expr != NULL)
			l_expr = context->expr;
		else
			l_expr = a_expr->lexpr;

		context->expr = NULL;

		r_res = find_clock_timestamp_expr(a_expr->rexpr, context);
		if (context->expr)
			r_expr = context->expr;
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
				context->expr = (Node *) makeA_Expr(AEXPR_OP, a_expr->name, l_expr, r_expr, -1);
			return l_res || r_res;
		case AEXPR_AND:
			if (l_res)
				context->expr = l_expr;
			else if (r_res)
				context->expr = r_expr;
			return l_res || r_res;
		case AEXPR_NOT:
			if (r_res)
				context->expr = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, r_expr, -1);
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

static Node *
get_column_ref(Node *node)
{
	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
			node = tc->arg;
	}

	if (IsA(node, ColumnRef))
		return node;

	return NULL;
}

/*
 * validate_clock_timestamp_expr
 */
static void
validate_clock_timestamp_expr(SelectStmt *stmt, Node *expr, ContAnalyzeContext *context)
{
	A_Expr *a_expr;
	ListCell *lc;
	Node *col;

	if (expr == NULL)
		return;

	context->cols = NIL;

	Assert(IsA(expr, A_Expr));
	a_expr = (A_Expr *) expr;

	collect_types_and_cols(expr, context);

	/* Only a single column can be compared to clock_timestamp */
	if (list_length(context->cols) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("clock_timestamp can only appear in an expression containing a single column reference"),
						parser_errposition(context->pstate, a_expr->location)));

	col = get_column_ref(linitial(context->cols));

	/* Ensure that context.cols[0] isn't being grouped on */
	foreach(lc, stmt->groupClause)
	{
		Node *node = get_column_ref(lfirst(lc));

		if (!node)
			continue;

		if (equal(col, node))
		{
			ColumnRef *cref = (ColumnRef *) node;
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("the column being compared to clock_timestamp cannot be in the GROUP BY clause"),
							parser_errposition(context->pstate, cref->location)));
		}
	}
}

/*
 * ValidateContinuousQuery
 */
void
ValidateContinuousQuery(CreateContinuousViewStmt *stmt, const char *sql)
{
	SelectStmt *select = (SelectStmt *) copyObject(stmt->query);
	ContAnalyzeContext *context = MakeContAnalyzeContext(make_parsestate(NULL), select);
	ListCell *lc;

	context->pstate->p_sourcetext = sql;

	collect_rels_and_streams((Node *) select->fromClause, context);
	collect_types_and_cols((Node *) select, context);
	collect_agg_funcs((Node *) select, context);

	if (list_length(select->sortClause) > 0)
	{
		SortBy *sortby = (SortBy *) linitial(select->sortClause);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("continuous queries don't support ORDER BY"),
						parser_errposition(context->pstate, sortby->location)));
	}

	/* Ensure that we're reading from at least one stream */
	if (!context->streams)
	{
		RangeVar *t = (RangeVar *) linitial(context->rels);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("continuous queries must include a stream in the FROM clause"),
						errhint("To include a relation in a continuous query, JOIN it with a stream."),
						parser_errposition(context->pstate, t->location)));
	}

	/* Ensure that we're not trying to read from ourselves, which right now would be treated as a stream. */
	foreach(lc, context->streams)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		if (equal(rv, stmt->into->rel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("continuous queries cannot read from themselves"),
					errhint("Remove \"%s\" from the FROM clause.", rv->relname),
					parser_errposition(context->pstate, rv->location)));
	}

	/* Ensure that each column being read from a stream is type-casted */
	foreach(lc, context->cols)
	{
		ListCell *lc2;
		ColumnRef *cref = lfirst(lc);
		bool needs_type = true;
		bool has_type = false;
		bool refs_target;
		char *colname = FigureColname((Node *) cref);
		char *qualname = NameListToString(cref->fields);

		/*
		 * ARRIVAL_TIMESTAMP doesn't require an explicit type cast. We don't care about the
		 * qualified name because even if it's a column for a table, we don't need an explicit
		 * type.
		 */
		if (pg_strcasecmp(colname, ARRIVAL_TIMESTAMP) == 0)
			continue;

		/*
		 * Ensure that we have no `*` in the target list.
		 *
		 * We can't SELECT * from streams because we don't know the schema of
		 * streams ahead of time.
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
					 errmsg("can't select \"%s\" in continuous queries", qualname),
					 errhint("Explicitly state the columns you want to SELECT."),
					 parser_errposition(context->pstate, cref->location)));
		}

		/*
		 * If the column is for a relation, we don't need any explicit type case.
		 */
		if (list_length(cref->fields) == 2)
		{
			char *col_relname = strVal(linitial(cref->fields));
			needs_type = false;

			foreach(lc2, context->streams)
			{
				RangeVar *rv = (RangeVar *) lfirst(lc2);
				char *streamname = rv->alias ? rv->alias->aliasname : rv->relname;

				if (pg_strcasecmp(col_relname, streamname) == 0)
				{
					needs_type = true;
					break;
				}
			}

			if (!needs_type)
				continue;
		}

		/* Do we have a TypeCast for this ColumnRef? */
		foreach(lc2, context->types)
		{
			TypeCast *tc = (TypeCast *) lfirst(lc2);
			if (equal(tc->arg, cref))
			{
				has_type = true;
				break;
			}
		}

		if (has_type)
			continue;

		/*
		 * If the column refers to a named target then it doesn't need an explicit type.
		 *
		 * This doesn't work in cases where target references the column being checked.
		 * For example:
		 *
		 *   SELECT date_trunc('hour', x) AS x FROM stream
		 *   SELECT substring(y::text, 1, 2) as x, substring(x, 1, 2) FROM stream
		 *
		 * In both these examples we need an explicit cast for `x`.
		 */
		needs_type = false;
		refs_target = false;

		foreach(lc2, select->targetList)
		{
			ResTarget *res = (ResTarget *) lfirst(lc2);

			if (contains_node((Node *) res, (Node *) cref))
			{
				needs_type = true;
				break;
			}

			if (res->name && pg_strcasecmp(qualname, res->name) == 0)
				refs_target = true;
		}

		if (refs_target && !needs_type)
			continue;

		/* If it doesn't reference a target or is contained in a target, it needs a type */
		if (needs_type || !refs_target)
		{
			/*
			 * If it's a stream-table join, try to do some extra work to make the error
			 * informative, as the user may be trying to join against a nonexistent table.
			 */
			bool has_join = false;
			RangeVar *rel = NULL;

			foreach(lc, select->fromClause)
			{
				Node *n = (Node *) lfirst(lc);
				if (IsA(n, JoinExpr))
				{
					has_join = true;
					break;
				}
			}

			if (list_length(cref->fields) > 1)
			{
				Value *alias = linitial(cref->fields);
				foreach(lc, context->streams)
				{
					RangeVar *rv = (RangeVar *) lfirst(lc);
					if (pg_strcasecmp(rv->relname, strVal(alias)) == 0 || pg_strcasecmp(rv->alias->aliasname, strVal(alias)) == 0)
					{
						rel = rv;
						break;
					}
				}
			}

			if (has_join)
			{
				if (rel)
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" has an ambiguous type", qualname),
							 errhint("Explicitly cast to the desired type. For example, %s::integer. "
									 "If \"%s\" is supposed to be a relation, create it first.", qualname, rel->relname),
							 parser_errposition(context->pstate, cref->location)));
				else
					ereport(ERROR,
							(errcode(ERRCODE_AMBIGUOUS_COLUMN),
							 errmsg("column reference \"%s\" has an ambiguous type", qualname),
							 errhint("Explicitly cast to the desired type. For example, %s::integer. "
									 "If \"%s\" is supposed to belong to a relation, create the relation first.", qualname, qualname),
							 parser_errposition(context->pstate, cref->location)));
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						errmsg("column reference \"%s\" has an ambiguous type", qualname),
						errhint("Explicitly cast to the desired type. For example, %s::integer.", qualname),
						parser_errposition(context->pstate, cref->location)));
		}
	}

	/* Ensure that any WINDOWs are legal */
	collect_windows(select, context);
	if (context->windows)
	{
		List *fromClause = NIL;

		foreach(lc, select->fromClause)
		{
			Node *n = (Node *) lfirst(lc);
			Node *newnode = replace_stream_rangevar_with_streamdesc(n, context);

			fromClause = lappend(fromClause, newnode);
		}

		transformFromClause(context->pstate, fromClause);

		foreach(lc, context->windows)
		{
			WindowDef *window = (WindowDef *) lfirst(lc);

			if (list_length(window->orderClause) > 1)
			{
				/* Can't ORDER BY multiple columns. */
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("only a single ORDER BY is allowed for WINDOWs"),
						parser_errposition(context->pstate, window->location)));
			}
			else if (list_length(window->orderClause) == 1)
			{
				SortBy *sort = (SortBy *) linitial(window->orderClause);
				Oid type = exprType(transformExpr(context->pstate, sort->node, EXPR_KIND_WHERE));
				/* ORDER BY must be on a date, timestamp or timestamptz field */
				if (!(type == 1082 || type == 1114 || type == 1184))
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("ORDER BY expression for WINDOWs must have a timestamp-like result type"),
							parser_errposition(context->pstate, sort->location)));
			}
		}
	}

	/* Ensure that the sliding window is legal */
	if (find_clock_timestamp_expr(select->whereClause, context))
		validate_clock_timestamp_expr(select, context->expr, context);

	/* Pass it to the analyzer to make sure all function definitions, etc. are correct */
	parse_analyze((Node *) select, sql, NULL, 0);
}

/*
 * transformContinuousSelectStmt
 */
void
transformContinuousSelectStmt(ParseState *pstate, SelectStmt *select)
{
	ContAnalyzeContext *context = MakeContAnalyzeContext(pstate, select);
	ListCell *lc;
	List *fromClause = NIL;

	collect_rels_and_streams((Node *) select->fromClause, context);
	collect_types_and_cols((Node *) select, context);

	foreach(lc, select->fromClause)
	{
		Node *n = (Node *) lfirst(lc);
		Node *newnode = replace_stream_rangevar_with_streamdesc(n, context);

		fromClause = lappend(fromClause, newnode);
	}

	select->fromClause = fromClause;
}

/*
 * transformStreamEntry
 *
 * Transform a StreamDesc to a RangeTblEntry
 */
RangeTblEntry *
transformStreamDesc(ParseState *pstate, StreamDesc *stream)
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
