/*-------------------------------------------------------------------------
 *
 * cont_analyze.c
 *	  Support for parsing and analyzing continuous queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_analyze.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_combine_fn.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream_fn.h"
#include "commands/pipelinecmds.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/planner.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "parser/analyze.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_agg.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/stream.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

double sliding_window_step_factor;

#define CLOCK_TIMESTAMP "clock_timestamp"
#define DATE_ROUND "date_round"
#define DATE_TRUNC_SECOND "second"
#define TO_TIMESTAMP "to_timestamp"

#define SECOND_USEC ((TimeOffset) 1000 * 1000)
#define MIN_USEC (60 * SECOND_USEC)
#define HOUR_USEC (60 * MIN_USEC)
#define INTERNAL_COLNAME_PREFIX "_"
#define IS_DISTINCT_ONLY_AGG(name) (pg_strcasecmp((name), "count") == 0 || pg_strcasecmp((name), "array_agg") == 0)

#define MIN_VIEW_MAX_AGE_FACTOR 0.5

/*
 * Maps hypothetical set aggregates to their streaming variants
 *
 * Perhaps we'll use a catalog for this one day, but this seems
 * sufficient for now, for simplicity's sake.
 */
static char *StreamingVariants[][2] = {
		{"rank", "cq_rank"},
		{"dense_rank", "hll_dense_rank"},
		{"percent_rank", "cq_percent_rank"},
		{"cume_dist", "cq_cume_dist"},
		{"count", "hll_count_distinct"},
		{"percentile_cont", "cq_percentile_cont"},
		{"array_agg", "set_agg"}
};

typedef struct ReplaceNodeContext
{
	Node *new;
	Node *old;
	Size size;
} ReplaceNodeContext;

/*
 * Replaces any node in tree that matches context.old into context.new.
 */
static bool
replace_node(Node *tree, ReplaceNodeContext *context)
{
	/*
	 * For now to be conservative we only allow replacing FuncCall and ColumnRefs.
	 */
	Assert(IsA(context->old, FuncCall) || IsA(context->old, TypeCast) || IsA(context->old, ColumnRef) ||
			IsA(context->old, Aggref) || IsA(context->old, WindowFunc) || IsA(context->old, A_Expr));
	Assert(IsA(context->new, FuncCall) || IsA(context->new, ColumnRef) || IsA(context->new, WindowFunc) ||
			IsA(context->new, FuncExpr));
	Assert(IsA(context->old, TypeCast) || IsA(context->old, ColumnRef) ? IsA(context->new, ColumnRef) : true);
	Assert(IsA(context->old, Aggref) ? IsA(context->new, WindowFunc) || IsA(context->new, FuncExpr) : true);
	Assert(IsA(context->old, WindowFunc) ? IsA(context->new, FuncExpr) : true);
	Assert(sizeof(ColumnRef) <= sizeof(A_Expr));
	Assert(sizeof(ColumnRef) <= sizeof(FuncCall));
	Assert(sizeof(ColumnRef) <= sizeof(TypeCast));
	Assert(sizeof(WindowFunc) <= sizeof(WindowFunc));
	Assert(sizeof(FuncExpr) <= sizeof(Aggref));
	Assert(sizeof(FuncExpr) <= sizeof(WindowFunc));

	if (tree == NULL)
		return false;

	if (equal(tree, context->old))
	{
		memcpy((void *) tree, (void *) context->new, context->size);
		return true;
	}

	return raw_expression_tree_walker(tree, replace_node, (void *) context);
}

/*
 * Looks up the streaming hypothetical set variant for the given function
 */
static char *
get_streaming_agg(FuncCall *fn)
{
	int i;
	int len = sizeof(StreamingVariants) / sizeof(char *) / 2;
	char *name = NameListToString(fn->funcname);

	if (IS_DISTINCT_ONLY_AGG(name) && fn->agg_distinct == false)
		return NULL;

	for (i = 0; i < len; i++)
	{
		char *k = StreamingVariants[i][0];
		char *v = StreamingVariants[i][1];

		if (pg_strcasecmp(k, name) == 0)
			return v;
	}

	return NULL;
}

/*
 * get_inv_streaming_agg
 */
char *
get_inv_streaming_agg(char *name, bool *is_distinct)
{
	int i;
	int len = sizeof(StreamingVariants) / sizeof(char *) / 2;

	for (i = 0; i < len; i++)
	{
		char *k = StreamingVariants[i][0];
		char *v = StreamingVariants[i][1];

		if (pg_strcasecmp(v, name) == 0)
		{
			if (is_distinct &&
					(pg_strcasecmp(v, "hll_count_distinct") == 0 || pg_strcasecmp(v, "set_agg") == 0))
				*is_distinct = true;

			return k;
		}
	}

	return name;
}

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

static List *
lappend_missing(List *l, char *str)
{
	ListCell *lc;
	bool exists = false;

	foreach(lc, l)
	{
		if (pg_strcasecmp(str, lfirst(lc)) == 0)
		{
			exists = true;
			break;
		}
	}

	if (!exists)
		l = lappend(l, str);

	return l;
}

static bool
collect_column_names(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		context->colnames = lappend_missing(context->colnames, res->name ? res->name : FigureColname(res->val));
	}
	else if (IsA(node, ColumnRef))
		context->colnames = lappend_missing(context->colnames, FigureColname(node));

	return raw_expression_tree_walker(node, collect_column_names, (void *) context);
}

ContAnalyzeContext *
MakeContAnalyzeContext(ParseState *pstate, SelectStmt *select, ContQueryProcType type)
{
	ContAnalyzeContext *context = palloc0(sizeof(ContAnalyzeContext));

	context->pstate = pstate;

	if (context->pstate)
		context->pstate->p_cont_view_context = context;

	/*
	 * Collect any column names being used, so we don't clobber them when generating
	 * internal column names for the materialization table.
	 */
	collect_column_names((Node *) select->targetList, context);

	context->is_sw = has_clock_timestamp(select->whereClause, NULL);
	context->proc_type = type;

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
		if (RangeVarIsForStream((RangeVar *) node, NULL))
			context->streams = lappend(context->streams, node);
		else
			context->rels = lappend(context->rels, node);

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
collect_funcs(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		context->funcs = lappend(context->funcs, node);
		return false;
	}

	return raw_expression_tree_walker(node, collect_funcs, context);
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

		if (!is_agg)
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
		windows = lappend(windows, lfirst(lc));

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
	bool lcontains, rcontains;

	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *funccall = (FuncCall *) node;
		char *name = NameListToString(funccall->funcname);
		if (pg_strcasecmp(name, CLOCK_TIMESTAMP) == 0)
		{
			context->expr = node;
			context->location = funccall->location;
			return true;
		}
	}
	else if (IsA(node, A_Expr))
	{
		A_Expr *aexpr = (A_Expr *) node;
		Node *lexpr, *rexpr;

		context->expr = NULL;
		lcontains = find_clock_timestamp_expr(aexpr->lexpr, context);
		if (lcontains)
			lexpr = context->expr;
		else
			lexpr = aexpr->lexpr;

		context->expr = NULL;
		rcontains = find_clock_timestamp_expr(aexpr->rexpr, context);
		if (rcontains)
			rexpr = context->expr;
		else
			rexpr = aexpr->rexpr;

		if (lcontains && rcontains)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("clock_timestamp() may only appear once in a WHERE clause"),
							parser_errposition(context->pstate, context->location)));

		if (aexpr->kind == AEXPR_OP)
		{
			if (lcontains || rcontains)
				context->expr = (Node *) makeA_Expr(AEXPR_OP, aexpr->name, lexpr, rexpr, -1);

			return lcontains || rcontains;
		}

		if (lcontains || rcontains)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("clock_timestamp() may only appear as a top-level conjunction predicate"),
					parser_errposition(context->pstate, context->location)));
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr *be = (BoolExpr *) node;
		ListCell *lc;
		int count = 0;
		Node *expr;

		foreach(lc, be->args)
		{
			if (find_clock_timestamp_expr((Node *) lfirst(lc), context))
			{
				if (be->boolop != AND_EXPR)
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("clock_timestamp() may only appear as a top-level conjunction predicate"),
							parser_errposition(context->pstate, context->location)));
				}
				else
				{
					expr = context->expr;
					count++;
				}
			}
		}

		if (count == 1)
		{
			context->expr = expr;
			return true;
		}
		else if (count > 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("clock_timestamp() may only appear once in a WHERE clause"),
							parser_errposition(context->pstate, context->location)));
		}
		else
		{
			return false;
		}
	}

	return raw_expression_tree_walker(node, find_clock_timestamp_expr, (void *) context);
}

static Node *
get_truncation_from_interval(SelectStmt *select, Node *time, Node *interval)
{
	Expr *expr = (Expr *) transformExpr(make_parsestate(NULL), interval, EXPR_KIND_WHERE);
	Const *c;
	Interval *window;
	FuncCall *func;
	Interval *step;
	Interval min_step;
	float8 factor = select->swStepFactor / (float8) 100.0;
	int cmp;
	char *step_str;
	A_Const *step_const;

	Assert(IsA(expr, Const));
	c = (Const *) expr;

	Assert(c->consttype == INTERVALOID);
	window = (Interval *) c->constvalue;

	MemSet(&min_step, 0, sizeof(Interval));
	min_step.time = 1000 * 1000;

	/* We make the step size some percentage of the total window width */
	step = (Interval *) DirectFunctionCall2(interval_mul, PointerGetDatum(window), Float8GetDatum(factor));
	cmp = (Datum) DirectFunctionCall2(interval_cmp, (Datum) step, (Datum) &min_step);
	if (DatumGetInt32(cmp) < 0)
		step = &min_step;

	step_str = (char *) DirectFunctionCall1(interval_out, (Datum) step);
	step_const = makeNode(A_Const);
	step_const->val = *makeString(step_str);

	func = makeNode(FuncCall);
	func->funcname = list_make1(makeString(DATE_ROUND));
	func->args = list_make2(time, step_const);

	return (Node *) func;
}

static bool
validate_window_timestamp_expr(Node *node, ContAnalyzeContext *context)
{
	Oid type = exprType(transformExpr(context->pstate, node, EXPR_KIND_WHERE));
	return TypeCategory(type) == TYPCATEGORY_DATETIME;
}

/*
 * validate_clock_timestamp_expr
 *
 * XXX(usmanm): Should some of this be put in the grammar?
 */
static void
validate_clock_timestamp_expr(SelectStmt *stmt, Node *expr, ContAnalyzeContext *context)
{
	A_Expr *aexpr = NULL;
	Node *ct_expr;
	Node *col_expr;
	FuncCall *fc;
	char *name;
	char *err = "";
	char *hint = NULL;
	int location;

	if (expr == NULL)
		return;

	if (!IsA(expr, A_Expr))
		goto error;

	aexpr = (A_Expr *) expr;
	if (aexpr->kind != AEXPR_OP)
		goto error;
	name = strVal(linitial(aexpr->name));

	if (has_clock_timestamp(aexpr->rexpr, NULL))
	{
		if (pg_strcasecmp(name, ">") != 0 && pg_strcasecmp(name, ">=") != 0)
		{
			hint = "Try switching the comparison operator to \"<\".";
			goto error;
		}

		col_expr = aexpr->lexpr;
		ct_expr = aexpr->rexpr;
	}
	else
	{
		if (pg_strcasecmp(name, "<") != 0 && pg_strcasecmp(name, "<=") != 0)
		{
			hint = "Try switching the comparison operator to \">\".";
			goto error;
		}

		col_expr = aexpr->rexpr;
		ct_expr = aexpr->lexpr;
	}

	if (!IsA(ct_expr, A_Expr))
		goto error;

	/* Validate clock_timestamp() - interval 'duration' expression */
	aexpr = (A_Expr *) ct_expr;
	if (aexpr->kind != AEXPR_OP || pg_strcasecmp("-", strVal(linitial(aexpr->name))) != 0 || !IsA(aexpr->lexpr, FuncCall))
		goto error;

	fc = (FuncCall *) aexpr->lexpr;
	if (pg_strcasecmp(NameListToString(fc->funcname), CLOCK_TIMESTAMP) != 0)
		goto error;

	context->expr = ct_expr;
	if (!validate_window_timestamp_expr(col_expr, context))
		goto error;

	return;

error:
	if (aexpr)
		location = parser_errposition(context->pstate, aexpr->location);
	else
		location = 0;

	if (hint)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg(strlen(err) ? "%s" : "sliding window expressions must look like <timestamp column> > clock_timestamp() - <interval>%s", err),
				errhint("%s", hint),
				location));

	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg(strlen(err) ? "%s" : "sliding window expressions must look like <timestamp column> > clock_timestamp() - <interval>%s", err),
			location));
}

/*
 * col_has_index
 *
 * Checks if the column of the given relation belongs to an index
 */
static bool
col_has_index(RangeVar *rv, ColumnRef *col)
{
	Relation rel = heap_openrv(rv, NoLock);
	Bitmapset *indexed = RelationGetIndexAttrBitmap(rel, INDEX_ATTR_BITMAP_ALL);
	TupleDesc desc = RelationGetDescr(rel);
	char *table;
	char *colname;
	int i;

	heap_close(rel, NoLock);
	DeconstructQualifiedName(col->fields, &table, &colname);

	for (i=0; i<desc->natts; i++)
	{
		AttrNumber attno = desc->attrs[i]->attnum - FirstLowInvalidHeapAttributeNumber;
		if (pg_strcasecmp(NameStr(desc->attrs[i]->attname), colname) == 0 &&
				bms_is_member(attno, indexed))
			return true;
	}

	return false;
}

/*
 * warn_unindexed_join
 *
 * Check for any stream-table joins on unindexed table columns. If we find any,
 * log a NOTICE that communicates the potential performance implications.
 */
static void
warn_unindexed_join(SelectStmt *stmt, ContAnalyzeContext *context)
{
	ListCell *lc;
	ContAnalyzeContext local;
	List *set = NIL;

	MemSet(&local, 0, sizeof(ContAnalyzeContext));

	collect_types_and_cols((Node *) stmt->fromClause, &local);
	collect_types_and_cols((Node *) stmt->whereClause, &local);

	/* we only want to log one notice per column, so de-dup */
	foreach(lc, local.cols)
	{
		Node *n = (Node *) lfirst(lc);
		ListCell *l;
		bool unique = true;

		foreach(l, set)
		{
			if (equal((Node *) lfirst(l), n))
			{
				unique = false;
				break;
			}
		}

		if (unique)
			set = lappend(set, n);
	}

	foreach(lc, set)
	{
		ListCell *rc;
		ColumnRef *col = (ColumnRef *) lfirst(lc);

		foreach(rc, context->rels)
		{
			RangeVar *rv = (RangeVar *) lfirst(rc);
			char *table;
			char *colname;
			char *aliased = rv->alias ? rv->alias->aliasname : rv->relname;

			DeconstructQualifiedName(col->fields, &table, &colname);
			if (table == NULL || pg_strcasecmp(table, aliased) != 0)
				continue;

			/* log the notice if the column doesn't have an index */
			if (!col_has_index(rv, col))
				elog(NOTICE, "consider creating an index on %s for improved stream-table join performance", NameListToString(col->fields));
		}
	}
}

static bool
has_relname(RangeVar *rv, char *relname)
{
	return pg_strcasecmp(rv->relname, relname) == 0 || (rv->alias && pg_strcasecmp(rv->alias->aliasname, relname) == 0);
}

static bool
make_selects_continuous(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, SelectStmt))
		((SelectStmt *) node)->forContinuousView = true;

	return raw_expression_tree_walker(node, make_selects_continuous, NULL);
}

/*
 * MakeSelectsContinuous
 *
 * Mark all SELECT queries as continuous. This is necessary for subqueries to be recognized
 * as continuous, as the grammar can't determine that.
 */
void
MakeSelectsContinuous(SelectStmt *stmt)
{
	make_selects_continuous((Node *) stmt, NULL);
}

/*
 * is_allowed_subquery
 *
 * Check if the given query is a valid subquery for a continuous view
 */
void
ValidateSubselect(Node *subquery, char *objdesc)
{
	SelectStmt *stmt;
	Query *q;

	/* the grammar should handle this one, but just to be safe... */
	if (!IsA(subquery, SelectStmt))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s must be SELECT statements", objdesc)));

	stmt = (SelectStmt *) copyObject(subquery);

	q = parse_analyze((Node *) stmt, "subquery", NULL, 0);

	if (q->hasAggs)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain aggregates", objdesc)));

	if (q->hasWindowFuncs)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain window functions", objdesc)));

	if (q->groupClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain GROUP BY clauses", objdesc)));

	if (q->havingQual)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain HAVING clauses", objdesc)));

	if (q->sortClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain ORDER BY clauses", objdesc)));

	if (q->distinctClause)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain DISTINCT clauses", objdesc)));

	if (q->limitOffset || q->limitCount)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain LIMIT clauses", objdesc)));

	if (q->hasForUpdate)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain FOR UPDATE clauses", objdesc)));

	if (q->cteList)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s cannot contain CTEs", objdesc)));
}

/*
 * validate_target_list
 */
static void
validate_target_list(SelectStmt *stmt)
{
	ListCell *lc;
	foreach(lc, stmt->targetList)
	{
		ResTarget *res = (ResTarget *) lfirst(lc);

		if (IsA(res->val, SubLink))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("subqueries are not allowed in a continuous view's target list"),
					errhint("Stream-table joins are the preferred method for combining other relations with continuous views.")));
		}
		else if (res->name && pg_strcasecmp(res->name, ARRIVAL_TIMESTAMP) == 0)
		{
			/* we don't allow aliasing other columns as arrival_timestamp */
			Node *n = (Node *) res->val;
			if (!IsA(n, ColumnRef) || pg_strcasecmp(FigureColname(n), ARRIVAL_TIMESTAMP) != 0)
				elog(ERROR, "arrival_timestamp is a reserved column name");
		}

	}
}

/*
 * ValidateContQuery
 */
void
ValidateContQuery(RangeVar *name, Node *node, const char *sql)
{
	SelectStmt *select;
	ContAnalyzeContext *context;
	ListCell *lc;
	RangeVar *stream;
	bool is_inferred;

	if (!IsA(node, SelectStmt))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("continuous views can only be defined using SELECT queries")));

	select = (SelectStmt *) copyObject(node);
	validate_target_list(select);

	context = MakeContAnalyzeContext(make_parsestate(NULL), select, WORKER);
	context->pstate->p_sourcetext = sql;

	/* No support for CTEs */
	if (select->withClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries don't support CTEs"),
				errhint("Try using sub-SELECTs instead."),
				parser_errposition(context->pstate, select->withClause->location)));

	/* No support for HAVING clause */
	if (select->havingClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries don't support HAVING clauses"),
				errhint("Try using a WHERE clause on the continuous view instead.")));

	/* recurse for any sub-SELECTs */
	if (list_length(select->fromClause) == 1 && IsA(linitial(select->fromClause), RangeSubselect))
	{
		RangeSubselect *sub = (RangeSubselect *) linitial(select->fromClause);

		ValidateSubselect(sub->subquery, "subqueries in continuous views");
		ValidateContQuery(name, sub->subquery, sql);
		return;
	}

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

	/* Ensure that we're reading from a stream */
	if (list_length(context->streams) == 0)
	{
		if (context->rels)
		{
			RangeVar *t = (RangeVar *) linitial(context->rels);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("continuous queries must include a stream in the FROM clause"),
							errhint("To include a relation in a continuous query, JOIN it with a stream."),
							parser_errposition(context->pstate, t->location)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("continuous queries must include a stream in the FROM clause")));
	}

	/* Ensure we're reading from at most one stream */
	if (list_length(context->streams) > 1)
	{
		RangeVar *rv = (RangeVar *) lsecond(context->streams);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries don't support stream-stream JOINs"),
				errhint("If %s is supposed to be a relation, create it first with CREATE TABLE.",
						rv->alias ? rv->alias->aliasname : rv->relname),
				parser_errposition(context->pstate, rv->location)));
	}

	stream = (RangeVar *) linitial(context->streams);

	if (equal(stream, name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries cannot read from themselves"),
				errhint("Remove \"%s\" from the FROM clause.", stream->relname),
				parser_errposition(context->pstate, stream->location)));

	/*
	 * Ensure that we have no `*` in the target list.
	 *
	 * We can't SELECT * from streams because we don't know the schema of
	 * streams ahead of time.
	 *
	 * The decision to disallow wild cards for relations & static streams was taken
	 * because they can be ALTERed in the future which would require us to
	 * ALTER the CQ's underlying materialization table and the VIEW. This can probably
	 * be accomplished by triggers, but lets just punt on this for now.
	 */
	foreach(lc, context->cols)
	{
		ColumnRef *cref = lfirst(lc);
		char *qualname = NameListToString(cref->fields);

		if (IsA(lfirst(cref->fields->tail), A_Star))
		{
			ereport(ERROR,
					(errcode(ERRCODE_AMBIGUOUS_COLUMN),
					 errmsg("can't select \"%s\" in continuous queries", qualname),
					 errhint("Explicitly state the columns you want to SELECT."),
					 parser_errposition(context->pstate, cref->location)));
		}
	}

	RangeVarIsForStream(stream, &is_inferred);

	/* Ensure that each column being read from an inferred stream is type-casted */
	if (is_inferred)
	{
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
			 * If the column is for a relation, we don't need any explicit type case.
			 */
			if (list_length(cref->fields) == 2)
			{
				char *col_relname = strVal(linitial(cref->fields));

				foreach(lc2, context->rels)
				{
					RangeVar *rv = (RangeVar *) lfirst(lc2);

					if (has_relname(rv, col_relname))
					{
						needs_type = false;
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
				char *name = res->name ? res->name : FigureColname(res->val);

				if (contains_node((Node *) res, (Node *) cref))
				{
					needs_type = true;
					break;
				}

				if (pg_strcasecmp(qualname, name) == 0)
					refs_target = true;
			}

			/* If it doesn't reference a target or is contained in a target, it needs a type */
			if (needs_type || !refs_target)
				ereport(ERROR,
						(errcode(ERRCODE_AMBIGUOUS_COLUMN),
						errmsg("column reference \"%s\" has an ambiguous type", qualname),
						errhint("Explicitly cast to the desired type, e.g. %s::integer, or create the stream \"%s\" with CREATE STREAM.",
								qualname, stream->relname),
						parser_errposition(context->pstate, cref->location)));
		}
	}

	/* Transform the fromClause because we do a little bit of expression type inference below */
	context->pstate->p_no_locking = true;
	transformFromClause(context->pstate, copyObject(select->fromClause));

	/* Validate aggregate functions */
	foreach(lc, context->funcs)
	{
		FuncCall *func = (FuncCall *) lfirst(lc);
		char *name = NameListToString(func->funcname);


		/* mode and xmlagg are not supported */
		if (pg_strcasecmp(name, "mode") == 0 ||
				pg_strcasecmp(name, "xmlagg") == 0 ||
				pg_strcasecmp(name, "percentile_disc") == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("continuous queries don't support \"%s\" aggregate", name),
					parser_errposition(context->pstate, func->location)));

		if (func->agg_distinct && !IS_DISTINCT_ONLY_AGG(name))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("continuous queries don't support DISTINCT expressions for \"%s\" aggregate", name),
					parser_errposition(context->pstate, func->location)));
	}

	/* Ensure that any WINDOWs are legal */
	collect_windows(select, context);
	if (list_length(context->windows))
	{
		WindowDef *window = (WindowDef *) linitial(context->windows);

		/*
		 * All WINDOW definitions must be *equivalent*. We allow different framing, but ordering and
		 * partitioning must be identical.
		 */
		if (list_length(context->windows) > 1)
		{
			int i;

			for (i = 1; i < list_length(context->windows); i++)
			{
				WindowDef *window2 = (WindowDef *) list_nth(context->windows, i);
				if (!equal(window->partitionClause, window2->partitionClause))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("all WINDOWs in a continuous query must have identical PARTITION BY clauses"),
							parser_errposition(context->pstate, window2->location)));
				if (!equal(window->orderClause, window2->orderClause))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("all WINDOWs in a continuous query must have identical ORDER BY clauses"),
							parser_errposition(context->pstate, window2->location)));
			}
		}

		if (list_length(window->orderClause) > 1)
		{
			/* Can't ORDER BY multiple columns. */
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("continuous queries allow only a single ORDER BY expression in WINDOWs"),
					parser_errposition(context->pstate, window->location)));
		}
		else if (list_length(window->orderClause) == 1)
		{
			SortBy *sort = (SortBy *) linitial(window->orderClause);

			if (!validate_window_timestamp_expr(sort->node, context))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("ORDER BY expression for WINDOWs must evaluate to datetime type category values"),
						parser_errposition(context->pstate, sort->location)));
		}
	}

	/* Ensure that the sliding window is legal */
	context->expr = NULL;
	if (find_clock_timestamp_expr(select->whereClause, context))
		validate_clock_timestamp_expr(select, context->expr, context);

	/* DISTINCT now allowed for windows */
	if ((context->is_sw || context->windows) && select->distinctClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries don't allow a DISTINCT clause with windows")));

	/* Pass it to the analyzer to make sure all function definitions, etc. are correct */
	parse_analyze(copyObject(node), sql, NULL, 0);

	/* Warn the user if it's a stream-table join with an unindexed join qual */
	warn_unindexed_join(select, context);
}

/*
 * parserGetStreamDescr
 *
 * Get a parse-time tuple descriptor for the given stream
 */
TupleDesc
parserGetStreamDescr(Oid relid, ContAnalyzeContext *context)
{
	List *attrs = NIL;
	ListCell *lc;
	TupleDesc tupdesc;
	bool onestream = list_length(context->streams) == 1 && list_length(context->rels) == 0;
	AttrNumber attnum = 1;
	bool saw_atime = false;
	Form_pg_attribute *attrsArray;
	int i;
	RangeVar *rv = (RangeVar *) linitial(context->streams); /* there is always one stream */
	MemoryContext old;

	foreach(lc, context->types)
	{
		TypeCast *tc = (TypeCast *) lfirst(lc);
		ColumnRef *ref = (ColumnRef *) tc->arg;
		Form_pg_attribute attr;
		char *colname;
		bool already_exists = false;
		ListCell *lc;
		Type type;
		int32 typemod;

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
				already_exists = true;
				break;
			}
		}

		if (already_exists)
			continue;

		if (pg_strcasecmp(colname, ARRIVAL_TIMESTAMP) == 0)
			saw_atime = true;

		type = typenameType(NULL, tc->typeName, &typemod);

		attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attndims = list_length(tc->typeName->arrayBounds);
		attr->attnum = attnum++;
		attr->atttypid = typeTypeId(type);
		attr->atttypmod = typemod;
		attr->attcollation = typeTypeCollation(type);

		namestrcpy(&attr->attname, colname);
		attrs = lappend(attrs, attr);

		ReleaseSysCache((HeapTuple) type);
	}

	/* Relcache entries are expected to live in CacheMemoryContext */
	old = MemoryContextSwitchTo(CacheMemoryContext);

	if (!saw_atime)
	{
		Form_pg_attribute attr = (Form_pg_attribute) palloc(sizeof(FormData_pg_attribute));
		attr->attnum = attnum++;
		attr->atttypid = TIMESTAMPTZOID;
		attr->attndims = 1;
		attr->atttypmod = -1;
		attr->attcollation = InvalidOid;
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
		TupleDescInitEntry(tupdesc, attr->attnum, NameStr(attr->attname),
				attr->atttypid, InvalidOid, attr->attndims);
	}

	MemoryContextSwitchTo(old);

	return tupdesc;
}

/*
 * transformContSelectStmt
 */
void
transformContSelectStmt(ParseState *pstate, SelectStmt *select)
{
	ContQueryProcType ptype = IsContQueryCombinerProcess() ? COMBINER : WORKER;
	ContAnalyzeContext *context = MakeContAnalyzeContext(pstate, select, ptype);

	collect_rels_and_streams((Node *) select->fromClause, context);
	collect_types_and_cols((Node *) select, context);

	pstate->p_cont_view_context = context;
}

/*
 * apply_transout
 *
 * Wrap the given aggregate in its transition out function, in place
 */
static void
apply_transout(Expr *expr, Oid aggoid)
{
	Oid combinefn;
	Oid transoutfn;
	Oid combineinfn;
	Oid statetype;
	FuncExpr *func;
	Expr *arg;
	ReplaceNodeContext context;

	Assert(IsA(expr, Aggref) || IsA(expr, WindowFunc));

	GetCombineInfo(aggoid, &combinefn, &transoutfn, &combineinfn, &statetype);

	if (!OidIsValid(transoutfn))
		return;

	arg = copyObject(expr);

	func = makeNode(FuncExpr);
	func->funcid = transoutfn;
	func->funcresulttype = statetype;
	func->args = list_make1(arg);

	context.old = (Node *) expr;
	context.new = (Node *) func;
	context.size = sizeof(FuncExpr);

	replace_node((Node *) expr, &context);
}

static bool
pull_var_and_aggs_walker(Node *node, List **varlist)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var) || IsA(node, Aggref) || IsA(node, WindowFunc) || IsA(node, PlaceHolderVar))
	{
		*varlist = lappend(*varlist, node);
		return false;
	}
	return expression_tree_walker(node, pull_var_and_aggs_walker,
								  (void *) varlist);
}

static List *
pull_var_and_aggs(Node *node)
{
	List *varlist = NIL;
	pull_var_and_aggs_walker(node, &varlist);
	return varlist;
}

static Oid
get_combine_state_type(Expr *expr)
{
	Oid result = InvalidOid;

	if (IsA(expr, Aggref))
		result = GetCombineStateType(((Aggref *) expr)->aggfnoid);
	else if (IsA(expr, WindowFunc))
		result = GetCombineStateType(((WindowFunc *) expr)->winfnoid);

	return result;
}

/*
 * ApplyTransitionOut
 *
 * Apply transition out functions to all relevant aggregates in the given list of nodes
 */
void
ApplyTransitionOut(List *nodes)
{
	ListCell *lc;
	foreach(lc, nodes)
	{
		Expr *expr = (Expr *) lfirst(lc);
		if (IsA(expr, TargetEntry))
			expr = ((TargetEntry *) expr)->expr;

		if (IsA(expr, Aggref))
		{
			Aggref *agg = (Aggref *) expr;
			Oid type = get_combine_state_type((Expr *) agg);

			if (OidIsValid(type))
				apply_transout((Expr *) agg, agg->aggfnoid);
		}
		else if (IsA(expr, WindowFunc))
		{
			WindowFunc *win = (WindowFunc *) expr;
			Oid type = get_combine_state_type((Expr *) win);

			if (OidIsValid(type))
				apply_transout((Expr *) win, win->winfnoid);
		}
	}
}

/*
 * transformContSelectTargetList
 */
List *
transformContSelectTargetList(ParseState *pstate, List *tlist)
{
	ListCell *lc;
	List *nodes = pull_var_and_aggs((Node *) tlist);

	Assert(pstate->p_cont_view_context);

	foreach(lc, nodes)
	{
		Node *n = lfirst(lc);

		if (IsA(n, Aggref))
		{
			Aggref *agg = (Aggref *) n;
			Oid type = get_combine_state_type((Expr *) agg);

			agg->aggfinaltype = agg->aggtype;

			if (OidIsValid(type))
			{
				agg->aggtype = type;

				/*
				 * We only want to do this for a running CQ proc, otherwise it
				 * will confuse the analyzer when creating a CV because the deparsed
				 * CV's aggregate types will not have been changed. This can cause a
				 * signature resolution error when resolving the transout function.
				 */
				if (IsContQueryProcess())
					apply_transout((Expr *) agg, agg->aggfnoid);
			}
		}
		else if (IsA(n, WindowFunc))
		{
			WindowFunc *win = (WindowFunc *) n;
			Oid type = get_combine_state_type((Expr *) win);

			win->winfinaltype = win->wintype;

			if (OidIsValid(type))
			{
				win->wintype = type;

				/*
				 * We only want to do this for a running CQ proc, otherwise it
				 * will confuse the analyzer when creating a CV because the deparsed
				 * CV's aggregate types will not have been changed. This can cause a
				 * signature resolution error when resolving the transout function.
				 */
				if (IsContQueryProcess())
					apply_transout((Expr *) win, win->winfnoid);
			}
		}
	}

	return tlist;
}

/*
 * transformCreateStreamStmt
 */
void
transformCreateStreamStmt(CreateStreamStmt *stmt)
{
	ListCell *lc;
	bool saw_atime = false;

	foreach(lc, stmt->ft.base.tableElts)
	{
		ColumnDef *coldef = (ColumnDef *) lfirst(lc);
		if (pg_strcasecmp(coldef->colname, ARRIVAL_TIMESTAMP) == 0)
		{
			saw_atime = true;
			break;
		}
	}

	if (!saw_atime)
	{
		ColumnDef *coldef;
		TypeName *typename;

		typename = makeNode(TypeName);
		typename->typeOid = TIMESTAMPTZOID;
		typename->typemod = InvalidOid;

		coldef = makeNode(ColumnDef);
		coldef->colname = ARRIVAL_TIMESTAMP;
		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->constraints = NIL;
		coldef->typeName = typename;

		stmt->ft.base.tableElts = lappend(stmt->ft.base.tableElts, coldef);
	}
}

static bool
create_inferred_streams(Node *node, void *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeVar))
	{
		RangeVar *rv = (RangeVar *) node;
		Oid relid = RangeVarGetRelid(rv, NoLock, true);

		if (!OidIsValid(relid))
			CreateInferredStream(rv);

		return false;
	}

	return raw_expression_tree_walker(node, create_inferred_streams, context);
}

/*
 * CreateInferredStreams
 */
void
CreateInferredStreams(SelectStmt *stmt)
{
	create_inferred_streams(copyObject(stmt->fromClause), NULL);
}

/*
 * rewrite_streaming_aggs
 *
 * Replaces hypothetical set aggregate functions with their streaming
 * variants if possible, since we can't use the sorting approach that
 * the standard functions use.
 */
static void
rewrite_streaming_aggs(SelectStmt *stmt, ContAnalyzeContext *context)
{
	ListCell *lc;

	foreach(lc, stmt->targetList)
	{
		ResTarget *res = (ResTarget *) lfirst(lc);
		ListCell *fnlc;
		char *prevname = NULL;
		char *newname = NULL;
		char *resname = FigureColname((Node *) res);

		if (context->funcs)
			list_free(context->funcs);
		context->funcs = NIL;
		collect_agg_funcs((Node *) res, context);

		/*
		 * For each agg in this ResTarget, replace any function names
		 * with the names of their streaming variants.
		 */
		foreach(fnlc, context->funcs)
		{
			FuncCall *fn = (FuncCall *) lfirst(fnlc);

			prevname = NameListToString(fn->funcname);
			newname = get_streaming_agg(fn);
			if (newname != NULL)
			{
				fn->funcname = list_make1(makeString(newname));
				fn->agg_distinct = false;
			}
		}

		/*
		 * If we didn't replace any agg, or the target is named, or has a different name than
		 * the previous function name, we're done.
		 */
		if (newname == NULL || res->name != NULL || pg_strcasecmp(prevname, resname) != 0)
			continue;

		/*
		 * Force name the target to the previous function's name.
		 */
		res->name = prevname;
	}
}

static bool
is_res_target_for_node(Node *rt, Node *node)
{
	if (rt == NULL || node == NULL)
		return false;

	if (equal(rt, node))
		return true;

	if (IsA(rt, ResTarget))
	{
		ResTarget *res = (ResTarget *) rt;

		if (is_res_target_for_node(res->val, node))
			return true;

		if (res->name != NULL && pg_strcasecmp(res->name, FigureColname((Node *) node)) == 0)
		{
			/*
			 * Is this ResTarget overriding a node it references?
			 * Example: substring(key::text, 1, 2) AS key
			 */
			if (contains_node(rt, node))
				return false;
			return true;
		}
	}

	return false;
}

static bool
node_for_cref(Node *node)
{
	while (node)
	{
		if (IsA(node, ColumnRef))
			return true;

		if (IsA(node, TypeCast))
		{
			TypeCast *tc = (TypeCast *) node;
			node = tc->arg;
			continue;
		}

		return false;
	}

	return false;
}

static ResTarget *
find_node_in_target_list(List *tlist, Node *node)
{
	ListCell *lc;

	foreach(lc, tlist)
	{
		ResTarget *res = lfirst(lc);

		if (equal(res->val, node))
			return res;
	}

	foreach(lc, tlist)
	{
		ResTarget *res = lfirst(lc);

		if (res->name != NULL && node_for_cref(node) && pg_strcasecmp(res->name, FigureColname((Node *) node)) == 0)
		{
			/*
			 * Is this ResTarget overriding a node it references?
			 * Example: substring(key::text, 1, 2) AS key
			 */
			if (contains_node((Node *) res, node))
				continue;
			return res;
		}
	}

	return NULL;
}

static char *
get_unique_colname(ContAnalyzeContext *context)
{
	StringInfoData colname;

	/* If hoisted name isn't used already, return that */
	if (context->hoisted_name)
	{
		ListCell *lc;
		bool exists = false;

		foreach(lc, context->colnames)
		{
			char *colname2 = lfirst(lc);
			if (pg_strcasecmp(context->hoisted_name, colname2) == 0)
			{
				exists = true;
				break;
			}
		}

		if (!exists)
			return context->hoisted_name;
	}

	initStringInfo(&colname);

	while (true)
	{
		ListCell *lc;
		bool exists = false;

		appendStringInfo(&colname, "%s%d", INTERNAL_COLNAME_PREFIX, context->colno);
		context->colno++;

		foreach(lc, context->colnames)
		{
			char *colname2 = lfirst(lc);
			if (pg_strcasecmp(colname.data, colname2) == 0)
			{
				exists = true;
				break;
			}
		}

		if (!exists)
			break;

		resetStringInfo(&colname);
	}

	context->colnames = lappend(context->colnames, colname.data);

	return colname.data;
}

static ResTarget *
create_unique_res_target(Node *node, ContAnalyzeContext *context)
{
	/*
	 * Any new node we add to the targetList should have a unique
	 * name. This isn't always necessary, but its always safe.
	 */
	ResTarget *res = makeNode(ResTarget);
	res->name = get_unique_colname(context);
	res->val = node;

	return res;
}

/*
 * Create a column reference for a target list entry by using its explicit
 * or implicit name.
 */
static ColumnRef *
create_colref_for_res_target(ResTarget *rt)
{
	ColumnRef *cref;
	char *name = rt->name;

	if (name == NULL)
		name = FigureColname(rt->val);

	cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(name));
	return cref;
}

/*
 * Create a ResTarget with node as the val. Explicitly name the target
 * if name is not NULL.
 */
static ResTarget *
create_res_target_for_node(Node *node, char *name)
{
	ResTarget *res = makeNode(ResTarget);
	if (name == NULL)
		name = FigureColname(node);
	res->name = name;
	res->val = node;
	return res;
}

/*
 * apply_combine
 *
 * Apply an aggregate combine() to the given ResTarget
 */
static Node *
apply_combine(ResTarget *rt)
{
	FuncCall *combine = makeNode(FuncCall);

	rt = create_res_target_for_node((Node *) combine, rt->name);

	combine->args = list_make1(create_colref_for_res_target(rt));
	combine->funcname = list_make1(makeString(MATREL_COMBINE));

	return (Node *) combine;
}

/*
 * select_from_matrel
 *
 * Adjust the given target list's ResTargets to select from matrel columns
 * as well as combine any aggregate calls.
 */
static List *
select_from_matrel(List *target_list)
{
	ListCell *lc;
	List *result = NIL;

	foreach(lc, target_list)
	{
		ResTarget *rt = (ResTarget *) lfirst(lc);
		ContAnalyzeContext context;
		ResTarget *matrel_res = create_res_target_for_node((Node *) create_colref_for_res_target(rt), rt->name);

		if (!IsA(rt->val, FuncCall))
		{
			result = lappend(result, matrel_res);
			continue;
		}

		MemSet(&context, 0, sizeof(ContAnalyzeContext));
		collect_agg_funcs(rt->val, &context);

		if (context.funcs == NIL)
		{
			result = lappend(result, matrel_res);
			continue;
		}

		Assert(list_length(context.funcs) == 1);

		matrel_res->val = apply_combine(matrel_res);
		result = lappend(result, matrel_res);
	}

	return result;
}

/*
 * Returns a column reference for the target list entry pointing to node. If the node
 * doesn't exist in the targetList, a new entry with a unique name is created for it.
 */
static ColumnRef *
hoist_node(List **target_list, Node *node, ContAnalyzeContext *context)
{
	ResTarget *rt = find_node_in_target_list(*target_list, node);

	if (rt == NULL)
	{
		rt = create_unique_res_target(node, context);
		*target_list = lappend(*target_list, rt);
	}

	return create_colref_for_res_target(rt);
}

static Node *
create_agg_node_for_view_overlay(ColumnRef *cref, FuncCall *workeragg, ContAnalyzeContext *context)
{
	FuncCall *finalize = makeNode(FuncCall);
	finalize->funcname = list_make1(makeString(MATREL_FINALIZE));

	if (context->view_combines)
	{
		FuncCall *combine = makeNode(FuncCall);
		combine->funcname = list_make1(makeString(MATREL_COMBINE));
		combine->args = list_make1(cref);
		/* Copy over any OVER clause from the worker. */
		combine->over = workeragg->over;

		finalize->args = list_make1(combine);
	}
	else
		finalize->args = list_make1(cref);

	return (Node *) finalize;
}

/*
 * Explicitly name all unnamed targets.
 */
static void
name_res_targets(List *tlist)
{
	ListCell *lc;
	foreach(lc, tlist)
	{
		ResTarget *res = (ResTarget *) lfirst(lc);
		if (res->name == NULL)
			res->name = FigureColname(res->val);
	}
}

/*
 * Hoistable columns include top-level columns and columns in expressions.
 * Function calls will either produce a value that is read as a simple column, or
 * a transition state, so any columns being referenced within a function call are only
 * needed by the worker.
 *
 * Any OVER clauses that are present have their expressions hoisted outside of this.
 */
static bool
collect_hoistable_cols(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
		return false;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;

		if (IsA(tc->arg, ColumnRef))
			context->types = lappend(context->types, tc);
	}

	if (IsA(node, ColumnRef))
	{
		context->cols = lappend(context->cols, node);
		return false;
	}

	return raw_expression_tree_walker(node, collect_hoistable_cols, context);
}

/*
 * Transform an implicit distinctClause to an explicit one.
 *
 *   SELECT DISTINCT x, y, ... => SELECT DISTINCT ON (x, y, ...) x, y, ...
 */
static void
make_distincts_explicit(SelectStmt *stmt, ContAnalyzeContext *context)
{
	if (equal(stmt->distinctClause, lcons(NIL, NIL)))
	{
		ListCell *lc;

		list_free(stmt->distinctClause);
		stmt->distinctClause = NIL;

		foreach(lc, stmt->targetList)
		{
			ResTarget *res = (ResTarget *) lfirst(lc);
			stmt->distinctClause = lappend(stmt->distinctClause, hoist_node(&stmt->targetList, res->val, context));
		}
	}
}

static void
truncate_timestamp_field(SelectStmt *select, Node *time, A_Expr *sw_expr, ContAnalyzeContext *context)
{
	context->funcs = NIL;
	collect_funcs(time, context);

	/* For sliding windows we try to keep around a certain "fan in" threshold */
	if (sw_expr)
	{
		A_Expr *ct_expr;

		if (equal(sw_expr->lexpr, time))
			ct_expr = (A_Expr *) sw_expr->rexpr;
		else
			ct_expr = (A_Expr *) sw_expr->lexpr;

		time = get_truncation_from_interval(select, time, ct_expr->rexpr);
	}
	else
	{
		FuncCall *func = makeNode(FuncCall);
		func->args = list_make1(copyObject(time));
		func->funcname = list_make1(makeString(DATE_TRUNC_SECOND));
		time = (Node *) func;
	}

	context->expr = time;
}

static ColumnRef *
hoist_time_node(SelectStmt *proc, Node *time, A_Expr *sw_expr, ContAnalyzeContext *context)
{
	ListCell *lc;
	bool found = false;
	ColumnRef *cref;
	List *groupClause;

	truncate_timestamp_field(proc, time, sw_expr, context);

	/* Replace timestamp column with the truncated expr */
	foreach(lc, proc->targetList)
	{
		ResTarget *res = lfirst(lc);
		if (equal(res->val, time))
			res->val = context->expr;
	}

	groupClause = NIL;
	foreach(lc, proc->groupClause)
	{
		Node *node = lfirst(lc);
		if (equal(node, time))
			groupClause = lappend(groupClause, context->expr);
		else
			groupClause = lappend(groupClause, node);
	}

	proc->groupClause = groupClause;

	time = context->expr;
	cref = hoist_node(&proc->targetList, time, context);

	foreach(lc, proc->groupClause)
	{
		Node *n = lfirst(lc);
		if (equal(n, time))
		{
			found = true;
			break;
		}
	}

	if (!found)
	{
		if (context->proc_type == COMBINER)
			proc->groupClause = lappend(proc->groupClause, cref);
		else
			proc->groupClause = lappend(proc->groupClause, time);
	}

	return cref;
}

static void
proj_and_group_for_windows(SelectStmt *proc, SelectStmt *view, ContAnalyzeContext *context)
{
	Node *time;
	A_Expr *sw_expr = NULL;
	ListCell *lc;
	ColumnRef *cref;

	if (!context->is_sw)
	{
		WindowDef *window = (WindowDef *) linitial(context->windows);
		ListCell *lc;
		List *partitionClause = NIL;

		/*
		 * All WINDOWs share the same PARTITION BY clause, so we only need to do hoisting
		 * for the first one.
		 */
		foreach(lc, window->partitionClause)
		{
			Node *node = (Node *) lfirst(lc);
			ColumnRef *cref = hoist_node(&proc->targetList, node, context);
			ListCell *lc2;
			bool found = false;

			foreach(lc2, proc->groupClause)
			{
				if (equal(node, lfirst(lc2)))
				{
					found = true;
					break;
				}
			}

			if (!found)
			{
				if (context->proc_type == COMBINER)
					proc->groupClause = lappend(proc->groupClause, cref);
				else
					proc->groupClause = lappend(proc->groupClause, node);
			}

			partitionClause = lappend(partitionClause, cref);
		}

		/*
		 * Since WINDOWs are applied to the view overlay, we replace their paritionClause
		 * with one that references the appropriate columns in the matrel.
		 */
		foreach(lc, context->windows)
		{
			window = (WindowDef *) lfirst(lc);
			window->partitionClause = partitionClause;
		}

		/* No ORDER BY? Add an explicit ORDER BY over arrival_timestamp. */
		if (list_length(window->orderClause) == 0)
		{
			ColumnRef *cref = makeNode(ColumnRef);
			SortBy *sort = makeNode(SortBy);
			RangeVar *stream = (RangeVar *) linitial(context->streams);

			if (stream->alias)
				cref->fields = list_make2(makeString(stream->alias->aliasname), makeString(ARRIVAL_TIMESTAMP));
			else if (stream->schemaname)
				cref->fields = list_make3(makeString(stream->schemaname), makeString(stream->relname), makeString(ARRIVAL_TIMESTAMP));
			else
				cref->fields = list_make2(makeString(stream->relname), makeString(ARRIVAL_TIMESTAMP));

			sort->node = (Node *) cref;

			window->orderClause = list_make1(sort);
		}

		Assert(list_length(window->orderClause) == 1);

		time = ((SortBy *) linitial(window->orderClause))->node;
	}
	else
	{
		context->expr = NULL;
		find_clock_timestamp_expr(proc->whereClause, context);

		Assert(context->expr && IsA(context->expr, A_Expr));

		sw_expr = (A_Expr *) context->expr;

		if (has_clock_timestamp(sw_expr->lexpr, NULL))
			time = sw_expr->rexpr;
		else
			time = sw_expr->lexpr;

		time = copyObject(time);
	}

	context->cols = NIL;
	collect_types_and_cols(time, context);

	Assert(list_length(context->cols) == 1);

	/* Create and truncate projection and grouping on temporal expression */
	context->hoisted_name = FigureColname(linitial(context->cols));
	if (context->view_combines)
		cref = hoist_time_node(proc, time, sw_expr, context);
	else
		cref = hoist_node(&proc->targetList, time, context);

	context->hoisted_name = NULL;

	if (context->is_sw)
	{
		ReplaceNodeContext cxt;

		view->whereClause = copyObject(sw_expr);

		cxt.old = time;
		cxt.new = (Node *) cref;
		cxt.size = sizeof(ColumnRef);

		replace_node(view->whereClause, &cxt);
	}

	/* Change all ORDER BY for WINDOWs to point to the matrel truncated timestamp column */
	foreach(lc, context->windows)
	{
		WindowDef *w = lfirst(lc);
		SortBy *sort = makeNode(SortBy);
		sort->node = (Node *) cref;
		w->orderClause = list_make1(sort);
	}
}

/*
 * TransformSelectStmtForContProc
 */
SelectStmt *
TransformSelectStmtForContProcess(RangeVar *mat_relation, SelectStmt *stmt, SelectStmt **viewptr, ContQueryProcType proc_type)
{
	ContAnalyzeContext *context;
	SelectStmt *proc;
	SelectStmt *view;
	int tl_len;
	int grp_len;
	List *tmp_list;
	ListCell *lc;
	int i;

	Assert(proc_type == WORKER || proc_type == COMBINER);

	proc = (SelectStmt *) copyObject(stmt);
	view = makeNode(SelectStmt);

	context = MakeContAnalyzeContext(make_parsestate(NULL), proc, proc_type);

	make_selects_continuous((Node *) proc, NULL);
	collect_rels_and_streams((Node *) proc->fromClause, context);
	collect_types_and_cols((Node *) proc, context);
	collect_agg_funcs((Node *) proc->targetList, context);
	collect_windows(proc, context);
	name_res_targets(proc->targetList);

	tl_len = list_length(proc->targetList);
	grp_len = list_length(proc->groupClause);

	context->stream_only = list_length(context->rels) == 0;

	/*
	 * Make the distinctClause explicit. This MUST be done before anything is modified in the
	 * statement's target list.
	 */
	make_distincts_explicit(proc, context);

	/*
	 * The view combines for WINDOWs or if there is a grouping/aggregation on a sliding window.
	 */
	context->view_combines = (list_length(context->windows) ||
			(context->is_sw && (list_length(context->funcs) || list_length(stmt->groupClause))));

	if (context->is_sw || context->view_combines)
		proj_and_group_for_windows(proc, view, context);

	/*
	 * We can't use the standard hypothetical/ordered set aggregate functions because
	 * they require sorting the input set, so replace them with their streaming variants, if possible.
	 */
	rewrite_streaming_aggs(proc, context);

	/*
	 * Hoist any nodes in the groupClause that are not being projected.
	 */
	tmp_list = NIL;
	for (i = 0; i < grp_len; i++)
	{
		Node *group_node = (Node *) list_nth(proc->groupClause, i);
		List *nodes;
		ListCell *glc;

		if (IsA(group_node, GroupingSet))
			nodes = ((GroupingSet *) group_node)->content;
		else
			nodes = list_make1(group_node);

		foreach(glc, nodes)
		{
			Node *node = (Node *) lfirst(glc);
			ColumnRef *cref = hoist_node(&proc->targetList, node, context);

			/*
			 * If the view combines, then the view will have a GROUP BY equivalent to that
			 * of the continuous query. The extra GROUP BY expression on the worker will be
			 * aggregated over in the view. The view MUST always reference the hoisted column reference
			 * of the matrel.
			 */
			if (context->view_combines)
				view->groupClause = lappend(view->groupClause, cref);

			/* The entire grouping set will be added to the worker's group clause later */
			if (IsA(group_node, GroupingSet) && proc_type == WORKER)
				continue;

			/*
			 * For workers, don't add the hoisted column because it can lead to ambiguity, for example if
			 * there is an id column in t for the query below:
			 *   CREATE CONTINUOUS VIEW v AS SELECT s.id, t.str, sum(s.val + t.val) FROM s JOIN t ON s.id = t.id GROUP BY s.id;
			 */
			if (proc_type == COMBINER)
				tmp_list = lappend(tmp_list, cref);
			else
				tmp_list = lappend(tmp_list, node);
		}

		if (IsA(group_node, GroupingSet) && proc_type == WORKER)
			tmp_list = lappend(tmp_list, group_node);
	}

	for (; i < list_length(proc->groupClause); i++)
		tmp_list = lappend(tmp_list, list_nth(proc->groupClause, i));

	proc->groupClause = tmp_list;

	/*
	 * Hoist any nodes in the distinctClause that are not being projected. The overlay view
	 * doesn't need any distinctClause.
	 */
	tmp_list = NIL;
	foreach(lc, proc->distinctClause)
	{
		Node *node = (Node *) lfirst(lc);
		ColumnRef *cref = hoist_node(&proc->targetList, node, context);
		tmp_list = lappend(tmp_list, (Node *) cref);
	}

	proc->distinctClause = tmp_list;

	/*
	 * Add all hoisted items to the target list before trying to hoist columns out of expressions. This
	 * ensures that we don't hoist the same column twice.
	 */
	tmp_list = NIL;
	for (i = tl_len; i < list_length(proc->targetList); i++)
		tmp_list = lappend(tmp_list, list_nth(proc->targetList, i));

	/*
	 * Hoist aggregates out of expressions in the target list.
	 */
	for (i = 0; i < tl_len; i++)
	{
		ResTarget *res = (ResTarget *) list_nth(proc->targetList, i);
		ResTarget *matrel_res;
		Node *res_val;
		FuncCall *agg;
		ListCell *lc;
		char *name = res->name;

		if (name == NULL)
			name = FigureColname(res->val);

		context->funcs = NIL;
		collect_agg_funcs((Node *) res, context);

		/* No aggregates? Keep as is */
		if (list_length(context->funcs) == 0)
		{
			/* combiners read from the worker's output columns, which are the same as the matrel's */
			matrel_res = create_res_target_for_node((Node *) create_colref_for_res_target(res), name);
			tmp_list = lappend(tmp_list, res);

			/* View should just reference the column from the matrel */
			view->targetList = lappend(view->targetList, matrel_res);
			continue;
		}

		agg = (FuncCall *) linitial(context->funcs);

		/* No need to re-write top level aggregates */
		if (is_res_target_for_node((Node *) res, (Node *) agg))
		{
			Node *node;

			Assert(list_length(context->funcs) == 1);

			node = create_agg_node_for_view_overlay(create_colref_for_res_target(res), agg, context);
			matrel_res = create_res_target_for_node(node, name);

			tmp_list = lappend(tmp_list, res);
			view->targetList = lappend(view->targetList, matrel_res);

			/* WINDOWs are applied to the overlay view. */
			agg->over = NULL;

			continue;
		}

		res_val = copyObject(res->val);

		/*
		 * Hoist columns out of expressions.
		 */
		context->cols = NIL;
		context->types = NIL;
		collect_hoistable_cols((Node *) res, context);

		foreach(lc, context->cols)
		{
			ColumnRef *cref = (ColumnRef *) lfirst(lc);
			TypeCast *tc = NULL;
			ListCell *lc2;
			Node *node;
			ReplaceNodeContext cxt;

			/* See if this ColumnRef was wrapped in a TypeCast */
			foreach(lc2, context->types)
			{
				TypeCast *tc2 = lfirst(lc2);

				if (equal(tc2->arg, cref))
				{
					tc = tc2;
					break;
				}
			}

			/* If it was wrapped in a TypeCast, we should check if any of the two already exist in the target list */
			if (tc)
			{
				ResTarget *rt = find_node_in_target_list(tmp_list, (Node *) tc);

				if (rt)
					node = (Node *) tc;
				else
					node = (Node *) cref;
			}
			else
				node = (Node *) cref;

			cxt.old = node;
			cxt.new = (Node *) hoist_node(&tmp_list, node, context);
			cxt.size = sizeof(ColumnRef);

			replace_node(res_val, &cxt);
		}

		/*
		 * Hoist each aggregate.
		 */
		foreach(lc, context->funcs)
		{
			FuncCall *fcall = (FuncCall *) lfirst(lc);
			ColumnRef *cref = hoist_node(&tmp_list, (Node *) fcall, context);
			ReplaceNodeContext cxt;

			cxt.old = (Node *) fcall;
			cxt.new = create_agg_node_for_view_overlay(cref, fcall, context);

			if (IsA(cxt.new, FuncCall))
				cxt.size = sizeof(FuncCall);
			else
				cxt.size = sizeof(ColumnRef);

			replace_node(res_val, &cxt);

			/* WINDOWs are applied to the overlay view only. */
			fcall->over = NULL;
		}

		/*
		 * Any expression in the target list only needs to be added to the view, and not the
		 * cont query. We have carefully replaces all aggregates and column refs in the expression
		 * node to match appropriate column refs on the matrel.
		 */
		view->targetList = lappend(view->targetList, create_res_target_for_node(res_val, name));
	}

	if (proc_type == COMBINER)
	{
		Assert(mat_relation != NULL);
		tmp_list = select_from_matrel(tmp_list);
		proc->fromClause = list_make1(mat_relation);
	}

	proc->targetList = tmp_list;

	/*
	 * Copy over the WINDOW clause from the worker statement to the
	 * view statement. These WINDOW clauses must be already modified so that
	 * they reference appropriate columns in the matrel rather than expressions
	 * from the continuous view target list.
	 */
	if (list_length(context->windows))
	{
		view->windowClause = proc->windowClause;
		proc->windowClause = NIL;
	}

	/* Copy over worker limit/offsets to the view */
	view->limitCount = proc->limitCount;
	view->limitOffset = proc->limitOffset;
	proc->limitCount = NULL;
	proc->limitOffset = NULL;

	/* Overlay view reads from matrel */
	view->fromClause = list_make1(mat_relation);

	if (proc_type == COMBINER)
	{
		/*
		 * Combiner shouldn't have to re-do the filtering work of the WHERE clause.
		 */
		proc->whereClause = NULL;

		/* Remove any FILTER clauses */
		context->funcs = NIL;
		collect_agg_funcs((Node *) proc->targetList, context);
		foreach(lc, context->funcs)
			((FuncCall *) lfirst(lc))->agg_filter = NULL;
	}

	if (viewptr)
		*viewptr = view;

	return proc;
}

static SelectStmt *
get_cont_query_select_stmt(RangeVar *rv)
{
	HeapTuple tup;
	bool isnull;
	Datum tmp;
	char *sql;
	Query *query;
	SelectStmt *select;

	tup = GetPipelineQueryTuple(rv);

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));

	sql = deparse_query_def(query);
	select = (SelectStmt *) linitial(pg_parse_query(sql));
	select->swStepFactor = query->swStepFactor;

	ReleaseSysCache(tup);

	return select;
}

/*
 * GetContViewQuery
 *
 * Returns an analyzed continuous query
 */
Query *
GetContViewQuery(RangeVar *rv)
{
	SelectStmt *sel;
	ContAnalyzeContext *context;

	sel = get_cont_query_select_stmt(rv);
	context = MakeContAnalyzeContext(NULL, sel, WORKER);
	rewrite_streaming_aggs(sel, context);

	MakeSelectsContinuous(sel);

	return parse_analyze((Node *) sel, "SELECT", 0, 0);
}

/*
 * GetContWorkerQuery
 */
Query *
GetContWorkerQuery(RangeVar *rv)
{
	SelectStmt *sel;
	RangeVar *matrel;

	matrel = GetMatRelName(rv);
	sel = get_cont_query_select_stmt(rv);
	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, WORKER);

	return parse_analyze((Node *) sel, "SELECT", 0, 0);
}

/*
 * GetContCombinerQuery
 */
Query *
GetContCombinerQuery(RangeVar *rv)
{
	SelectStmt *sel;
	RangeVar *matrel;

	matrel = GetMatRelName(rv);
	sel = get_cont_query_select_stmt(rv);
	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, COMBINER);
	return parse_analyze((Node *) sel, "SELECT", 0, 0);
}

/*
 * attr_to_agg
 *
 * Given a matrel attribute, returns the aggregate node that it corresponds to
 */
static Node *
attr_to_aggs(AttrNumber attr, List *tlist)
{
	TargetEntry *te;
	List *l;
	Node *n;

	Assert(list_length(tlist) >= attr);

	te = list_nth(tlist, attr - 1);
	l = pull_var_and_aggs((Node *) te);

	Assert(list_length(l) == 1);

	n = linitial(l);

	Assert(IsA(n, Aggref) || IsA(n, WindowFunc));

	return n;
}

/*
 * make_combine_args
 *
 * Possibly wraps the target argument in a recv function call to deserialize
 * hidden state before calling combine()
 */
static List *
make_combine_args(ParseState *pstate, Oid combineinfn, Node *arg)
{
	List *args;

	if (OidIsValid(combineinfn))
	{
		FuncCall *fn = makeNode(FuncCall);
		fn->funcname = list_make1(makeString(get_func_name(combineinfn)));
		fn->args = list_make1(arg);
		args = list_make1(transformFuncCall(pstate, fn));
	}
	else
	{
		/* transition state is stored as a first-class type, no deserialization needed */
		args = list_make1(arg);
	}

	return args;
}

/*
 * extract_agg_final_info
 *
 * Given an Aggref or WindowFunc, extracts its aggregate function, output type,
 * and final type
 */
static void
extract_agg_final_info(Node *n, Oid *aggfn, Oid *type, Oid *finaltype)
{
	if (IsA(n, Aggref))
	{
		Aggref *agg = (Aggref *) n;
		*aggfn = agg->aggfnoid;
		*type = agg->aggtype;
		*finaltype = agg->aggfinaltype;
	}
	else if (IsA(n, WindowFunc))
	{
		WindowFunc *wfunc = (WindowFunc *) n;
		*aggfn = wfunc->winfnoid;
		*type = wfunc->wintype;
		*finaltype = wfunc->winfinaltype;
	}
}

static Query *
get_worker_query_for_id(Oid id)
{
	HeapTuple tup;
	bool isnull;
	Datum tmp;
	char *sql;
	Query *query;
	SelectStmt *sel;
	RangeVar *matrel;
	Form_pipeline_query row;

	tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(id));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view with id \"%d\" does not exist", id)));

	tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));

	sql = deparse_query_def(query);
	sel = (SelectStmt *) linitial(pg_parse_query(sql));
	sel->swStepFactor = query->swStepFactor;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	matrel = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);

	ReleaseSysCache(tup);

	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, WORKER);
	return parse_analyze((Node *) sel, "SELECT", 0, 0);
}

/*
 * make_combine_agg_for_viewdef
 *
 * Creates a combine aggregate for a view definition against a matrel
 */
static Node *
make_combine_agg_for_viewdef(ParseState *pstate, Oid cvid, Var *var,
		 List *order, WindowDef *over)
{
	List *args;
	Node *result;
	Oid fnoid = InvalidOid;
	Oid combinefn;
	Oid transoutfn;
	Oid combineinfn;
	Oid type = InvalidOid;
	Oid finaltype = InvalidOid;
	Oid statetype;
	Oid aggtype;
	Query *q = get_worker_query_for_id(cvid);

	Assert(!IsContQueryWorkerProcess());

	result = attr_to_aggs(var->varattno, q->targetList);

	extract_agg_final_info(result, &fnoid, &type, &finaltype);
	GetCombineInfo(fnoid, &combinefn, &transoutfn, &combineinfn, &statetype);

	aggtype = OidIsValid(statetype) ? statetype : finaltype;

	if (IsA(result, Aggref))
	{
		Aggref *agg = (Aggref *) result;

		/*
		 * Since we call attr_to_aggs on the target list of the worker plan, we'll see Aggrefs
		 * instead of WindowFuncs. Force such cases into WindowFuncs.
		 */
		if (over)
		{
			WindowFunc *win = makeNode(WindowFunc);

			win->winfnoid = agg->aggfnoid;
			win->winaggkind = AGGKIND_COMBINE;
			win->wintype = aggtype;
			win->winagg = true;
			win->winstar = false;

			result = (Node *) win;
		}
		else
		{
			agg->aggkind = AGGKIND_COMBINE;
			agg->aggfilter = NULL;
			agg->aggtype = aggtype;
			agg->aggstar = false;
		}
	}

	if (OidIsValid(statetype))
		var->vartype = statetype;

	args = make_combine_args(pstate, combineinfn, (Node *) var);

	if (IsA(result, Aggref))
	{
		Aggref *agg = (Aggref *) result;
		agg->orig_args = agg->args;
		agg->orig_directargs = agg->aggdirectargs;
		agg->orig_order = agg->aggorder;
		transformAggregateCall(pstate, agg, args, order, false);
	}
	else
	{
		((WindowFunc *) result)->args = args;
		transformWindowFuncCall(pstate, (WindowFunc *) result, over);
	}

	return (Node *) result;
}

/*
 * coerce_to_finalize
 *
 * Given an aggregate node, converts it to a finalize function call
 */
static Node *
coerce_to_finalize(ParseState *pstate, Oid aggfn, Oid type,
		Oid finaltype, Node *arg, Oid combineinfn)
{
	HeapTuple tup;
	Form_pg_aggregate aggform;
	Form_pg_proc procform;
	Oid final;
	FuncExpr *func;
	int i;

	tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfn));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for aggregate %u", aggfn);
	aggform = (Form_pg_aggregate) GETSTRUCT(tup);
	final = aggform->aggfinalfn;

	ReleaseSysCache(tup);
	if (!OidIsValid(final))
		return arg;

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(final));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for function %u", final);
	procform = (Form_pg_proc) GETSTRUCT(tup);

	func = (FuncExpr *) makeNode(FuncExpr);

	/*
	 * A final function may have a polymorphic type, but we know the exact output
	 * type of this aggregate because it was already previously analyzed, so use it.
	 */
	func->funcresulttype = OidIsValid(finaltype) ? finaltype : type;
	func->funcid = final;

	if (IsA(arg, Var))
		func->args = make_combine_args(pstate, combineinfn, arg);
	else
		func->args = list_make1(arg);

	/* final functions may take null dummy arguments for signature resolution */
	for (i=1; i<procform->pronargs; i++)
		func->args = lappend(func->args, makeNullConst(procform->proargtypes.values[i], -1, InvalidOid));

	ReleaseSysCache(tup);

	return (Node *) func;
}

/*
 * make_finalize_for_viewdef
 *
 * Creates a combine aggregate for a view definition against a matrel
 */
static Node *
make_finalize_for_viewdef(ParseState *pstate, Oid cvid, Var *var, Node *arg)
{
	Node *result;
	Oid fnoid = InvalidOid;
	Oid combinefn;
	Oid transoutfn;
	Oid combineinfn;
	Oid statetype;
	Oid type = InvalidOid;
	Oid finaltype = InvalidOid;
	Query *q = get_worker_query_for_id(cvid);

	result = attr_to_aggs(var->varattno, q->targetList);
	extract_agg_final_info(result, &fnoid, &type, &finaltype);
	GetCombineInfo(fnoid, &combinefn, &transoutfn, &combineinfn, &statetype);

	if (!OidIsValid(statetype))
		return (Node *) arg;

	return coerce_to_finalize(pstate, fnoid, type, finaltype, arg, combineinfn);
}

/*
 * combine_target_belongs_to_cv
 *
 * Verify that the given combine target actually belongs to a continuous view.
 * This allows us to run combines on complex range tables such as joins, as long
 * as the target combine column is from a CV.
 */
static bool
combine_target_belongs_to_cv(Var *target, List *rangetable, RangeVar **cv)
{
	RangeTblEntry *targetrte;
	ListCell *lc;
	Value *colname;

	if (list_length(rangetable) < target->varno)
	{
		elog(ERROR, "range table entry %d not in range table of length %d",
				target->varno, list_length(rangetable));
	}

	targetrte = (RangeTblEntry *) list_nth(rangetable, target->varno - 1);

	if (list_length(targetrte->eref->colnames) < target->varattno)
	{
		elog(ERROR, "attribute %d not column list of length %d",
						target->varattno, list_length(targetrte->eref->colnames));
	}

	colname = (Value *) list_nth(targetrte->eref->colnames, target->varattno - 1);

	foreach(lc, rangetable)
	{
		ListCell *clc;
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

		if (rte->relkind != RELKIND_CONTVIEW)
			continue;

		foreach(clc, rte->eref->colnames)
		{
			Value *c = (Value *) lfirst(clc);
			Relation rel;
			RangeVar *rv;
			char *cvname;
			char *namespace;

			if (!equal(colname, c))
				continue;

			/*
			 * The view contains a column name that matches the target column,
			 * so we just need to verify that it's actually a continuous view.
			 */
			rel = heap_open(rte->relid, NoLock);
			namespace = get_namespace_name(RelationGetNamespace(rel));
			cvname = RelationGetRelationName(rel);
			relation_close(rel, NoLock);
			rv = makeRangeVar(namespace, cvname, -1);

			if (IsAContinuousView(rv))
			{
				*cv = rv;
				return true;
			}
		}
	}

	return false;
}

/*
 * find_attr_from_joinlist
 *
 * Given a join list and a variable within it, finds the attribute position of that variable
 * in its matrel's descriptor.
 *
 * This also sets the given Var's varno to the varno of the continuous view RTE in the range
 * table, because it simplifies things if we're not selecting directly from the messy join RTE.
 */
static AttrNumber
find_cv_attr(ParseState *pstate, RangeVar *cvrv, RangeTblEntry *joinrte, Var *var)
{
	RangeTblEntry *cvrte = NULL;
	ListCell *lc;
	Value *colname = (Value *) list_nth(joinrte->eref->colnames, var->varattno - 1);
	Oid relid = RangeVarGetRelid(cvrv, NoLock, false);
	int i = 0;
	int varno = 0;

	foreach(lc, pstate->p_rtable)
	{
		varno++;
		cvrte = (RangeTblEntry *) lfirst(lc);
		if (cvrte->relid == relid)
			break;
	}

	if (!cvrte || cvrte->relid != relid)
		elog(ERROR, "could not find continuous view range table entry");

	foreach(lc, cvrte->eref->colnames)
	{
		Value *c = (Value *) lfirst(lc);
		i++;
		if (equal(colname, c))
		{
			var->varno = varno;
			return i;
		}
	}

	elog(ERROR, "could not find column \"%s\" in continuous view range table entry", strVal(colname));

	return InvalidAttrNumber;
}

/*
 * ParseCombineFuncCall
 *
 * Builds an expression for a combine() call on an aggregate CV column.
 * This may involve deserializing the column transition state.
 */
Node *
ParseCombineFuncCall(ParseState *pstate, List *fargs,
		List *order, Expr *filter, WindowDef *over, int location)
{
	Node *arg;
	Var *var;
	List *args;
	Oid combinefn;
	Oid transoutfn;
	Oid combineinfn;
	Oid statetype;
	RangeVar *rv;
	Query *cont_qry;
	TargetEntry *target;
	RangeTblEntry *rte;
	AttrNumber cvatt = InvalidAttrNumber;

	if (list_length(fargs) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with %d arguments", list_length(fargs)),
				 errhint("combine must be called with a single aggregate continuous view column reference.")));

	arg = linitial(fargs);

	if (!IsA(arg, Var))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with an invalid expression"),
				 errhint("combine must be called with a single aggregate continuous view column reference.")));

	var = (Var *) arg;

	if (!combine_target_belongs_to_cv(var, pstate->p_rtable, &rv))
	{
		Oid cvid;
		RangeTblEntry *rte = list_nth(pstate->p_rtable, var->varno - 1);

		if (rte->relkind != RELKIND_RELATION)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("the FROM clause is not a continuous view"),
					 errhint("Only aggregate continuous view columns can be combined.")));

		/*
		 * Sliding-window CQ's use combine aggregates in their
		 * view definition, so when they're created we can also
		 * end up here. We do this check second because it's slow.
		 */
		if (!RelIdIsForMatRel(rte->relid, &cvid))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("\"%s\" is not a continuous view", get_rel_name(rte->relid)),
					 errhint("Only aggregate continuous view columns can be combined.")));

		return make_combine_agg_for_viewdef(pstate, cvid, var, order, over);
	}

	/* Ok, it's a user combine query against an existing continuous view */
	cont_qry = GetContViewQuery(rv);

	rte = (RangeTblEntry *) list_nth(pstate->p_rtable, var->varno - 1);
	cvatt = var->varattno;

	/*
	 * If this is a join, our varattno will point to the position of the target
	 * combine column within the joined target list, so we need to pull out the
	 * table-level var that will point us to the CQ's target list
	 */
	cvatt = find_cv_attr(pstate, rv, rte, var);

	/*
	 * Find the aggregate node in the CQ that corresponds
	 * to the target combine column
	 */
	target = (TargetEntry *) list_nth(cont_qry->targetList, cvatt - 1);

	if (IsA(target->expr, Aggref) || IsA(target->expr, WindowFunc))
	{
		Oid fnoid = InvalidOid;
		Oid type = InvalidOid;
		Oid finaltype = InvalidOid;
		Oid aggtype;
		Var *v;
		Node *result;

		extract_agg_final_info((Node *) target->expr, &fnoid, &type, &finaltype);
		GetCombineInfo(fnoid, &combinefn, &transoutfn, &combineinfn, &statetype);

		aggtype = OidIsValid(statetype) ? statetype : finaltype;

		v = makeVar(var->varno, cvatt, aggtype, InvalidOid, InvalidOid, InvalidOid);
		args = make_combine_args(pstate, combineinfn, (Node *) v);

		if (over)
		{
			WindowFunc *wfunc = makeNode(WindowFunc);

			wfunc->winfnoid = fnoid;
			wfunc->wintype = aggtype;
			wfunc->args = args;
			wfunc->winstar = false;
			wfunc->winagg = true;
			wfunc->aggfilter = filter;
			wfunc->winaggkind = AGGKIND_COMBINE;

			transformWindowFuncCall(pstate, wfunc, over);

			result = (Node *) wfunc;
		}
		else
		{
			Aggref *agg = makeNode(Aggref);
			Aggref *orig_agg = (Aggref *) target->expr;

			Assert(IsA(orig_agg, Aggref));

			agg->aggfnoid = fnoid;
			agg->aggtype = aggtype;
			agg->args = args;
			agg->aggstar = false;
			agg->aggfilter = filter;
			agg->aggkind = AGGKIND_COMBINE;
			agg->orig_args = orig_agg->args;
			agg->orig_directargs = orig_agg->aggdirectargs;
			agg->orig_order = orig_agg->aggorder;

			transformAggregateCall(pstate, agg, args, order, false);

			result = (Node *) agg;
		}

		/* combines on continuous views should always finalize */
		return coerce_to_finalize(pstate, fnoid, type, finaltype, result, combineinfn);
	}

	/* TODO(usmanm): Eventually we should support nested aggregates and aggregates in expressions */
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
			errmsg("combine called with an invalid expression"),
			errhint("combine must be called with a single aggregate continuous view column reference.")));
}

/*
 * ParseFinalizeFuncCall
 */
Node *
ParseFinalizeFuncCall(ParseState *pstate, List *fargs, int location)
{
	Node *arg;
	Var *var;
	Oid cvid;
	RangeTblEntry *rte;
	RangeVar *matrelrv;
	Relation rel;
	List *vars;

	if (list_length(fargs) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("finalize called with %d arguments", list_length(fargs)),
				 errhint("finalize must be called with a single expression as its argument.")));
	}

	arg = linitial(fargs);

	if (IsA(arg, Aggref))
	{
		Aggref *agg = (Aggref *) arg;

		if (!AGGKIND_IS_COMBINE(agg->aggkind))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("finalize called with an invalid expression"),
					errhint("finalize must be called with a single column reference or a combine(...) as its argument.")));

		arg = linitial(agg->args);
	}
	else if (IsA(arg, WindowFunc))
	{
		WindowFunc *win = (WindowFunc *) arg;

		if (!AGGKIND_IS_COMBINE(win->winaggkind))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("finalize called with an invalid expression"),
					errhint("finalize must be called with a single column reference or a combine(...) as its argument.")));

		arg = linitial(win->args);
	}

	/* The argument could be wrapped in a transout function */
	vars = pull_var_and_aggs(arg);

	if (list_length(vars) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("finalize called with an invalid expression"),
				 errhint("finalize must be called with a single column reference or a combine(...) as its argument.")));
	}

	var = (Var *) linitial(vars);
	rte = list_nth(pstate->p_rtable, var->varno - 1);

	rel = heap_open(rte->relid, NoLock);
	matrelrv = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel), -1);
	relation_close(rel, NoLock);

	if (!RelIdIsForMatRel(rte->relid, &cvid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("\"%s\" is not a continuous view", matrelrv->relname),
				 errhint("Only aggregate continuous view columns can be finalized.")));
	}

	return make_finalize_for_viewdef(pstate, cvid, var, linitial(fargs));
}

/*
 * CollectUserCombines
 *
 * Collect all combine aggregate calls
 */
static bool
collect_combines_aggs(Node *node, List **combines)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		if (AGGKIND_IS_COMBINE(((Aggref *) node)->aggkind))
			*combines = lappend(*combines, node);
	}
	else if (IsA(node, WindowFunc))
	{
		if (AGGKIND_IS_COMBINE(((WindowFunc *) node)->winaggkind))
			*combines = lappend(*combines, node);
	}

	return expression_tree_walker(node, collect_combines_aggs, combines);
}

static bool
pull_vars(Node *node, List **varlist)
{
	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		*varlist = lappend(*varlist, node);
		return false;
	}

	return expression_tree_walker(node, pull_var_and_aggs_walker,
								  (void *) varlist);
}

/*
 * push_down_sw_predicate
 *
 * Push down any upper sliding-window predicates to the matrel SELECT.
 * This makes it possible to create multiple sliding windows over the same
 * continuous view.
 */
static void
push_down_sw_predicate(Var *dummy, Query *upper, Query *lower)
{
	List *vars = NIL;
	Var *real;

	pull_vars(lower->jointree->quals, &vars);

	if (list_length(vars) != 1)
		elog(ERROR, "column \"arrival_timestamp\" does not exist");

	/* sliding window predicates should only have a single qual */
	if (!IsA(lower->jointree->quals, OpExpr))
		elog(ERROR, "column \"arrival_timestamp\" does not exist");

	real = (Var *) linitial(vars);

	memcpy(dummy, real, sizeof(Var));
	lower->jointree->quals = upper->jointree->quals;
	upper->jointree->quals = (Node *) NIL;
}

/*
 * get_dummy_sw_predicate
 *
 * Returns a placeholder sliding-window predicate that should be pushed down,
 * or NULL if one doesn't exist.
 */
static Var *
get_dummy_sw_predicate(Node *where)
{
	List *vars = NIL;
	Var *var;

	pull_vars(where, &vars);

	if (list_length(vars) != 1)
		return NULL;

	var = (Var *) linitial(vars);
	if (IS_SW_TIMESTAMP_REF(var))
		return var;

	return NULL;
}

/*
 * RewriteContinuousViewSelect
 *
 * Possibly modify an overlay view SELECT rule before it's applied.
 * This is mainly used for user_combines where we strip away the finalize function
 * and put it on the *outer* SELECT.
 */
Query *
RewriteContinuousViewSelect(Query *query, Query *rule, Relation cv, int rtindex)
{
	RangeVar *rv = makeRangeVar(get_namespace_name(RelationGetNamespace(cv)), RelationGetRelationName(cv), -1);
	List *combines = NIL;
	ListCell *lc;
	RangeTblEntry *rte;
	Var *dummy;

	/* RTE is not a view? */
	rte = rt_fetch(rtindex, query->rtable);
	if (rte->relkind != RELKIND_CONTVIEW)
		return rule;

	/* try to bail early because this gets called from a hot path */
	if (!IsAContinuousView(rv))
		return rule;

	/*
	 * If there is an upper reference to arrival_timestamp, push it down
	 * to the matrel select.
	 */
	dummy = get_dummy_sw_predicate(query->jointree->quals);
	if (dummy)
		push_down_sw_predicate(dummy, query, rule);

	/* We only need to rewrite the overlay view definition if there are some user defines */
	collect_combines_aggs((Node *) query->targetList, &combines);
	if (list_length(combines) == 0)
		return rule;

	/* Remove finalize from any aggregate targets that are in combines */
	foreach(lc, combines)
	{
		List *l = NIL;
		Var *v;
		Node *agg;
		TargetEntry *te;
		Oid fnoid = InvalidOid;
		Oid type;
		Oid finaltype;
		Oid finalfn;
		HeapTuple tup;
		Form_pg_aggregate aggform;
		FuncExpr *fexpr;
		Expr *expr;
		Oid combinefn;
		Oid transoutfn;
		Oid combineinfn;
		Oid statetype;

		pull_vars(lfirst(lc), &l);
		Assert(list_length(l) == 1);
		v = (Var *) linitial(l);

		/* Not reading from us? */
		if (v->varno != rtindex)
			continue;

		l = pull_var_and_aggs(lfirst(lc));
		Assert(list_length(l) == 1);
		agg = (Node *) linitial(l);

		if (!IsA(agg, Aggref) && !IsA(agg, WindowFunc))
		{
			/* WTF? */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					errmsg("combine called with an invalid expression"),
					errhint("combine must be called with a single aggregate continuous view column reference.")));
		}

		extract_agg_final_info(agg, &fnoid, &type, &finaltype);

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(fnoid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for aggregate %u", fnoid);
		aggform = (Form_pg_aggregate) GETSTRUCT(tup);
		finalfn = aggform->aggfinalfn;
		ReleaseSysCache(tup);

		/* No finalize func? */
		if (!OidIsValid(finalfn))
			continue;

		te = list_nth(rule->targetList, v->varattno - 1);

		Assert(IsA(te->expr, FuncExpr));

		/* Strip away the finalize func */
		fexpr = (FuncExpr *) te->expr;

		Assert(fexpr->funcid == finalfn);
		expr = linitial(fexpr->args);

		GetCombineInfo(fnoid, &combinefn, &transoutfn, &combineinfn, &statetype);

		/* XXX(usmanm): We can probably avoid this extra serialize/deserialize step */
		if (OidIsValid(transoutfn))
		{
			fexpr->funcid = transoutfn;
			fexpr->funcresulttype = statetype;
		}
		else
			te->expr = expr;
	}

	return rule;
}

/*
 * GetSWExpr
 */
Node *
GetSWExpr(RangeVar *cv)
{
	SelectStmt *view;
	SelectStmt *sel;

	sel = get_cont_query_select_stmt(cv);
	TransformSelectStmtForContProcess(cv, sel, &view, WORKER);

	return view->whereClause;
}

static Interval *
parse_node_to_interval(Node *inode)
{
	ExprState *estate;
	ParseState *ps = make_parsestate(NULL);
	Expr *expr;
	Datum d;
	ExprContext *econtext = CreateStandaloneExprContext();
	bool isnull;

	inode = transformExpr(ps, (Node *) inode, EXPR_KIND_OTHER);
	expr = expression_planner((Expr *) inode);

	estate = ExecInitExpr(expr, NULL);
	d = ExecEvalExpr(estate, econtext, &isnull, NULL);

	free_parsestate(ps);
	FreeExprContext(econtext, false);

	return DatumGetIntervalP(d);
}

/*
 * GetSWInterval
 *
 * Returns the interval associated with a SW
 */
Interval *
GetSWInterval(RangeVar *rv)
{
	Node *sw_expr = GetSWExpr(rv);
	ContAnalyzeContext context;
	Node *expr;

	find_clock_timestamp_expr(sw_expr, &context);
	Assert(IsA(context.expr, A_Expr));

	/* arrival_timestamp > clock_timestamp() - interval... */
	expr = ((A_Expr *) context.expr)->rexpr;
	Assert(IsA(expr, A_Expr));

	expr = ((A_Expr *) expr)->rexpr;

	return parse_node_to_interval(expr);
}

ColumnRef *
GetSWTimeColumn(RangeVar *rv)
{
	Node *expr = GetSWExpr(rv);
	ContAnalyzeContext context;

	if (expr == NULL)
		return NULL;

	context.cols = NIL;
	context.types = NIL;

	collect_types_and_cols(expr, &context);

	Assert(list_length(context.cols) == 1);

	return linitial(context.cols);
}

ColumnRef *
GetWindowTimeColumn(RangeVar *cv)
{
	SelectStmt *view;
	SelectStmt *sel;
	ContAnalyzeContext context;

	sel = get_cont_query_select_stmt(cv);
	TransformSelectStmtForContProcess(cv, sel, &view, WORKER);

	if (view->whereClause)
	{
		context.cols = NIL;
		context.types = NIL;

		collect_types_and_cols(view->whereClause, &context);

		Assert(list_length(context.cols) == 1);

		return linitial(context.cols);
	}

	context.windows = NIL;
	collect_windows(view, &context);

	if (list_length(context.windows))
	{
		WindowDef *win = linitial(context.windows);
		SortBy *sort = linitial(win->orderClause);

		Assert(IsA(sort->node, ColumnRef));

		return (ColumnRef *) sort->node;
	}

	return NULL;
}

/*
 * CreateOuterSWTimeColumnRef
 *
 * If arrival_timestamp isn't found in an RTE, this hook is invoked by the parser.
 * It ultimately allows us to try to push any references to arrival_timestamp down.
 * This makes it easy to create sliding-window views on top of other sliding-window
 * views because we can just push down the arrival_timestamp predicate down to the
 * matrel SELECT.
 */
Node *
CreateOuterSWTimeColumnRef(ParseState *pstate, ColumnRef *cref, Node *var)
{
	Var *result;
	char *nspname = NULL;
	char *relname = NULL;
	char *colname = NULL;
	RangeTblEntry *rte = NULL;
	int	levels_up;
	RangeVar *rv;
	ColumnRef *sw_cref;

	if (var != NULL)
		return NULL;

	/* Try to figure out the name of the sliding window timestamp column */
	switch (list_length(cref->fields))
	{
		case 1:
		{
			Assert(list_length(pstate->p_rtable) == 1);
			rte = linitial(pstate->p_rtable);
			colname = FigureColname((Node *) cref);
			break;
		}
		case 2:
		{
			relname = strVal(linitial(cref->fields));
			colname = strVal(lsecond(cref->fields));
			rte = refnameRangeTblEntry(pstate, nspname, relname,
					cref->location,
					&levels_up);
			break;
		}
		case 3:
		{
			nspname = strVal(linitial(cref->fields));
			relname = strVal(lsecond(cref->fields));
			colname = strVal(lthird(cref->fields));
			rte = refnameRangeTblEntry(pstate, nspname, relname,
					cref->location,
					&levels_up);
			break;
		}
		case 4:
		{
			nspname = strVal(lsecond(cref->fields));
			relname = strVal(lthird(cref->fields));
			colname = strVal(lfourth(cref->fields));
			rte = refnameRangeTblEntry(pstate, nspname, relname,
					cref->location,
					&levels_up);
			break;
		}
		default:
			return NULL;
	}

	if (rte->relkind != RELKIND_CONTVIEW)
		return NULL;

	rv = makeRangeVar(
			get_namespace_name(get_rel_namespace(rte->relid)), get_rel_name(rte->relid), -1);

	if (!GetGCFlag(rv))
		return NULL;

	sw_cref = GetSWTimeColumn(rv);
	Assert(sw_cref);

	/*
	 * TODO(usmanm): For now we always allow ARRIVAL_TIMESTAMP because max_age hard codes it.
	 * Should fix it.
	 */
	if (pg_strcasecmp(colname, FigureColname((Node *) sw_cref)) != 0 &&
			pg_strcasecmp(colname, ARRIVAL_TIMESTAMP) != 0)
		return NULL;

	/* we only need to do something if the column couldn't be resolved at this level */
	result = makeVar(SW_TIMESTAMP_REF, 1, TIMESTAMPTZOID, -1, InvalidOid, 0);

	return (Node *) result;
}

/*
 * GetContinuousViewOption
 *
 * Returns the given option or NULL if it wasn't supplied
 */
DefElem *
GetContinuousViewOption(List *options, char *name)
{
	ListCell *lc;

	foreach(lc, options)
	{
		DefElem *de;

		if (!IsA(lfirst(lc), DefElem))
			continue;

		de = (DefElem *) lfirst(lc);
		if (de->defname && pg_strcasecmp(de->defname, name) == 0)
			return de;
	}

	return NULL;
}

/*
 * ApplyMaxAge
 *
 * Transforms a max_age WITH parameter into a sliding-window WHERE predicate
 */
void
ApplyMaxAge(SelectStmt *stmt, DefElem *max_age)
{
	A_Expr *where;
	FuncCall *clock_ts;
	TypeCast *interval;
	A_Const *arg;
	A_Expr *rexpr;
	ColumnRef *arrival_ts;
	RangeVar *sw_cv;

	if (has_clock_timestamp(stmt->whereClause, NULL))
		elog(ERROR, "cannot specify both \"max_age\" and a sliding window expression in the WHERE clause");

	sw_cv = GetSWContinuousViewRangeVar(stmt->fromClause);
	if (sw_cv == NULL && stmt->forContinuousView == false)
		elog(ERROR, "\"max_age\" can only be specified when reading from a stream or continuous view");

	if (!IsA(max_age->arg, String))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"max_age\" must be a valid interval string"),
				 errhint("For example, ... WITH (max_age = '1 hour') ...")));

	arg = makeNode(A_Const);
	arg->val = *((Value *) max_age->arg);

	interval = makeNode(TypeCast);
	interval->arg = (Node *) arg;
	interval->typeName = SystemTypeName("interval");

	/*
	 * If we are creating a view on top a continuous view, we must ensure that the step_size of the
	 * continuous view is at most MIN_VIEW_MAX_AGE_FACTOR of the max_age being specified.
	 */
	if (sw_cv)
	{
		Interval *sw_interval = GetSWInterval(sw_cv);
		int step_factor = GetContWorkerQuery(sw_cv)->swStepFactor;
		Interval *view_interval = parse_node_to_interval((Node *) interval);
		Interval *step_interval;
		Interval *min_interval;
		bool is_lt;

		Assert(sw_interval);

		step_interval = DatumGetIntervalP(DirectFunctionCall2(
				interval_mul, PointerGetDatum(sw_interval), Float8GetDatum(step_factor / (float8) 100.0)));
		min_interval = DatumGetIntervalP(DirectFunctionCall2(
				interval_div, PointerGetDatum(step_interval), Float8GetDatum(MIN_VIEW_MAX_AGE_FACTOR)));
		is_lt = DatumGetBool(DirectFunctionCall2(
				interval_lt, PointerGetDatum(view_interval), PointerGetDatum(min_interval)));

		if (is_lt)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("\"max_age\" value is too small"),
					errhint("\"max_age\" must be at least twice as much as the step size of the base continuous view.")));
	}

	arrival_ts = makeNode(ColumnRef);
	arrival_ts->fields = list_make1(makeString(ARRIVAL_TIMESTAMP));

	clock_ts = makeNode(FuncCall);
	clock_ts->funcname = list_make1(makeString(CLOCK_TIMESTAMP));

	rexpr = makeA_Expr(AEXPR_OP, list_make1(makeString("-")), (Node *) clock_ts, (Node *) interval, -1);
	where = makeA_Expr(AEXPR_OP, list_make1(makeString(">")), (Node *) arrival_ts, (Node *) rexpr, -1);

	if (stmt->whereClause)
	  stmt->whereClause = (Node *) makeBoolExpr(AND_EXPR, list_make2(where, stmt->whereClause), -1);
	else
	  stmt->whereClause = (Node *) where;
}

/*
 * ApplyStorageOptions
 */
void
ApplyStorageOptions(CreateContViewStmt *stmt)
{
	DefElem *def;
	SelectStmt *select = (SelectStmt *) stmt->query;

	/* max_age */
	def = GetContinuousViewOption(stmt->into->options, OPTION_MAX_AGE);
	if (def)
	{
		ApplyMaxAge(select, def);
		stmt->into->options = list_delete(stmt->into->options, def);
	}

	/* step_factor */
	select->swStepFactor = 0;
	def = GetContinuousViewOption(stmt->into->options, OPTION_STEP_FACTOR);
	if (def)
	{
		double factor;

		if (!has_clock_timestamp(select->whereClause, NULL))
			elog(ERROR, "can only specify \"step_factor\" for sliding window queries");

		if (IsA(def->arg, Integer))
			factor = intVal(def->arg);
		else
			factor = floatVal(def->arg);

		if (factor <= 0 || factor > 50)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"step_size\" must be a valid float in the range 0..50"),
					 errhint("For example, ... WITH (step_factor = '25') ...")));

		select->swStepFactor = factor;
		stmt->into->options = list_delete(stmt->into->options, def);
	}
	else
		select->swStepFactor = sliding_window_step_factor;
}
