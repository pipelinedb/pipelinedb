/*-------------------------------------------------------------------------
 *
 * analyzer.c
 *	  Support for parsing and analyzing continuous queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/analyzer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/binary_upgrade.h"
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
#include "miscadmin.h"
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
#include "parser/parse_func.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/analyzer.h"
#include "pipeline/matrel.h"
#include "pipeline/scheduler.h"
#include "pipeline/stream.h"
#include "pipeline/syscache.h"
#include "rewrite/rewriteHandler.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/bytea.h"
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
#define IS_DISTINCT_ONLY_AGG(name) (pg_strcasecmp((name), "count") == 0 || \
		pg_strcasecmp((name), "array_agg") == 0)

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
			IsA(context->old, Aggref) || IsA(context->old, A_Expr));
	Assert(IsA(context->new, FuncCall) || IsA(context->new, ColumnRef) || IsA(context->new, FuncExpr));
	Assert(IsA(context->old, TypeCast) || IsA(context->old, ColumnRef) ? IsA(context->new, ColumnRef) : true);
	Assert(IsA(context->old, Aggref) ? IsA(context->new, WindowFunc) || IsA(context->new, FuncExpr) : true);
	Assert(IsA(context->old, WindowFunc) ? IsA(context->new, FuncExpr) : true);
	Assert(sizeof(ColumnRef) <= sizeof(A_Expr));
	Assert(sizeof(ColumnRef) <= sizeof(FuncCall));
	Assert(sizeof(ColumnRef) <= sizeof(TypeCast));
	Assert(sizeof(FuncExpr) <= sizeof(Aggref));
	Assert(sizeof(WindowFunc) <= sizeof(Aggref));

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
		if (RangeVarIsForStream((RangeVar *) node, false))
			context->streams = lappend(context->streams, node);
		else
			context->rels = lappend(context->rels, node);

		return false;
	}

	return raw_expression_tree_walker(node, collect_rels_and_streams, (void *) context);
}

/*
 * collect_cols
 */
bool
collect_cols(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
		context->cols = lappend(context->cols, node);

	return raw_expression_tree_walker(node, collect_cols, (void *) context);
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
get_truncation_from_interval(SelectStmt *select, Node *time, double sf, Node *interval)
{
	Expr *expr = (Expr *) transformExpr(make_parsestate(NULL), interval, EXPR_KIND_WHERE);
	Const *c;
	Interval *window;
	FuncCall *func;
	Interval *step;
	Interval min_step;
	float8 factor = sf / (float8) 100.0;
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

	collect_cols((Node *) stmt->fromClause, &local);
	collect_cols((Node *) stmt->whereClause, &local);

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

HTAB *query_cache;
TransactionId query_cache_xid = InvalidTransactionId;
int current_query_id = 0;

typedef struct QueryState
{
	int queryId;
	Oid cqId;
	double swStepFactor;
	bool isContinuous;
} QueryState;

static QueryState *
get_query_state(Query *query)
{
	MemoryContext old;
	QueryState *entry;
	bool found;

	/*
	 * We lazily create and destroy the Query hashtable such that it's only valid for
	 * a single transaction. This does mean that a garbage reference to the cache will
	 * persist after a transaction finishes, but we detect this condition by associating
	 * a transaction id with the hashtable. If the hashtable's transaction id is different
	 * from the current transaction's, we immediately destroy and recreate it.
	 */
	if (TransactionIdIsValid(query_cache_xid))
	{
		if (query_cache_xid != GetCurrentTransactionId())
		{
			if (query_cache)
				hash_destroy(query_cache);
			query_cache = NULL;
			current_query_id = 0;
		}
	}

	if (query_cache == NULL)
	{
		HASHCTL		ctl;

		Assert(TopMemoryContext);

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(int);
		ctl.entrysize = sizeof(QueryState);
		ctl.hcxt = TopMemoryContext;
		query_cache = hash_create("query_cache", 16, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		query_cache_xid = InvalidTransactionId;

		if (!RecoveryInProgress())
			query_cache_xid = GetCurrentTransactionId();
	}

	if (query->queryId <= 0)
		query->queryId = ++current_query_id;

	old = MemoryContextSwitchTo(TopTransactionContext);
	entry = (QueryState *) hash_search(query_cache, &query->queryId, HASH_ENTER, &found);
	if (!found)
	{
		entry->isContinuous = false;
		entry->cqId = InvalidOid;
		entry->swStepFactor = 0;
	}
	MemoryContextSwitchTo(old);

	Assert(entry);

	return entry;
}

typedef struct IsContinuousContext
{
	Query *query;
	bool isContinuous;
} IsContinuousContext;


static bool
query_is_continuous_walker(Node *node, IsContinuousContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *ref = (RangeTblRef *) node;
		RangeTblEntry *rte = rt_fetch(ref->rtindex, context->query->rtable);

		if (IsStream(rte->relid))
		{
			context->isContinuous = true;
			return false;
		}
		else if (rte->relkind == RELKIND_RELATION && RelIdIsForMatRel(rte->relid, NULL) &&
				IsContQueryCombinerProcess() && !PipelineContextIsCombinerLookup())
		{
			/*
			 * It's a combiner plan
			 *
			 * Note that the groups lookup plan is also generated by the combiner but since it's
			 * a regular SELECT it should not be considered continuous
			 */
			context->isContinuous = true;
			return false;
		}
		else if (rte->rtekind == RTE_SUBQUERY)
		{
			context->isContinuous = QueryIsContinuous(rte->subquery);
			return true;
		}
	}

	return expression_tree_walker(node, query_is_continuous_walker, context);
}

/*
 * ViewStmtIsForContinuousView
 */
bool
ViewStmtIsForContinuousView(ViewStmt *stmt)
{
	ListCell *lc;
	List *new_opts = NIL;

	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, "forcv") != 0)
			new_opts = lappend(new_opts, def);
	}

	if (list_length(new_opts) < list_length(stmt->options))
	{
		stmt->options = new_opts;
		return true;
	}

	return false;
}

/*
 * QueryIsContinuous
 */
bool
QueryIsContinuous(Query *query)
{
	IsContinuousContext context;

	if (query->commandType != CMD_SELECT)
		return false;

	if (!query->jointree)
		return false;

	MemSet(&context, 0, sizeof(IsContinuousContext));

	context.query = query;
	query_is_continuous_walker((Node *) query->jointree->fromlist, &context);

	return context.isContinuous;
}

/*
 * GetSWStepFactor
 */
double
QueryGetSWStepFactor(Query *query)
{
	QueryState *state = get_query_state(query);
	return state->swStepFactor;
}

/*
 * SetSWStepFactor
 */
void
QuerySetSWStepFactor(Query *query, double sf)
{
	QueryState *state = get_query_state(query);
	state->swStepFactor = sf;
}

/*
 * GetQueryId
 */
Oid
QueryGetContQueryId(Query *query)
{
	QueryState *state = get_query_state(query);
	return state->cqId;
}

/*
 * SetContQueryId
 */
void
QuerySetContQueryId(Query *query, Oid id)
{
	QueryState *state = get_query_state(query);
	state->cqId = id;
}

/*
 * ValidateParsedContQuery
 */
void
ValidateParsedContQuery(RangeVar *name, Node *node, const char *sql)
{
	SelectStmt *select;
	ContAnalyzeContext *context;
	ListCell *lc;

	if (!IsA(node, SelectStmt))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				errmsg("continuous views can only be defined using SELECT queries")));

	select = (SelectStmt *) copyObject(node);
	validate_target_list(select);

	context = MakeContAnalyzeContext(make_parsestate(NULL), select, Worker);
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

	collect_windows(select, context);
	if (context->windows)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("continuous queries don't support WINDOW functions")));

	/* recurse for any sub-SELECTs */
	if (list_length(select->fromClause) == 1 && IsA(linitial(select->fromClause), RangeSubselect))
	{
		RangeSubselect *sub = (RangeSubselect *) linitial(select->fromClause);

		ValidateSubselect(sub->subquery, "subqueries in continuous views");
		ValidateParsedContQuery(name, sub->subquery, sql);
		return;
	}

	collect_rels_and_streams((Node *) select->fromClause, context);
	collect_cols((Node *) select, context);
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

	/* Transform the fromClause because we do a little bit of expression type inference below */
	transformFromClause(context->pstate, copyObject(select->fromClause));

	/* Validate aggregate functions */
	foreach(lc, context->funcs)
	{
		FuncCall *func = (FuncCall *) lfirst(lc);
		char *name = NameListToString(func->funcname);

		if (func->agg_distinct && !IS_DISTINCT_ONLY_AGG(name))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("continuous queries don't support DISTINCT expressions for \"%s\" aggregate", name),
					parser_errposition(context->pstate, func->location)));
	}

	/* Ensure that the sliding window is legal, if present */
	context->expr = NULL;
	if (find_clock_timestamp_expr(select->whereClause, context))
		validate_clock_timestamp_expr(select, context->expr, context);

	/* DISTINCT now allowed for sliding windows */
	if (context->is_sw && select->distinctClause)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("sliding window continuous queries don't allow a DISTINCT clause")));

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

	Assert(IsA(expr, Aggref));

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

/*
 * validate_agg
 */
static void
validate_agg(Node *node)
{
	Oid aggfnoid = InvalidOid;
	HeapTuple	aggtup;
	HeapTuple combtup;
	Form_pg_aggregate aggform;

	Assert(IsA(node, Aggref));

	aggfnoid = ((Aggref *) node)->aggfnoid;

	aggtup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	Assert(HeapTupleIsValid(aggtup));

	aggform = (Form_pg_aggregate) GETSTRUCT(aggtup);
	ReleaseSysCache(aggtup);

	combtup = SearchPipelineSysCache2(PIPELINECOMBINETRANSFNOID,
			ObjectIdGetDatum(aggform->aggfinalfn), ObjectIdGetDatum(aggform->aggtransfn));

	if (!HeapTupleIsValid(combtup))
	{
		if (aggform->aggtranstype == INTERNALOID)
			elog(ERROR, "aggregate %u requires a pipeline_combine record but does not have one", aggfnoid);
		return;
	}

	ReleaseSysCache(combtup);
}

/*
 * ValidateContQuery
 */
void
ValidateContQuery(Query *query)
{
	List *aggs = pull_var_and_aggs((Node *) query->targetList);
	ListCell *lc;

	foreach(lc, query->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (exprType((Node *) te->expr) == UNKNOWNOID)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
					 errmsg("column %u has an unknown type", te->resno),
					 errhint("Explicitly cast the expression to a known type.")));
		}
	}

	foreach(lc, aggs)
	{
		Node *n = (Node *) lfirst(lc);
		if (IsA(n, Aggref) || IsA(n, WindowFunc))
			validate_agg(n);
	}
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
transformCreateStreamStmt(CreateForeignTableStmt *stmt)
{
	ListCell *lc;
	bool saw_atime = false;

	foreach(lc, stmt->base.tableElts)
	{
		ColumnDef *coldef = (ColumnDef *) lfirst(lc);

		if (pg_strcasecmp(coldef->colname, ARRIVAL_TIMESTAMP) == 0)
		{
			Oid typ;
			int typmod;

			typenameTypeIdAndMod(NULL, coldef->typeName, &typ, &typmod);

			/*
			 * HACK(usmanm): If arrival_timestamp is the last column and has the correct type, then let it slide. This
			 * it for making CREATE STREAM ... (LIKE ...) and pg_dump/pg_restore to work. Should be fixed by #1616.
			 */
			if (!lnext(lc) && typ == TIMESTAMPTZOID)
				saw_atime = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column name \"%s\" conflicts with a system column name",
								ARRIVAL_TIMESTAMP)));
		}
	}

	if (!saw_atime)
	{
		ColumnDef *coldef;
		TypeName *typename;

		typename = makeNode(TypeName);
		typename->typeOid = TIMESTAMPTZOID;
		typename->typemod = -1;

		coldef = makeNode(ColumnDef);
		coldef->colname = ARRIVAL_TIMESTAMP;
		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->constraints = NIL;
		coldef->typeName = typename;

		stmt->base.tableElts = lappend(stmt->base.tableElts, coldef);
	}
}

/*
 * rewrite_from_clause
 */
static Node *
rewrite_from_clause(Node *from)
{
	Node *new_from = from;

	if (from == NULL)
		return NULL;

	if (IsA(from, RangeSubselect))
	{
		RangeSubselect *sub = (RangeSubselect *) from;
		sub->subquery = rewrite_from_clause(sub->subquery);

		if (IsA(sub->subquery, SelectStmt))
		{
			SelectStmt *s = (SelectStmt *) sub->subquery;
			s->fromClause = (List *) rewrite_from_clause((Node *) s->fromClause);
		}
		new_from = from;
	}
	else if (IsA(from, JoinExpr))
	{
		JoinExpr *join = (JoinExpr *) from;
		join->larg = rewrite_from_clause(join->larg);
		join->rarg = rewrite_from_clause(join->rarg);
		new_from = from;
	}
	else if (IsA(from, List))
	{
		List *result = NIL;
		ListCell *lc;

		foreach(lc, (List *) from)
		{
			Node *node = (Node *) lfirst(lc);
			result = lappend(result, rewrite_from_clause(node));
		}
		new_from = (Node *) result;
	}
	else if (IsA(from, RangeFunction))
	{
		 /*
		  * If this is a from item of the form:
		  *
		  * ... FROM output_of('cv_name')
		  *
		  * We rewrite it to refer to the target CV's underlying
		  * output stream.
		  */
		 RangeFunction *rf = (RangeFunction *) from;
		 FuncCall *fn;
		 RangeVar *rv;
		 Relation cvrel;
		 A_Const *arg;
		 char *cv_name;

		 /* Functions here are represented by a lists of length 2 */
		 if (list_length(rf->functions) != 1 || list_length(linitial(rf->functions)) != 2)
			 return from;

		 fn = (FuncCall *) linitial(linitial(rf->functions));
		 if (list_length(fn->funcname) != 1)
			 return from;

		 if (pg_strcasecmp(strVal(linitial(fn->funcname)), OUTPUT_OF) != 0)
			 return from;

		 if (list_length(fn->args) != 1)
			 return from;

		 if (!IsA(linitial(fn->args), A_Const))
			 return from;

		 arg = (A_Const *) linitial(fn->args);
		 if (!IsA(&arg->val, String))
			 return from;

		 cv_name = strVal(&arg->val);
		 rv = makeRangeVarFromNameList(textToQualifiedNameList((text *) CStringGetTextDatum(cv_name)));

		 /* Just fail if the target CV doesn't exist */
		 cvrel = heap_openrv(rv, NoLock);
		 heap_close(cvrel, NoLock);

		 rv->relname = CVNameToOSRelName(rv->relname);
		 new_from = (Node *) rv;
	}

	return new_from;
}

/*
 * RewriteFromClause
 */
void
RewriteFromClause(SelectStmt *stmt)
{
	if (stmt->op)
		elog(ERROR, "set operations within continuous views are currently not supported");
	stmt->fromClause = (List *) rewrite_from_clause((Node *) stmt->fromClause);
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

static ColumnRef *
node_for_cref(Node *node)
{
	while (node)
	{
		if (IsA(node, ColumnRef))
			return (ColumnRef *) node;

		if (IsA(node, TypeCast))
		{
			TypeCast *tc = (TypeCast *) node;
			node = tc->arg;
			continue;
		}

		return NULL;
	}

	return NULL;
}

static bool
no_tc_equal(Node *node1, Node *node2)
{
	while (IsA(node1, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node1;
		node1 = tc->arg;
	}
	while (IsA(node2, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node2;
		node2 = tc->arg;
	}
	return equal(node1, node2);
}

static ResTarget *
find_node_in_target_list(List *tlist, Node *node)
{
	ListCell *lc;

	foreach(lc, tlist)
	{
		ResTarget *res = lfirst(lc);
		ColumnRef *res_cref;
		ColumnRef *node_cref;
		char *resname;
		char *colname;

		if (no_tc_equal(res->val, node))
			return res;

		node_cref = node_for_cref(node);
		if (!node_cref)
			continue;

		/* At this point we're sure that node is for a column */

		if (res->name)
			resname = res->name;
		else
			resname = FigureColname((Node *) res);

		colname = FigureColname(node);
		if (pg_strcasecmp(resname, colname) != 0)
			continue;

		res_cref = node_for_cref(res->val);
		if (res_cref)
		{
			if (equal(res_cref, node_cref))
				return res;
			continue;
		}

		/*
		 * Is this ResTarget overriding a node it references?
		 * Example: substring(key::text, 1, 2) AS key
		 */
		if (!contains_node(res->val, node))
			return res;
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
 * is_constant_expr
 */
static bool
is_constant_expr(Node *node)
{
	ContAnalyzeContext context;

	MemSet(&context, 0, sizeof(ContAnalyzeContext));

	context.funcs = NIL;
	collect_funcs(node, &context);
	if (context.funcs)
		return false;

	context.cols = NIL;
	collect_cols(node, &context);
	if (context.cols)
		return false;

	return true;
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
		Node *val;
		ResTarget *matrel_res;

		/*
		 * For constant expressions we don't want to reference the corresponding column in the worker query,
		 * because that could produce an invalid combiner query. For example, consider this CV definition:
		 *
		 * CREATE CONTINUOUS VIEW v AS SELECT 1 AS const_ref, count(*) FROM s
		 *
		 * If we referenced the worker's constant output as a column, the combiner query would look like this:
		 *
		 * SELECT const_ref, count FROM worker
		 *
		 * which is invalid because const_ref is ungrouped. So we just evaluate the constant expression in
		 * the combiner in these cases.
		 */
		if (is_constant_expr(rt->val))
			val = copyObject(rt->val);
		else
			val = (Node *) create_colref_for_res_target(rt);

		matrel_res = create_res_target_for_node(val, rt->name);

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
truncate_timestamp_field(SelectStmt *select, Node *time, A_Expr *sw_expr, double sf, ContAnalyzeContext *context)
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

		time = get_truncation_from_interval(select, time, sf, ct_expr->rexpr);
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
hoist_time_node(SelectStmt *proc, Node *time, A_Expr *sw_expr, double sf, ContAnalyzeContext *context)
{
	ListCell *lc;
	bool found = false;
	ColumnRef *cref;
	List *groupClause;

	truncate_timestamp_field(proc, time, sw_expr, sf, context);

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
		if (context->proc_type == Combiner)
			proc->groupClause = lappend(proc->groupClause, cref);
		else
			proc->groupClause = lappend(proc->groupClause, time);
	}

	return cref;
}

static void
proj_and_group_for_sliding_window(SelectStmt *proc, SelectStmt *view, double sw_step_factor, ContAnalyzeContext *context)
{
	Node *time;
	A_Expr *sw_expr = NULL;
	ColumnRef *cref;
	ReplaceNodeContext cxt;

	context->expr = NULL;
	find_clock_timestamp_expr(proc->whereClause, context);

	Assert(context->expr && IsA(context->expr, A_Expr));
	sw_expr = (A_Expr *) context->expr;

	if (has_clock_timestamp(sw_expr->lexpr, NULL))
		time = sw_expr->rexpr;
	else
		time = sw_expr->lexpr;

	time = copyObject(time);

	context->cols = NIL;
	collect_cols(time, context);

	Assert(list_length(context->cols) == 1);

	/* Create and truncate projection and grouping on temporal expression */
	context->hoisted_name = FigureColname(linitial(context->cols));
	if (context->view_combines)
		cref = hoist_time_node(proc, time, sw_expr, sw_step_factor, context);
	else
		cref = hoist_node(&proc->targetList, time, context);

	context->hoisted_name = NULL;

	view->whereClause = copyObject(sw_expr);

	cxt.old = time;
	cxt.new = (Node *) cref;
	cxt.size = sizeof(ColumnRef);

	replace_node(view->whereClause, &cxt);
}

/*
 * TransformSelectStmtForContProc
 */
SelectStmt *
TransformSelectStmtForContProcess(RangeVar *mat_relation, SelectStmt *stmt, SelectStmt **viewptr, double sw_step_factor, ContQueryProcType proc_type)
{
	ContAnalyzeContext *context;
	SelectStmt *proc;
	SelectStmt *view;
	int tl_len;
	int grp_len;
	List *tmp_list;
	ListCell *lc;
	int i;
	bool has_aggs = false;
	List *distinct_on = NIL;

	Assert(proc_type == Worker || proc_type == Combiner);

	proc = (SelectStmt *) copyObject(stmt);
	view = makeNode(SelectStmt);

	context = MakeContAnalyzeContext(make_parsestate(NULL), proc, proc_type);

	collect_rels_and_streams((Node *) proc->fromClause, context);
	collect_cols((Node *) proc, context);
	collect_agg_funcs((Node *) proc->targetList, context);
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
	context->view_combines = (context->is_sw &&
			(list_length(context->funcs) || list_length(stmt->groupClause)));

	if (context->is_sw)
		proj_and_group_for_sliding_window(proc, view, sw_step_factor, context);

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
			if (IsA(group_node, GroupingSet) && proc_type == Worker)
				continue;

			/*
			 * For workers, don't add the hoisted column because it can lead to ambiguity, for example if
			 * there is an id column in t for the query below:
			 *   CREATE CONTINUOUS VIEW v AS SELECT s.id, t.str, sum(s.val + t.val) FROM s JOIN t ON s.id = t.id GROUP BY s.id;
			 */
			if (proc_type == Combiner)
				tmp_list = lappend(tmp_list, cref);
			else
				tmp_list = lappend(tmp_list, node);
		}

		if (IsA(group_node, GroupingSet) && proc_type == Worker)
			tmp_list = lappend(tmp_list, group_node);
	}

	for (; i < list_length(proc->groupClause); i++)
		tmp_list = lappend(tmp_list, list_nth(proc->groupClause, i));

	proc->groupClause = tmp_list;

	/*
	 * Hoist any nodes in the distinctClause that are not being projected
	 */
	foreach(lc, proc->distinctClause)
	{
		Node *node = (Node *) lfirst(lc);
		ColumnRef *cref = hoist_node(&proc->targetList, node, context);
		distinct_on = lappend(distinct_on, cref);
	}

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
		has_aggs = true;

		/* No need to re-write top level aggregates */
		if (is_res_target_for_node((Node *) res, (Node *) agg))
		{
			Node *node;

			Assert(list_length(context->funcs) == 1);

			node = create_agg_node_for_view_overlay(create_colref_for_res_target(res), agg, context);
			matrel_res = create_res_target_for_node(node, name);

			tmp_list = lappend(tmp_list, res);
			view->targetList = lappend(view->targetList, matrel_res);

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
		}

		/*
		 * Any expression in the target list only needs to be added to the view, and not the
		 * cont query. We have carefully replaces all aggregates and column refs in the expression
		 * node to match appropriate column refs on the matrel.
		 */
		view->targetList = lappend(view->targetList, create_res_target_for_node(res_val, name));
	}

	if (proc_type == Combiner)
	{
		Assert(mat_relation != NULL);
		tmp_list = select_from_matrel(tmp_list);
		proc->fromClause = list_make1(mat_relation);
	}

	proc->targetList = tmp_list;

	/*
	 * We use grouping for SELECT DISTINCT since results are updated incrementally across xacts over time
	 *
	 * e.g. SELECT DISTINCT x, y, z FROM stream becomes,
	 *
	 * SELECT x, y, z FROM stream GROUP BY x, y, z
	 */
	if (proc->distinctClause && !has_aggs)
	{
		List *tl_cols = NIL;

		foreach(lc, tmp_list)
		{
			ResTarget *rt = (ResTarget *) lfirst(lc);
			ColumnRef *cref = create_colref_for_res_target(rt);

			tl_cols = lappend(tl_cols, cref);
		}

		proc->distinctClause = NIL;
		proc->groupClause = tl_cols;

		view->distinctClause = distinct_on;
	}

	/* Copy over worker limit/offsets to the view */
	view->limitCount = proc->limitCount;
	view->limitOffset = proc->limitOffset;
	proc->limitCount = NULL;
	proc->limitOffset = NULL;

	/* Overlay view reads from matrel */
	view->fromClause = list_make1(mat_relation);

	if (proc_type == Combiner)
	{
		/*
		 * Combiner shouldn't have to re-do the filtering work of the WHERE clause.
		 */
		proc->whereClause = NULL;

		/* Remove any FILTER clauses */
		context->funcs = NIL;
		collect_agg_funcs((Node *) proc->targetList, context);
		foreach(lc, context->funcs)
		{
			FuncCall *fn = lfirst(lc);
			fn->agg_filter = NULL;
		}
	}

	if (viewptr)
		*viewptr = view;

	return proc;
}

static SelectStmt *
get_cont_query_select_stmt(RangeVar *rv, double *step_factor)
{
	HeapTuple tup;
	bool isnull;
	Datum tmp;
	char *sql;
	Query *query;
	SelectStmt *select;
	Form_pipeline_query row;

	tup = GetPipelineQueryTuple(rv);

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tup);
	tmp = PipelineSysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));

	sql = deparse_query_def(query);
	select = (SelectStmt *) linitial(pg_parse_query(sql));
	if (step_factor)
		*step_factor = (double) row->step_factor;

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

	sel = get_cont_query_select_stmt(rv, NULL);
	context = MakeContAnalyzeContext(NULL, sel, Worker);
	rewrite_streaming_aggs(sel, context);

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
	double sf;

	matrel = GetMatRelName(rv);
	sel = get_cont_query_select_stmt(rv, &sf);
	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, sf, Worker);

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
	double sf;

	matrel = GetMatRelName(rv);
	sel = get_cont_query_select_stmt(rv, &sf);

	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, sf, Combiner);
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

	Assert(IsA(n, Aggref));

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
		Node *placeholder = NULL;
		FuncCall *fn = makeNode(FuncCall);
		Node *result;
		FuncExpr *expr;

		/*
		 * The below transformFuncCall call doesn't handle FieldSelects, so if
		 * we have one we use a null constant as a placeholder and swap in the
		 * FieldSelect when we get the analyzed function call back.
		 */
		if (IsA(arg, FieldSelect))
		{
			FieldSelect *fs = (FieldSelect *) arg;
			placeholder = (Node *) makeNullConst(fs->resulttype, fs->resulttypmod, fs->resultcollid);
		}

		fn->funcname = list_make1(makeString(get_func_name(combineinfn)));
		fn->args = list_make1(placeholder ? placeholder : arg);

		result = transformFuncCall(pstate, fn);
		Assert(IsA(result, FuncExpr));

		expr = (FuncExpr *) result;
		expr->args = list_make1(arg);
		args = list_make1(expr);
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
	Aggref *agg = (Aggref *) n;
	*aggfn = agg->aggfnoid;
	*type = agg->aggtype;
	*finaltype = agg->aggfinaltype;
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

	tup = SearchPipelineSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(id));

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view with id \"%d\" does not exist", id)));

	row = (Form_pipeline_query) GETSTRUCT(tup);
	tmp = PipelineSysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));

	sql = deparse_query_def(query);
	sel = (SelectStmt *) linitial(pg_parse_query(sql));

	matrel = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);

	ReleaseSysCache(tup);

	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, row->step_factor, Worker);
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
		WindowFunc *win = (WindowFunc *) result;
		win->args = args;
		transformWindowFuncCall(pstate, win, over);
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
 * combine_target_for_cv
 *
 * Verify that the given combine target actually belongs to a continuous view.
 * This allows us to run combines on complex range tables such as joins, as long
 * as the target combine column is from a CV.
 */
static bool
combine_target_for_cv(Var *target, List *rangetable, RangeVar **cv)
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
 * combine_target_for_osrel
 */
static bool
combine_target_for_osrel(Node *node, List *rtable, FieldSelect **fsp, Oid *cqid, Expr **expr)
{
	Var *v;
	RangeTblEntry *rte;
	FieldSelect *fs;
	TypeCacheEntry *typ;
	ContQuery *cq;
	Query *q;
	ListCell *lc;
	char *target;

	/*
	 * The FieldSelect may be wrapped in a final function
	 */
	if (IsA(node, FuncExpr))
	{
		FuncExpr *f = (FuncExpr *) node;
		if (list_length(f->args) == 1)
			node = linitial(f->args);
	}

	if (!IsA(node, FieldSelect))
		return false;

	fs = (FieldSelect *) node;

	if (!IsA(fs->arg, Var))
		return false;

	v = (Var *) fs->arg;

	rte = rt_fetch(v->varno, rtable);
	if (!IsStream(rte->relid))
		return false;

	if (!RelIdIsForOutputStream(rte->relid, cqid))
		return false;

	*fsp = fs;
	cq = GetContQueryForId(*cqid);
	q = GetContViewQuery(cq->name);
	typ = lookup_type_cache(v->vartype, TYPECACHE_TUPDESC);

	target = NameStr(typ->tupDesc->attrs[fs->fieldnum - 1]->attname);

	foreach(lc, q->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);

		if (te->resname && pg_strcasecmp(te->resname, target) == 0)
		{
			*expr = te->expr;
			return true;
		}
	}

	elog(ERROR, "could not find column \"%s\" in continuous view's output stream", target);

	return false;
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
	Oid cqid;
	FieldSelect *fs;
	Expr *expr;

	if (list_length(fargs) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with %d arguments", list_length(fargs)),
				 errhint("combine must be called with a single aggregate continuous view column reference.")));

	arg = linitial(fargs);

	/*
	 * There are 3 types of combine calls that we must handle:
	 *
	 * 1) combine on an output stream's delta row, at CV create time.
	 * 2) combine call in a SW's overlay view, at overlay create time and thus CV create time.
	 * 3) combine call within user-issued query against a CV, at SELECT time.
	 */

	/*
	 * 1) Is it a combine call on an output stream?
	 */
	if (combine_target_for_osrel(arg, pstate->p_rtable, &fs, &cqid, &expr))
	{
		/*
		 * We may need to pull off a final function to get to the aggregate
		 */
		if (IsA(expr, FuncExpr))
		{
			FuncExpr *f = (FuncExpr *) expr;
			if (list_length(f->args) == 1)
				expr = linitial(f->args);
		}

		if (IsA(expr, Aggref))
		{
			Oid fnoid = InvalidOid;
			Oid type = InvalidOid;
			Oid finaltype = InvalidOid;
			Aggref *agg = makeNode(Aggref);
			Aggref *orig_agg = (Aggref *) expr;

			extract_agg_final_info((Node *) expr, &fnoid, &type, &finaltype);
			GetCombineInfo(fnoid, &combinefn, &transoutfn, &combineinfn, &statetype);

			args = make_combine_args(pstate, combineinfn, (Node *) fs);

			Assert(IsA(orig_agg, Aggref));

			agg->aggfnoid = fnoid;
			agg->aggtype = finaltype;
			agg->args = args;
			agg->aggstar = false;
			agg->aggfilter = filter;
			agg->aggkind = AGGKIND_COMBINE;
			agg->orig_args = orig_agg->args;
			agg->orig_directargs = orig_agg->aggdirectargs;
			agg->orig_order = orig_agg->aggorder;

			transformAggregateCall(pstate, agg, args, order, false);

			return (Node *) agg;
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("combine called with an invalid expression"),
					 errhint("combine must be called with a single aggregate continuous view column reference.")));

		}
	}

	if (!IsA(arg, Var))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with an invalid expression"),
				 errhint("combine must be called with a single aggregate continuous view column reference.")));

	var = (Var *) arg;

	/*
	 * 2) Are we creating a SW overlay view that has a combine call?
	 */
	if (!combine_target_for_cv(var, pstate->p_rtable, &rv))
	{
		Oid cvid;
		RangeTblEntry *rte = rt_fetch(var->varno, pstate->p_rtable);

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

	/*
	 * 3) It must be a combine call within user-issued query against a CV at SELECT time
	 */
	cont_qry = GetContViewQuery(rv);

	rte = (RangeTblEntry *) rt_fetch(var->varno, pstate->p_rtable);

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

	if (IsA(target->expr, Aggref))
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
	rte = rt_fetch(var->varno, pstate->p_rtable);

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
	List *unfinalized = NIL;

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
		Var *matrel_ref;

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

		/*
		 * The TL may contain duplicate combine calls, in which case we may
		 * have already processed this entry
		 */
		if (list_member_ptr(unfinalized, te->expr))
			continue;

		/*
		 * We want the Var referencing the matrel, which could be wrapped in
		 * a combinein function
		 */
		l = NIL;
		pull_vars((Node *) te->expr, &l);
		Assert(list_length(l) == 1);

		matrel_ref = (Var *) linitial(l);
		te->expr = (Expr *) matrel_ref;

		unfinalized = lappend(unfinalized, te->expr);
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
	double sf;

	sel = get_cont_query_select_stmt(cv, &sf);
	TransformSelectStmtForContProcess(cv, sel, &view, sf, Worker);

	return view->whereClause;
}

/*
 * GetTTLExpiredExpr
 */
Node *
GetTTLExpiredExpr(RangeVar *cv)
{
	HeapTuple tup = GetPipelineQueryTuple(cv);
	Form_pipeline_query row;
	Relation rel;
	TupleDesc desc;
	TypeCast *interval;
	int i;
	char *colname = NULL;
	ColumnRef *ttl_col;
	A_Expr *rexpr;
	FuncCall *clock_ts;
	A_Expr *where;
	StringInfoData buf;
	A_Const *arg;
	Value *v;

	Assert(HeapTupleIsValid(tup));

	row = (Form_pipeline_query) GETSTRUCT(tup);
	rel = heap_open(row->matrelid, NoLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		if (desc->attrs[i]->attnum == row->ttl_attno)
		{
			colname = pstrdup(NameStr(desc->attrs[i]->attname));
			break;
		}
	}

	Assert(colname);
	heap_close(rel, NoLock);

	initStringInfo(&buf);
	appendStringInfo(&buf, "%d seconds", row->ttl);

	arg = makeNode(A_Const);
	v = makeString(buf.data);
	arg->val = *v;

	interval = makeNode(TypeCast);
	interval->arg = (Node *) arg;
	interval->typeName = SystemTypeName("interval");

	ttl_col = makeNode(ColumnRef);
	ttl_col->fields = list_make1(makeString(colname));

	clock_ts = makeNode(FuncCall);
	clock_ts->funcname = list_make1(makeString(CLOCK_TIMESTAMP));

	rexpr = makeA_Expr(AEXPR_OP, list_make1(makeString("-")), (Node *) clock_ts, (Node *) interval, -1);
	where = makeA_Expr(AEXPR_OP, list_make1(makeString("<")), (Node *) ttl_col, (Node *) rexpr, -1);

	ReleaseSysCache(tup);

	return (Node *) where;
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
	ContQuery *view;
	Relation rel;
	ColumnRef *col;
	TupleDesc desc;
	Form_pg_attribute attr;

	view = GetContQueryForView(rv);
	rel = heap_open(view->matrelid, AccessShareLock);
	desc = RelationGetDescr(rel);

	Assert(AttributeNumberIsValid(view->sw_attno));
	attr = desc->attrs[view->sw_attno - 1];

	col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(NameStr(attr->attname))));

	heap_close(rel, AccessShareLock);

	return col;
}

ColumnRef *
GetWindowTimeColumn(RangeVar *cv)
{
	SelectStmt *view;
	SelectStmt *sel;
	ContAnalyzeContext context;
	double sf;

	sel = get_cont_query_select_stmt(cv, &sf);
	TransformSelectStmtForContProcess(cv, sel, &view, sf, Worker);

	if (view->whereClause)
	{
		context.cols = NIL;
		context.types = NIL;

		collect_cols(view->whereClause, &context);

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
 * GetOptionAsString
 */
bool
GetOptionAsString(List *options, char *option, char **result)
{
	DefElem *def = GetContinuousViewOption(options, option);

	if (!def)
		return false;

	if (IsA(def->arg, String))
	{
		*result = strVal(def->arg);
	}
	else if (IsA(def->arg, Integer))
	{
		StringInfoData buf;

		initStringInfo(&buf);
		appendStringInfo(&buf, "%ld", intVal(def->arg));

		*result = buf.data;
	}
	else if (IsA(def->arg, TypeName))
	{
		TypeName *t = (TypeName *) def->arg;
		*result = NameListToString(t->names);
	}
	else
	{
		return false;
	}

	return true;
}

/*
 * GetOptionAsInteger
 */
bool
GetOptionAsInteger(List *options, char *option, int *result)
{
	DefElem *def = GetContinuousViewOption(options, option);

	if (!def)
		return false;

	if (IsA(def->arg, String))
		*result = atoi(strVal(def->arg));
	else if (IsA(def->arg, Integer))
		*result = intVal(def->arg);
	else
		return false;

	return true;
}

/*
 * IntervalToEpoch
 */
int
IntervalToEpoch(Interval *i)
{
	return (int) DatumGetFloat8(DirectFunctionCall2(interval_part, CStringGetTextDatum("epoch"), (Datum) i));
}

/*
 * ApplySlidingWindow
 *
 * Transforms a sw WITH parameter into a sliding-window WHERE predicate
 */
void
ApplySlidingWindow(SelectStmt *stmt, DefElem *sw, int *ttl)
{
	A_Expr *where;
	FuncCall *clock_ts;
	TypeCast *interval;
	A_Const *arg;
	A_Expr *rexpr;
	ColumnRef *arrival_ts;
	RangeVar *sw_cv;

	if (has_clock_timestamp(stmt->whereClause, NULL))
		elog(ERROR, "cannot specify both \"sw\" and a sliding window expression in the WHERE clause");

	sw_cv = GetSWContinuousViewRangeVar(stmt->fromClause);
	if (sw_cv == NULL && !PipelineContextIsDDL())
		elog(ERROR, "\"sw\" can only be specified when reading from a stream or continuous view");

	if (!IsA(sw->arg, String))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"sw\" must be a valid interval string"),
				 errhint("For example, ... WITH (sw = '1 hour') ...")));

	arg = makeNode(A_Const);
	arg->val = *((Value *) sw->arg);

	interval = makeNode(TypeCast);
	interval->arg = (Node *) arg;
	interval->typeName = SystemTypeName("interval");

	/*
	 * If we are creating a view on top a continuous view, we must ensure that the step_size of the
	 * continuous view is at most MIN_VIEW_MAX_AGE_FACTOR of the sw being specified.
	 */
	if (sw_cv)
	{
		Interval *sw_interval = GetSWInterval(sw_cv);
		int step_factor = QueryGetSWStepFactor(GetContWorkerQuery(sw_cv));
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
					errmsg("\"sw\" value is too small"),
					errhint("\"sw\" must be at least twice as much as the step size of the base continuous view.")));
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

	if (ttl)
	{
		Interval *i = parse_node_to_interval((Node *) interval);
		*ttl = IntervalToEpoch(i);
	}
}

/*
 * ApplyStorageOptions
 */
void
ApplyStorageOptions(CreateContViewStmt *stmt, bool *has_sw, int *ttl, char **ttl_column)
{
	DefElem *def;
	SelectStmt *select = (SelectStmt *) stmt->query;

	Assert(has_sw);

	/* sw */
	def = GetContinuousViewOption(stmt->into->options, OPTION_SLIDING_WINDOW);
	if (def)
	{
		*has_sw = true;
		ApplySlidingWindow(select, def, ttl);
		stmt->into->options = list_delete(stmt->into->options, def);
	}

	/* step_factor */
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

		stmt->into->options = list_delete(stmt->into->options, def);
		PipelineContextSetStepFactor(factor);
	}
	else if (*has_sw)
	{
		PipelineContextSetStepFactor(sliding_window_step_factor);
	}
}

AttrNumber
FindSWTimeColumnAttrNo(SelectStmt *viewselect, Oid matrelid, int *ttl)
{
	ContAnalyzeContext context;
	char *colname;
	Relation rel;
	TupleDesc desc;
	int i;
	AttrNumber attno = InvalidAttrNumber;

	if (!has_clock_timestamp(viewselect->whereClause, NULL))
		return attno;

	context.cols = context.types = NIL;
	collect_cols(viewselect->whereClause, &context);

	Assert(list_length(context.cols) == 1);
	colname = FigureColname(linitial(context.cols));

	rel = heap_open(matrelid, AccessShareLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = desc->attrs[i];
		if (pg_strcasecmp(colname, NameStr(attr->attname)) == 0)
		{
			attno = i + 1;
			break;
		}
	}

	heap_close(rel, AccessShareLock);

	Assert(AttributeNumberIsValid(attno));

	if (ttl)
	{
		Node *expr;
		Interval *i;

		find_clock_timestamp_expr(viewselect->whereClause, &context);
		Assert(IsA(context.expr, A_Expr));

		/* arrival_timestamp > clock_timestamp() - interval... */
		expr = ((A_Expr *) context.expr)->rexpr;
		Assert(IsA(expr, A_Expr));

		expr = ((A_Expr *) expr)->rexpr;

		i = parse_node_to_interval(expr);
		*ttl = IntervalToEpoch(i);
	}

	return attno;
}

/*
 * FindTTLColumnAttrNo
 */
AttrNumber
FindTTLColumnAttrNo(char *colname, Oid matrelid)
{
	AttrNumber attno = InvalidAttrNumber;
	Relation rel;
	TupleDesc desc;
	int i;

	if (!colname)
		return InvalidAttrNumber;

	rel = heap_open(matrelid, AccessShareLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute attr = desc->attrs[i];
		if (pg_strcasecmp(colname, NameStr(attr->attname)) == 0)
		{
			attno = i + 1;
			break;
		}
	}

	heap_close(rel, AccessShareLock);

	Assert(AttributeNumberIsValid(attno));
	return attno;
}

/*
 * delete_continuous_transform_options
 */
static void
delete_continuous_transform_options(ViewStmt *stmt)
{
	ListCell *lc;
	List *to_delete = NIL;

	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, OPTION_ACTION) == 0 ||
				pg_strcasecmp(def->defname, OPTION_OUTPUTFUNC) == 0)
		{
			to_delete = lappend(to_delete, def);
		}
	}

	foreach(lc, to_delete)
		stmt->options = list_delete(stmt->options, lfirst(lc));
}

/*
 * LookupOutputFunc
 */
Oid
LookupOutputFunc(List *name)
{
	Oid fargtypes[1];
	Oid tgfnid = InvalidOid;
	Oid funcrettype = InvalidOid;

	if (name)
	{
		tgfnid = LookupFuncName(name, 0, fargtypes, false);
		funcrettype = get_func_rettype(tgfnid);
		if (funcrettype != TRIGGEROID)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
					errmsg("function %s must return type \"trigger\"",
					NameListToString(name))));
	}

	return tgfnid;
}

/*
 * DeparseOutputFuncArgs
 */
char *
DeparseOutputFuncArgs(List *args)
{
	bytea *tgbytes;
	char *tgs;

	if (list_length(args))
	{
		ListCell *lc;
		char *argsbytes;
		int	len = 0;

		foreach(lc, args)
		{
			char *ar = strVal(lfirst(lc));

			len += strlen(ar) + 4;
			for (; *ar; ar++)
			{
				if (*ar == '\\')
					len++;
			}
		}

		argsbytes = (char *) palloc(len + 1);
		argsbytes[0] = '\0';

		foreach(lc, args)
		{
			char *s = strVal(lfirst(lc));
			char *d = argsbytes + strlen(argsbytes);

			while (*s)
			{
				if (*s == '\\')
					*d++ = '\\';
				*d++ = *s++;
			}
			strcpy(d, "\\000");
		}

		tgbytes = (bytea *) DirectFunctionCall1(byteain, CStringGetDatum(argsbytes));
		tgs = (char *) DirectFunctionCall1(byteaout, (Datum) tgbytes);
	}
	else
	{
		/* No arguments to output function */
		tgbytes = (bytea *) DirectFunctionCall1(byteain, CStringGetDatum(""));
		tgs = (char *) DirectFunctionCall1(byteaout, (Datum) tgbytes);
	}

	return tgs;
}

/*
 * AnalyzeCreateViewForTransform
 */
void
AnalyzeCreateViewForTransform(ViewStmt *stmt)
{
	ListCell *lc;
	TypeName *t = NULL;
	List *func = NIL;
	List *args = NIL;

	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, OPTION_OUTPUTFUNC) == 0 && IsA(def->arg, TypeName))
			t = (TypeName *) def->arg;
	}

	if (t != NULL)
	{
		ListCell *mlc;

		func = t->names;
		foreach(mlc, t->typmods)
		{
			A_Const *v;

			if (!IsA(lfirst(mlc), A_Const))
				elog(ERROR, "invalid output function argument");

			v = (A_Const *) lfirst(mlc);
			if (v->val.type != T_String)
				elog(ERROR, "invalid output function argument");

			args = lappend(args, &v->val);
		}
	}

	delete_continuous_transform_options(stmt);

	if (func)
	{
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGFN, (Node *) makeString(NameListToString(func))));
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGNARGS, (Node *) makeInteger(list_length(args))));
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGARGS, (Node *) makeString(DeparseOutputFuncArgs(args))));
	}
}

post_parse_analyze_hook_type SavePostParseAnalyzeHook = NULL;

/*
 * PostParseAnalyzeHook
 */
void
PostParseAnalyzeHook(ParseState *pstate, Query *query)
{
	if (QueryIsContinuous(query))
		query->targetList = transformContSelectTargetList(pstate, query->targetList);

	if (query->commandType == CMD_SELECT)
		QuerySetSWStepFactor(query, PipelineContextGetStepFactor());
}

#define PIPELINE_EXECUTING_DDL 	0x1
#define COMBINER_LOOKUP_PLAN 		0x2
#define DDL_HAS_STEP_FACTOR 			0x4

static int pipeline_context_flags = 0;
static int pipeline_context_step_factor = 0;

/*
 * PipelineContextSetIsDDL
 *
 * Indicate that a CREATE CONTINUOUS statement is currently being executed
 */
void
PipelineContextSetIsDDL(void)
{
	pipeline_context_flags |= PIPELINE_EXECUTING_DDL;
}

/*
 * PipelineContextSetStepFactor
 */
void
PipelineContextSetStepFactor(double sf)
{
	pipeline_context_flags |= DDL_HAS_STEP_FACTOR;
	pipeline_context_step_factor = sf;
}

double
PipelineContextGetStepFactor(void)
{
	if ((pipeline_context_flags & DDL_HAS_STEP_FACTOR) == 0)
		return 0.0;
	return pipeline_context_step_factor;
}

/*
 * PipelineContextIsDDL
 *
 * Determine if a CREATE CONTINUOUS statement is currently executing
 */
bool
PipelineContextIsDDL(void)
{
	return (pipeline_context_flags & PIPELINE_EXECUTING_DDL) > 0;
}

/*
 * PipelineContextSetCombinerLookup
 *
 * Indicate that a combiner groups lookup plan is currently being planned
 */
void
PipelineContextSetCombinerLookup(void)
{
	pipeline_context_flags |= COMBINER_LOOKUP_PLAN;
}

/*
 * PipelineContextIsCombinerLookup
 *
 * Determine if a combiner groups lookup plan is currently being planned
 */
bool
PipelineContextIsCombinerLookup(void)
{
	return (pipeline_context_flags & COMBINER_LOOKUP_PLAN) > 0;
}

/*
 * ClearPipelineContext
 *
 * Unset all current context flags
 */
void
ClearPipelineContext(void)
{
	pipeline_context_flags = 0;
	pipeline_context_step_factor = 0;
}
