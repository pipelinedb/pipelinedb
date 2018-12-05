/*-------------------------------------------------------------------------
 *
 * analyzer.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "analyzer.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog.h"
#include "compat.h"
#include "config.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "matrel.h"
#include "miscadmin.h"
#include "mutator.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "rewrite/rewriteHandler.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "pipeline_combine.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "ruleutils.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

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

GetCombineTargetFunc GetCombineTargetHook = NULL;

/*
 * Maps standard aggregates to their streaming variants for use within CQs.
 * There are two type of aggregates we substitute here:
 *
 * 1) Standard aggregates that are not combinable. For these, we just have our
 *    own combinable versions that are otherwise identical to the standard version.
 *
 * 2) Standard aggregates that aren't feasible to run on infinitely large inputs
 *    and thus require some kind of approximation implementation (e.g. COUNT DISTINCT).
 *
 * Perhaps we'll use a catalog for this one day, but this seems
 * sufficient for now, for simplicity's sake.
 */
typedef struct StreamingAggVariant
{
	char *name;
	char *variant_name;
	bool distinct_only;
} StreamingAggVariant;

static StreamingAggVariant StreamingVariants[] = {
	{"array_agg", "combinable_array_agg", false},
	{"array_agg", "set_agg", true},
	{"count", "hll_count_distinct", true},
	{"json_agg", "combinable_json_agg", false},
	{"json_object_agg", "combinable_json_object_agg", false},
	{"jsonb_agg", "combinable_jsonb_agg", false},
	{"jsonb_object_agg", "combinable_jsonb_object_agg", false},
	{"string_agg", "combinable_string_agg", false},
	{"rank", "combinable_rank", false},
	{"dense_rank", "combinable_dense_rank", false},
	{"percent_rank", "combinable_percent_rank", false},
	{"percentile_cont", "combinable_percentile_cont", false},
	{"cume_dist", "combinable_cume_dist", false},
};

#define StreamingVariantsSize	((int) lengthof(StreamingVariants))

typedef struct ReplaceNodeContext
{
	Node *new;
	Node *old;
	Size size;
} ReplaceNodeContext;

static post_parse_analyze_hook_type save_post_parse_analyze_hook = NULL;

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
	char *name = NameListToString(fn->funcname);

	for (i = 0; i < StreamingVariantsSize; i++)
	{
		StreamingAggVariant variant = StreamingVariants[i];

		if (pg_strcasecmp(variant.name, name) == 0 && variant.distinct_only == fn->agg_distinct)
			return variant.variant_name;
	}

	return NULL;
}

/*
 * lappend_missing
 */
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
 * col_has_index
 *
 * Checks if the column of the given relation belongs to an index
 */
static bool
col_has_index(RangeVar *rv, ColumnRef *col)
{
	Relation rel = heap_openrv(rv, NoLock);
	Bitmapset *indexed = RelationGetIndexAttrBitmap(rel, PIPELINE_COMPAT_INDEX_ATTR_BITMAP_ALL);
	TupleDesc desc = RelationGetDescr(rel);
	char *table;
	char *colname;
	int i;

	heap_close(rel, NoLock);
	DeconstructQualifiedName(col->fields, &table, &colname);

	for (i=0; i<desc->natts; i++)
	{
		AttrNumber attno = TupleDescAttr(desc, i)->attnum - FirstLowInvalidHeapAttributeNumber;
		if (pg_strcasecmp(NameStr(TupleDescAttr(desc, i)->attname), colname) == 0 &&
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

/*
 * get_truncation_from_interval
 */
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
 * collect_column_names
 */
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

/*
 * get_query_state
 */
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
	if (!IsInParallelMode() && TransactionIdIsValid(query_cache_xid))
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

/*
 * query_is_continuous_walker
 */
static bool
query_is_continuous_walker(Node *node, IsContinuousContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *ref = (RangeTblRef *) node;
		RangeTblEntry *rte = rt_fetch(ref->rtindex, context->query->rtable);

		if (RelidIsStream(rte->relid))
		{
			context->isContinuous = true;
			return false;
		}
		else if (rte->relkind == RELKIND_RELATION && RelidIsMatRel(rte->relid, NULL) &&
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
		 Node *nfn;
		 char *cv_name;

		 /* Functions here are represented by a lists of length 2 */
		 if (list_length(rf->functions) != 1 || list_length(linitial(rf->functions)) != 2)
			 return from;

		 nfn = linitial(linitial(rf->functions));
		 if (!IsA(nfn, FuncCall))
			 return from;

		 fn = (FuncCall *) nfn;
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
 * contains_node
 */
static bool
contains_node(Node *node, Node *child)
{
	if (node == NULL || child == NULL)
		return false;

	if (equal(node, child))
		return true;

	return raw_expression_tree_walker(node, contains_node, (void *) child);
}

/*
 * collect_funcs
 */
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

/*
 * collect_agg_funcs
 */
static bool
collect_agg_funcs(Node *node, ContAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
	{
		FuncCall *func = (FuncCall *) node;
		FuncCandidateList clist;
		bool is_agg = func->agg_within_group;

		if (!is_agg)
		{
			clist = FuncnameGetCandidates(func->funcname, list_length(func->args), NIL, true, false, true);
			while (clist)
			{
				is_agg = CompatProcOidIsAgg(clist->oid);

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

/*
 * collect_windows
 */
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
 * get_cont_query_select_stmt
 */
static SelectStmt *
get_cont_query_select_stmt(RangeVar *rv, double *step_factor)
{
	HeapTuple tup;
	char *sql;
	Query *query;
	SelectStmt *select;
	Form_pipeline_query row;
	RawStmt *raw;

	tup = GetPipelineQueryTuple(rv);

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "continuous view \"%s\" does not exist", rv->relname);

	row = (Form_pipeline_query) GETSTRUCT(tup);
	query = GetContQueryDef(row->defrelid);

	sql = deparse_query_def(query);
	raw = (RawStmt *) linitial(pg_parse_query(sql));
	select = (SelectStmt *) raw->stmt;

	if (step_factor)
		*step_factor = (double) row->step_factor;

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
	RawStmt *raw;

	sel = get_cont_query_select_stmt(rv, NULL);
	raw = makeNode(RawStmt);
	raw->stmt = (Node *) sel;

	return parse_analyze(raw, "SELECT", NULL, 0, NULL);
}

/*
 * RangeVarGetContWorkerQuery
 */
Query *
RangeVarGetContWorkerQuery(RangeVar *rv)
{
	ContQuery *cq = RangeVarGetContQuery(rv);

	return GetContWorkerQuery(cq);
}

/*
 * RangeVarGetContCombinerQuery
 */
Query *
RangeVarGetContCombinerQuery(RangeVar *rv)
{
	SelectStmt *sel;
	RangeVar *matrel;
	RawStmt *raw;
	double sf;

	matrel = RangeVarGetMatRelName(rv);
	sel = get_cont_query_select_stmt(rv, &sf);
	sel = TransformSelectStmtForContProcess(matrel, sel, NULL, sf, Combiner);

	raw = makeNode(RawStmt);
	raw->stmt = (Node *) sel;

	return parse_analyze(raw, "SELECT", NULL, 0, NULL);
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
		if (RangeVarIsStream((RangeVar *) node, true))
			context->streams = lappend(context->streams, node);
		else
			context->rels = lappend(context->rels, node);

		return false;
	}

	return raw_expression_tree_walker(node, collect_rels_and_streams, (void *) context);
}

/*
 * QueryHasStream
 */
bool
QueryHasStream(Node *node)
{
	SelectStmt *stmt;
	ContAnalyzeContext cxt;

	if (!IsA(node, SelectStmt))
		return false;

	/* We need to make sure output_of is transformed before checking for streams */
	stmt = (SelectStmt *) copyObject(node);
	RewriteFromClause(stmt);

	MemSet(&cxt, 0, sizeof(ContAnalyzeContext));
	collect_rels_and_streams((Node *) stmt->fromClause, &cxt);

	/*
	 * Set operations are currently unsupported on CVs, but we'll want to collect
	 * any streams here anyways so that we know this is in fact a CV definition, and
	 * we can fail properly later on.
	 */
	if (stmt->op)
	{
		collect_rels_and_streams((Node *) stmt->larg, &cxt);
		collect_rels_and_streams((Node *) stmt->rarg, &cxt);
	}

	if (cxt.streams)
		return true;

	return false;
}

/*
 * MakeContAnalyzeContext
 */
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
 * QueryIsContinuous
 */
bool
QueryIsContinuous(Query *query)
{
	IsContinuousContext context;

	if (PipelineContextIsDefRel())
		return false;

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
	RawStmt *raw;

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

	raw = makeNode(RawStmt);
	raw->stmt = copyObject(node);

	/* Pass it to the analyzer to make sure all function definitions, etc. are correct */
	parse_analyze(raw, sql, NULL, 0, NULL);

	/* Warn the user if it's a stream-table join with an unindexed join qual */
	warn_unindexed_join(select, context);
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
	RawStmt *raw;
	Query *q;

	/* the grammar should handle this one, but just to be safe... */
	if (!IsA(subquery, SelectStmt))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("%s must be SELECT statements", objdesc)));

	stmt = (SelectStmt *) copyObject(subquery);

	raw = makeNode(RawStmt);
	raw->stmt = (Node *) stmt;
	q = parse_analyze(raw, "subquery", NULL, 0, NULL);

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
 * RewriteFromClause
 */
void
RewriteFromClause(SelectStmt *stmt)
{
	if (PipelineContextIsDDL() && stmt->op)
		elog(ERROR, "set operations within continuous views are currently not supported");
	stmt->fromClause = (List *) rewrite_from_clause((Node *) stmt->fromClause);
}

/*
 * parse_node_to_interval
 */
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
	d = ExecEvalExpr(estate, econtext, &isnull);

	free_parsestate(ps);
	FreeExprContext(econtext, false);

	return DatumGetIntervalP(d);
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

/*
 * GetSWTimeColumn
 */
ColumnRef *
GetSWTimeColumn(RangeVar *rv)
{
	ContQuery *view;
	Relation rel;
	ColumnRef *col;
	TupleDesc desc;
	Form_pg_attribute attr;

	view = RangeVarGetContView(rv);
	rel = heap_open(view->matrelid, AccessShareLock);
	desc = RelationGetDescr(rel);

	Assert(AttributeNumberIsValid(view->sw_attno));
	attr = TupleDescAttr(desc, view->sw_attno - 1);

	col = makeNode(ColumnRef);
	col->fields = list_make1(makeString(pstrdup(NameStr(attr->attname))));

	heap_close(rel, AccessShareLock);

	return col;
}

/*
 * GetWindowTimeColumn
 */
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
 * GetContQueryOption
 */
DefElem *
GetContQueryOption(List *options, char *name)
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
	DefElem *def = GetContQueryOption(options, option);

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
#if (PG_VERSION_NUM < 110000)
		appendStringInfo(&buf, "%ld", intVal(def->arg));
#else
		appendStringInfo(&buf, "%d", intVal(def->arg));
#endif

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
	DefElem *def = GetContQueryOption(options, option);

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
 * GetOptionAsDouble
 */
bool
GetOptionAsDouble(List *options, char *option, double *result)
{
	DefElem *def = GetContQueryOption(options, option);

	if (!def)
		return false;

  if (IsA(def->arg, Integer))
		*result = (double) intVal(def->arg);
	else if (IsA(def->arg, Float))
		*result = floatVal(def->arg);
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
 */
void
ApplySlidingWindow(SelectStmt *stmt, DefElem *sw, DefElem *column, int *ttl)
{
	A_Expr *where;
	FuncCall *clock_ts;
	TypeCast *interval;
	A_Const *arg;
	A_Expr *rexpr;
	ColumnRef *ts_col;
	RangeVar *sw_cv;

	if (has_clock_timestamp(stmt->whereClause, NULL))
		elog(ERROR, "cannot specify both \"sw\" and a sliding window expression in the WHERE clause");

	sw_cv = GetSWContViewRelName(stmt->fromClause);
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
		int step_factor = QueryGetSWStepFactor(RangeVarGetContWorkerQuery(sw_cv));
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


	ts_col = makeNode(ColumnRef);

	if (column)
	{
		/*
		 * Note: if this is an invalid column we'll fail later on in a more appropriate place
		 */
		ts_col->fields = list_make1(column->arg);
	}
	else
	{
		/*
		 * We default to arrival_timestamp since every stream has it
		 */
		ts_col->fields = list_make1(makeString(ARRIVAL_TIMESTAMP));
	}

	clock_ts = makeNode(FuncCall);
	clock_ts->funcname = list_make1(makeString(CLOCK_TIMESTAMP));

	rexpr = makeA_Expr(AEXPR_OP, list_make1(makeString("-")), (Node *) clock_ts, (Node *) interval, -1);
	where = makeA_Expr(AEXPR_OP, list_make1(makeString(">")), (Node *) ts_col, (Node *) rexpr, -1);

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
List *
ApplyStorageOptions(SelectStmt *select, List *options, bool *has_sw, int *ttl, char **ttl_column)
{
	DefElem *def;

	Assert(has_sw);

	/* sw */
	def = GetContQueryOption(options, OPTION_SLIDING_WINDOW);
	if (def)
	{
		DefElem *col;

		*has_sw = true;
		col = GetContQueryOption(options, OPTION_SLIDING_WINDOW_COLUMN);

		if (col && !IsA(col->arg, String))
		 elog(ERROR, "\"%s\" must be a valid column name specified as a string", OPTION_SLIDING_WINDOW_COLUMN);

		ApplySlidingWindow(select, def, col, ttl);
		options = list_delete(options, def);
	}
	else if (GetContQueryOption(options, OPTION_SLIDING_WINDOW_COLUMN))
	{
		elog(ERROR, "\"%s\" must be passed in conjunction with the \"%s\" option", OPTION_SLIDING_WINDOW_COLUMN, OPTION_SLIDING_WINDOW);
	}

	/* step_factor */
	def = GetContQueryOption(options, OPTION_STEP_FACTOR);
	if (def)
	{
		double factor;

		if (!has_clock_timestamp(select->whereClause, NULL))
			elog(ERROR, "can only specify \"step_factor\" for sliding window queries");

		if (IsA(def->arg, Integer))
			factor = intVal(def->arg);
		else if (IsA(def->arg, String))
			factor = floatVal(def->arg);
		else if (IsA(def->arg, Float))
			factor = 100 * floatVal(def->arg);
		else
			elog(ERROR, "failed to parse \"step_factor\"");

		if (factor <= 0 || factor > 50)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"step_factor\" must be a valid float in the range 0..50"),
					 errhint("For example, ... WITH (step_factor = 25) ...")));

		options = list_delete(options, def);
		PipelineContextSetStepFactor(factor);
	}
	else if (*has_sw)
	{
		PipelineContextSetStepFactor(sliding_window_step_factor);
	}

	return options;
}

/*
 * FindSWTimeColumnAttrNo
 */
AttrNumber
FindSWTimeColumnAttrNo(SelectStmt *viewselect, Oid matrelid, int *ttl, char **ttl_column)
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
		Form_pg_attribute attr = TupleDescAttr(desc, i);
		if (pg_strcasecmp(colname, NameStr(attr->attname)) == 0)
		{
			attno = i + 1;
			*ttl_column = pstrdup(NameStr(attr->attname));
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
		Form_pg_attribute attr = TupleDescAttr(desc, i);
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

#define PIPELINE_EXECUTING_DDL 	0x1
#define COMBINER_LOOKUP_PLAN 		0x2
#define DDL_HAS_STEP_FACTOR 			0x4
#define IS_COMBINE_TABLE   			0x8
#define PIPELINE_IS_DEFREL				0x10
#define PIPELINE_CONT_PLAN 			0x20

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
 * PipelineContextSetIsDefRel
 *
 * Indicate that we're currently defining a CQ's defrel
 */
void
PipelineContextSetIsDefRel(bool defrel)
{
	if (defrel)
		pipeline_context_flags |= PIPELINE_IS_DEFREL;
	else
		pipeline_context_flags &= (~PIPELINE_IS_DEFREL);
}

/*
 * PipelineContextIsDefRel
 *
 * Determine if we're currently defining a CQ's defrel
 */
bool
PipelineContextIsDefRel(void)
{
	return (pipeline_context_flags & PIPELINE_IS_DEFREL) > 0;
}

/*
 * PipelineContextSetCombineTable
 *
 * indicate that pipelinedb.combine_table is being run
 */
void
PipelineContextSetCombineTable(void)
{
	pipeline_context_flags |= IS_COMBINE_TABLE;
}

/*
 * PipelineContextSetContPlan
 */
void
PipelineContextSetContPlan(bool cp)
{
	if (cp)
		pipeline_context_flags |= PIPELINE_CONT_PLAN;
	else
		pipeline_context_flags &= (~PIPELINE_CONT_PLAN);
}

/*
 * PipelineContextIsStreamScanAllowed
 */
bool
PipelineContextIsContPlan(void)
{
	return (pipeline_context_flags & PIPELINE_CONT_PLAN) > 0;
}


/*
 * PipelineContextIsDDL
 *
 * Determine if pipelinedb.combine_table is currently being rnu
 */
bool
PipelineContextIsCombineTable(void)
{
	return (pipeline_context_flags & IS_COMBINE_TABLE) > 0;
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

/*
 * Rewrite ordered- and hypothetical-set aggregates to use our own streaming variants that are implemented as regular aggregates
 *
 * Standard OS/HS aggregates in PG are not combinable so we have our own streaming variants that are. These basically get rewritten from:
 *
 * agg(const) WITHIN GROUP (ORDER BY x) ...
 *
 * to:
 *
 * agg(const, x) ...
 */
static void
rewrite_within_group_agg(FuncCall *fn)
{
	ListCell *lc;

	Assert(fn->agg_within_group);
	Assert(fn->agg_order);

	foreach(lc, fn->agg_order)
	{
		SortBy *sb = (SortBy *) lfirst(lc);
		if (IsA(sb->node, RowExpr))
			elog(ERROR, "argument type record is unsupported");
		fn->args = lappend(fn->args, sb->node);
	}

	fn->agg_order = NIL;
	fn->agg_within_group = false;
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

			if (fn->agg_within_group)
				rewrite_within_group_agg(fn);
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

/*
 * is_res_target_for_node
 */
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

/*
 * node_for_cref
 */
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

/*
 * no_tc_equal
 */
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

/*
 * find_node_in_target_list
 */
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

/*
 * get_unique_colname
 */
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

/*
 * create_unique_res_target
 */
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

/*
 * create_agg_node_for_view_overlay
 */
static Node *
create_agg_node_for_view_overlay(ColumnRef *cref, FuncCall *workeragg, ContAnalyzeContext *context)
{
	if (context->view_combines)
	{
		FuncCall *combine = makeNode(FuncCall);
		combine->funcname = list_make1(makeString(SW_COMBINE));
		combine->args = list_make1(cref);
		/* Copy over any OVER clause from the worker. */
		combine->over = workeragg->over;

		return (Node *) combine;
	}

	return (Node *) cref;
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

/*
 * truncate_timestamp_field
 */
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

/*
 * hoist_time_node
 */
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

/*
 * proj_and_group_for_sliding_window
 */
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

	view->whereClause = (Node *) copyObject(sw_expr);

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
		 * cont query. We have carefully replaced all aggregates and column refs in the expression
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

/*
 * pull_var_and_aggs_walker
 */
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

/*
 * pull_var_and_aggs
 */
static List *
pull_var_and_aggs(Node *node)
{
	List *varlist = NIL;
	pull_var_and_aggs_walker(node, &varlist);
	return varlist;
}

/*
 * transform_cont_select_tlist
 */
static List *
transform_cont_select_tlist(ParseState *pstate, List *tlist)
{
	ListCell *lc;
	List *nodes = pull_var_and_aggs((Node *) tlist);

	foreach(lc, nodes)
	{
		Node *n = lfirst(lc);
		Aggref *agg;
		HeapTuple tup;
		Form_pg_aggregate form;

		if (!IsA(n, Aggref))
			continue;

		agg = (Aggref *) n;

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid));
		if (!HeapTupleIsValid(tup))
			elog(ERROR, "cache lookup failed for aggregate %u", agg->aggfnoid);

		form = (Form_pg_aggregate) GETSTRUCT(tup);
		agg->aggtranstype = form->aggtranstype;

		if (agg->aggtranstype == INTERNALOID)
			agg->aggtype = BYTEAOID;
		else
			agg->aggtype = agg->aggtranstype;

		ReleaseSysCache(tup);
	}

	return tlist;
}

/*
 * validate_agg
 */
static void
validate_agg(Node *node)
{
	if (IsA(node, Aggref))
	{
		Aggref *agg = (Aggref *) node;
		if (agg->aggdirectargs)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_DEFINITION),
					 errmsg("WITHIN GROUP is not supported by continuous views"),
					 errhint("Try using PipelineDB's streaming variant of this aggregate")));
		}
	}
}

/*
 * get_type_name
 */
static char *
get_type_name(Oid typeid)
{
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeid));
	char *nsp;
	char *name;
	char *result;
	Form_pg_type form;

	if (!HeapTupleIsValid(tup))
		return NULL;

	form = (Form_pg_type) GETSTRUCT(tup);
	nsp = get_namespace_name(form->typnamespace);
	name = NameStr(form->typname);

	result = NameListToString(list_make2(makeString(nsp), makeString(name)));
	ReleaseSysCache(tup);

	return result;
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

/*
 * get_anyarray_result_type
 *
 * Given an aggregate with an anyarray return type,
 * determine the actual array type of the final result
 */
static Oid
get_anyarray_result_type(Aggref *agg)
{
	HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(agg->aggfnoid));
	Form_pg_proc proc;
	Node *arg;
	TargetEntry *te;
	Oid result;
	int polyarg = -1;
	int i;

  Assert(HeapTupleIsValid(tup));
	proc = (Form_pg_proc) GETSTRUCT(tup);

	for (i = 0; i < proc->pronargs; i++)
	{
		if (proc->proargtypes.values[i] == ANYELEMENTOID ||
				proc->proargtypes.values[i] == ANYARRAYOID ||
				proc->proargtypes.values[i] == ANYNONARRAYOID ||
				proc->proargtypes.values[i] == ANYOID ||
				proc->proargtypes.values[i] == ANYNONARRAYOID)
		{
			polyarg = i;
			break;
		}
	}

	ReleaseSysCache(tup);

	if (polyarg == -1)
		elog(ERROR, "no polymorphic arguments found");

	if (polyarg >= list_length(agg->args))
		elog(ERROR, "expected polymorphic argument at position %u but only found %u arguments",
				polyarg + 1, list_length(agg->args));

	arg = (Node *) list_nth(agg->args, polyarg);
	Assert(IsA(arg, TargetEntry));

	te = (TargetEntry *) arg;
	result = exprType((Node *) te->expr);

	/*
	 * If we've already determined a specific array type, we're done
	 */
	if (type_is_array_domain(result))
		return result;

	/*
	 * Type is currently a non-array type, so we just need to look up
	 * its corresponding array type if it has one
	 */
	result = get_array_type(result);

	if (!OidIsValid(result))
		elog(ERROR, "no array type found for type %u", exprType((Node *) te->expr));

	return result;
}

/*
 * get_agg_name
 */
static A_Const *
get_agg_name(Oid aggfnoid)
{
	HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
	Form_pg_proc proc;
	char *nsp;
	A_Const *result;

	Assert(HeapTupleIsValid(tup));
	proc = (Form_pg_proc) GETSTRUCT(tup);

	nsp = get_namespace_name(proc->pronamespace);
	Assert(nsp);

	ReleaseSysCache(tup);

	result = makeNode(A_Const);
	result->val.type = T_String;
	result->val.val.str = NameListToString(list_make2(makeString(nsp), makeString(NameStr(proc->proname))));

	return result;
}

/*
 * get_agg_arg_typenames
 */
static TypeCast *
get_agg_arg_typenames(Oid aggfnoid)
{
	HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(aggfnoid));
	Form_pg_proc proc;
	int i;
	ArrayType *arr;
	Datum *values;
	FmgrInfo flinfo;
	char *str;
	A_Const *aconst;
	TypeCast *tc;

	Assert(HeapTupleIsValid(tup));
	proc = (Form_pg_proc) GETSTRUCT(tup);

	values = palloc0(sizeof(Datum) * proc->pronargs);

	/*
	 * We just build an actual array of qualified type names we can pass to array_out
	 * instead of duplicating the array stringification logic here
	 */
	for (i = 0; i < proc->pronargs; i++)
	{
		char *name = get_type_name(proc->proargtypes.values[i]);
		Assert(name);
		values[i] = CStringGetTextDatum(name);
	}

	arr = construct_array(values, proc->pronargs, TEXTOID, -1, false, 'i');

	MemSet(&flinfo, 0, sizeof(FmgrInfo));
	flinfo.fn_mcxt = CurrentMemoryContext;

	str = (char *) CallerFInfoFunctionCall1(array_out, &flinfo, InvalidOid, (Datum) arr);

	ReleaseSysCache(tup);

	aconst = makeNode(A_Const);
	aconst->val.type = T_String;
	aconst->val.val.str = str;

	tc = makeNode(TypeCast);
	tc->typeName = SystemTypeName("text");

	/*
	 * This indicates it's an array type, so ultimately our cast is ::text[]
	 */
	tc->typeName->arrayBounds = list_make1(makeInteger(-1));
	tc->arg = (Node *) aconst;

	return tc;
}

/*
 * make_finalize_call
 */
static Node *
make_finalize_call(Aggref *agg, ColumnRef *cref)
{
	HeapTuple tup;
	Form_pg_aggregate aggform;
	Form_pg_proc finalform;
	Oid finalfn = InvalidOid;
	Oid deserfn = InvalidOid;
	Oid transtype = InvalidOid;
	FuncCall *result;
	List *name = NIL;
	List *args = NIL;
	int i;

	tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for aggregate %u", agg->aggfnoid);

	aggform = (Form_pg_aggregate) GETSTRUCT(tup);
	finalfn = aggform->aggfinalfn;
	transtype = aggform->aggtranstype;
	deserfn = aggform->aggdeserialfn;

	ReleaseSysCache(tup);

	if (!OidIsValid(finalfn))
		return (Node *) cref;

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(finalfn));
	if (!HeapTupleIsValid(tup))
		elog(ERROR, "cache lookup failed for function %u", finalfn);

	finalform = (Form_pg_proc) GETSTRUCT(tup);
	name = list_make2(makeString(pstrdup(get_namespace_name(finalform->pronamespace))), makeString(pstrdup(NameStr(finalform->proname))));

	args = list_make1((Node *) cref);

	if (transtype == INTERNALOID && OidIsValid(deserfn))
	{
		/* Internal transition states must first be deserialized from their bytea on-disk representation */
		FuncCall *finalize;
		List *finalizeargs = NIL;
		A_Const *type_dummy;
		A_Const *agg_name;
		TypeCast *argtypes;
		TypeCast *type_arg;
		Oid type;

		/*
		 * For internal transition states, we'll need to deserialize the bytea transition state
		 * and pass the deserialized value to the final function.
		 *
		 * When this is done in overlay views, we can't call the deserialization functions directly
		 * from a user query, so we have our own wrapper that will call them directly at execution
		 * time.
		 *
		 * The deserialize/finalize wrapper has a signature of the form:
		 *
		 * pipelinedb.finalize(aggname, {argtypes}, bytea, anyelement) -> anyelement
		 *
		 * The result type is determined by the type of the polymorphic anyelement argument, so we
		 * generate a dummy NULL argument coerced to the final result type to ensure the types
		 * resolve properly.
		 */
		type_dummy = makeNode(A_Const);
		type_dummy->val.type = T_Null;

		type_arg = makeNode(TypeCast);
		type_arg->arg = (Node *) type_dummy;

		/*
		 * If the result type is anyarray, we determine the actual array type of the result
		 * by examining the Aggref's arguments since they'll have types at this point.
		 */
		if (finalform->prorettype == ANYARRAYOID)
			type = get_anyarray_result_type(agg);
		else
			type = finalform->prorettype;

		type_arg->typeName = typeStringToTypeName(get_type_name(type));
		type_arg->typeName->typeOid = type;

		agg_name = get_agg_name(agg->aggfnoid);
		argtypes = get_agg_arg_typenames(agg->aggfnoid);

		finalizeargs = list_make4(agg_name, argtypes, cref, type_arg);
		finalize = makeFuncCall(list_make2(makeString(PIPELINEDB_EXTENSION_NAME), makeString("finalize")), finalizeargs, -1);

		ReleaseSysCache(tup);

		return (Node *) finalize;
	}

	/*
	 * Create arguments to the final function in order to enable correct signature
	 * resolution downstream in the analyzer
	 */
	for (i=1; i<finalform->pronargs; i++)
	{
		Oid argtype;
		char *typname;
		A_Const *c;
		TypeCast *tc;

		/*
		 * Certain polymorphic final functions take the argument type of the aggregate
		 * in order to determine their own return type, so we use the aggregate arguments
		 * here with a TypeCast to make the type of the argument known.
		 */
		argtype = list_nth_oid(agg->aggargtypes, i - 1);
		typname = get_type_name(argtype);

		if (typname == NULL)
			elog(ERROR, "type %u not found", argtype);

		c = makeNode(A_Const);
		c->val.type = T_Null;

		tc = makeNode(TypeCast);
		tc->typeName = typeStringToTypeName(typname);
		tc->arg = (Node *) c;

		args = lappend(args, tc);
	}

	result = makeFuncCall(name, args, -1);

	ReleaseSysCache(tup);

	return (Node *) result;
}

/*
 * delete_unrecognized_options
 */
static void
delete_unrecognized_options(ViewStmt *stmt)
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
 * deparse_output_func_args
 */
static char *
deparse_output_func_args(List *args)
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
AnalyzeDumped(ViewStmt *stmt)
{
	delete_unrecognized_options(stmt);
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

	delete_unrecognized_options(stmt);

	if (func)
	{
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGFN, (Node *) makeString(NameListToString(func)), -1));
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGNARGS, (Node *) makeInteger(list_length(args)), -1));
		stmt->options = lappend(stmt->options, makeDefElem(OPTION_TGARGS, (Node *) makeString(deparse_output_func_args(args)), -1));
	}
}

/*
 * finalize_aggs_mutator
 */
static Node *
finalize_aggs_mutator(Node *node, Query *worker_query)
{
	if (node == NULL)
		return false;

	if (IsA(node, ColumnRef))
	{
		ColumnRef *cref = (ColumnRef *) node;
		char *colname = NameListToString(cref->fields);
		ListCell *lc;

		foreach(lc, worker_query->targetList)
		{
			TargetEntry *te = (TargetEntry *) lfirst(lc);
			if (!pg_strcasecmp(te->resname, colname))
			{
				/* We've found the column it references, so if it's an aggregate we need to finalize it */
				if (IsA(te->expr, Aggref))
					return make_finalize_call((Aggref *) te->expr, cref);
				break;
			}
		}

		return node;
	}

	return raw_expression_tree_mutator(node, finalize_aggs_mutator, worker_query);
}

/*
 * FinalizeOverlayAggrefs
 */
void
FinalizeOverlayStmtAggregates(SelectStmt *overlay, Query *worker_query)
{
	overlay->targetList = (List *) raw_expression_tree_mutator((Node *) overlay->targetList, finalize_aggs_mutator, worker_query);
}

/*
 * PostParseAnalyzeHook
 */
void
PostParseAnalyzeHook(ParseState *pstate, Query *query)
{
	if (save_post_parse_analyze_hook)
		save_post_parse_analyze_hook(pstate, query);

	if (IsBinaryUpgrade || !PipelineDBExists())
		return;

	if (QueryIsContinuous(query))
	{
		query->targetList = transform_cont_select_tlist(pstate, query->targetList);
		QuerySetSWStepFactor(query, PipelineContextGetStepFactor());
	}
}

/*
 * InstallAnalyzerHooks
 */
void
InstallAnalyzerHooks(void)
{
	save_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = PostParseAnalyzeHook;
}

/*
 * GetContQueryAction
 */
ContQueryAction
GetContQueryAction(ViewStmt *stmt)
{
	ListCell *lc;

	foreach(lc, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);
		if (pg_strcasecmp(def->defname, OPTION_ACTION) == 0)
		{
			Value *v;
			if (IsA(def->arg, String))
			{
				v = (Value *) def->arg;
			}
			else if (IsA(def->arg, TypeName))
			{
				TypeName *t = (TypeName *) def->arg;
				if (list_length(t->names) != 1)
					elog(ERROR, "invalid action");
				v = linitial(t->names);
			}
			else
			{
				elog(ERROR, "invalid action");
			}

			Assert(v);
			if (pg_strcasecmp(strVal(v), ACTION_TRANSFORM) == 0)
				return TRANSFORM;
			else if (pg_strcasecmp(strVal(v), ACTION_MATERIALIZE) == 0)
				return MATERIALIZE;
			else if (pg_strcasecmp(strVal(v), ACTION_DUMPED) == 0)
				return DUMPED;
			else
				elog(ERROR, "invalid action \"%s\"", strVal(v));
		}
	}

	/*
	 * If no action is specified but the query reads from a stream,
	 * we default to MATERIALIZE since it's the most common.
	 */
	if (QueryHasStream(stmt->query))
		return MATERIALIZE;

	return NONE;
}

/*
 * GetContViewOverlayStmt
 */
RawStmt*
GetContViewOverlayStmt(ContQuery *view)
{
	List *parsetree_list;
	SelectStmt *viewstmt;
	RawStmt *raw;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	raw = (RawStmt *) linitial(parsetree_list);
	TransformSelectStmtForContProcess(view->matrel, (SelectStmt *) raw->stmt, &viewstmt, view->sw_step_factor, Worker);

	raw->stmt = (Node *) viewstmt;

	return raw;
}

/*
 * GetContWorkerSelectStmt
 */
RawStmt *
GetContWorkerSelectStmt(ContQuery* view, SelectStmt** viewptr)
{
	List *parsetree_list;
	SelectStmt *selectstmt;
	RawStmt *result;

	parsetree_list = pg_parse_query(view->sql);
	Assert(list_length(parsetree_list) == 1);

	result = (RawStmt *) linitial(parsetree_list);
	selectstmt = TransformSelectStmtForContProcess(view->matrel, (SelectStmt *) result->stmt, viewptr, view->sw_step_factor, Worker);

	result->stmt = (Node *) selectstmt;

	return result;
}

/*
 * GetContWorkerQuery
 */
Query *
GetContWorkerQuery(ContQuery *view)
{
	RawStmt* stmt = GetContWorkerSelectStmt(view, NULL);
	Query *query = linitial(pg_analyze_and_rewrite(stmt, view->sql, NULL, 0, NULL));

	return query;
}

/*
 * Get the intial argument type of the aggregate function currently
 * being executed. This works for regular and windowed aggregates.
 */
Oid
AggGetInitialArgType(FunctionCallInfo fcinfo)
{
	List *args = NIL;
	Oid type;
	Node *node;
	Aggref *agg;

	if (fcinfo->context == NULL)
		return InvalidOid;

	agg = AggGetAggref(fcinfo);

	if (agg)
		args = agg->args;
	else
		elog(ERROR, "fcinfo must be an aggregate function call");

	node = linitial(args);
	if (IsA(node, TargetEntry))
		type = exprType((Node *) ((TargetEntry *) node)->expr);
	else
		type = exprType(node);

	return type;
}

/*
 * make_deserialization_call
 */
static Expr *
make_deserialization_call(Node *arg, Oid deseroid)
{
	HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(deseroid));
	FuncExpr *deser;
	List *args;
	Const *c;

	Assert(exprType(arg) == BYTEAOID);

	Assert(HeapTupleIsValid(tup));
	c = makeConst(REGPROCOID, -1, 0, 4, ObjectIdGetDatum(HeapTupleGetOid(tup)), false, true);
	args = list_make2(c, arg);
	deser = makeFuncExpr(GetDeserializeOid(), INTERNALOID, args, 0, 0, COERCE_EXPLICIT_CALL);

	ReleaseSysCache(tup);

	return (Expr *) deser;
}

/*
 * search_for_partial_combine_aggregate
 */
static Oid
search_for_partial_combine_aggregate(Oid combinefn, Oid transfn, Oid serialfn)
{
	HeapTuple	tup;
	Oid oid = InvalidOid;
	Form_pg_aggregate row;

	tup = PipelineCatalogLookup(PGAGGPARTIALCOMBINEFN, 4, ObjectIdGetDatum(combinefn), ObjectIdGetDatum(transfn), ObjectIdGetDatum(serialfn), ObjectIdGetDatum(serialfn));

	if (HeapTupleIsValid(tup))
	{
		row = (Form_pg_aggregate) GETSTRUCT(tup);
		oid = row->aggfnoid;
	}

	return oid;
}

/*
 * get_partial_combine_aggfnoid
 */
static Oid
get_partial_combine_aggfnoid(Oid aggfnoid, Oid *rettype)
{
	HeapTuple tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	Form_pg_aggregate aggrow;
	Form_pg_proc procrow;
	Oid result;

	Assert(rettype);
	*rettype = InvalidOid;

	Assert(HeapTupleIsValid(tup));
	aggrow = (Form_pg_aggregate) GETSTRUCT(tup);

	result = search_for_partial_combine_aggregate(aggrow->aggcombinefn, aggrow->aggtransfn, aggrow->aggserialfn);
	ReleaseSysCache(tup);

	if (!OidIsValid(result))
		elog(ERROR, "no partial combine aggregate found for combine aggregate %u", aggfnoid);

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(result));
	Assert(HeapTupleIsValid(tup));
	procrow = (Form_pg_proc) GETSTRUCT(tup);

	*rettype = procrow->prorettype;

	ReleaseSysCache(tup);

	return result;
}

/*
 * add_unfinalized_var
 *
 * Given a column reference, determine if it is a finalized overlay expression
 * and if so, add an unfinalized reference to the output to be used as an input
 * into a combine aggregate.
 */
static Var *
add_unfinalized_var(Index varno, AttrNumber attr, Query *q,
		bool toplevel, bool subquery, bool setops, Oid *matrelid, AttrNumber *cvattr, int32 *attrtypmod)
{
	RangeTblEntry *rte = rt_fetch(varno, q->rtable);

	if (rte->rtekind == RTE_RELATION)
	{
		/*
		 * The attribute is a reference to a relation column, so determine
		 * if it's a matrel. If so, the attribute is targeting a continuous view
		 * and we can proceed with possibly adding an unfinalized column reference
		 * if the attribute references a finalized value.
		 */
		TargetEntry *te;
		Var *combine_target;
		AttrNumber resno;
		List *args;
		Var *result;
		Node *arg;
		Oid relid = InvalidOid;

		if (!RelidIsMatRel(rte->relid, NULL))
		{
			if (GetCombineTargetHook)
				relid = GetCombineTargetHook(rte);

			if (!OidIsValid(relid))
			{
				/*
				 * If we or any extensions don't know how to combine it, it must not be a valid combine argument
				 */
				elog(ERROR, "relation is not a continuous view");
				return NULL;
			}
		}
		else
		{
			relid = rte->relid;
		}

		/*
		 * If this RTE isn't for a subquery, then it's just a direct scan of the matrel/delta stream, so we
		 * don't need to add anything to the Query's targetlist. We just need to directly reference
		 * the target matrel column
		 */
		if (!subquery)
		{
			Relation rel = heap_open(relid, AccessShareLock);
			Form_pg_attribute att = TupleDescAttr(rel->rd_att, attr - 1);

			if (attrtypmod)
				*attrtypmod = att->atttypmod;

			combine_target = makeVar(varno, att->attnum, att->atttypid, att->atttypmod, att->attcollation, 0);
			heap_close(rel, AccessShareLock);

			*matrelid = relid;
			*cvattr = combine_target->varattno;

			return NULL;
		}

		/*
		 * This is most likely an overlay Query selecting from the matrel,
		 * so determine if the target attribute is a finalized overlay expression.
		 */
		te = (TargetEntry *) list_nth(q->targetList, attr - 1);
		if (IsA(te->expr, Var))
		{
			/*
			 * If it's a simple Var, it can be used directly as a combine argument and
			 * no expansion is necessary.
			 */
			combine_target = copyObject((Var *) te->expr);
		}
		else
		{
			/*
			 * If it's not a bare column reference, we assume it's wrapped in a finalize
			 * function and pull out the underlying Var for expansion.
			 */
			args = pull_var_clause((Node *) te->expr, 0);
			if (list_length(args) != 1)
				elog(ERROR, "combine argument must be a single aggregate column");

			arg = linitial(args);
			if (!IsA(arg, Var))
				elog(ERROR, "combine argument must be a single aggregate column");

			combine_target = (Var *) copyObject(linitial(args));
		}

		/*
		 * Add the unfinalized column to the end of the target list, and return it
		 * for use as an argument to a combine aggregate.
		 *
		 * TODO(derekjn) If the original input expression was just a bare matrel column reference,
		 * we don't really need to add anything to the target list here.
		 */
		resno = list_length(q->targetList) + 1;
		te = makeTargetEntry((Expr *) combine_target, resno, "", false);
		q->targetList = lappend(q->targetList, te);

		result = copyObject(combine_target);
		result->varattno = resno;
		result->varno = varno;
		result->varoattno = combine_target->varattno;

		*matrelid = relid;
		*cvattr = combine_target->varattno;

		return result;
	}
	else if (rte->rtekind == RTE_SUBQUERY)
	{
		/*
		 * It's an attribute of a subquery, so ultimately we need to recurse until
		 * we hit the leaf Query, which will be reading from a matrel.
		 */
		TargetEntry *te = (TargetEntry *) list_nth(rte->subquery->targetList, attr - 1);
		List *args = pull_var_clause((Node *) te->expr, PVC_INCLUDE_AGGREGATES);
		Var *v;
		AttrNumber resno;
		Var *result;
		Node *arg;

		if (list_length(args) != 1)
			elog(ERROR, "combine argument must be a single aggregate column");

		arg = linitial(args);

		if (IsA(arg, Aggref))
		{
			/*
			 * If we're combining a lower combine aggregate, we'll end up here. The primary situation
			 * in which this will happen is when user combines are being run on SW CVs. Since SW CVs already
			 * combine over their window at read time, that result must be combined again by the top-level
			 * user combine. In these cases we look up the appropriate partial aggregate and use that for
			 * the lower combine, which will produce output identical to the matrel.
			 *
			 * We use completely separate aggregates for partial combines because it keeps these queries
			 * well-supported by PG's standard planning path. Anything more complex would require quite a bit
			 * of tedium here as well as at planning time, which we currently leave almost entirely up to PG.
			 */
			Aggref *target = (Aggref *) arg;
			List *aggargs = pull_var_clause((Node *) target->args, 0);
			Var *aggv;
			int32 typmod = -1;

			if (list_length(aggargs) != 1)
				elog(ERROR, "combine argument must be a single aggregate column");
			if (!IsA(linitial(aggargs), Var))
				elog(ERROR, "combine argument must be a single aggregate column");

			aggv = (Var *) linitial(aggargs);
			target->aggfnoid = get_partial_combine_aggfnoid(target->aggfnoid, &target->aggtype);

			/* We just call this to pull out the matrelid and target CV attribute */
			add_unfinalized_var(aggv->varno, aggv->varattno, rte->subquery, false, false, true, matrelid, cvattr, &typmod);

			/* And return a Var referencing this partial combine aggregate */
			v = makeVar(varno, te->resno, target->aggtype, typmod, target->aggcollid, 0);

			return v;
		}

		arg = linitial(args);
		if (!IsA(arg, Var))
			elog(ERROR, "combine argument must be a single aggregate column");

		/*
		 * Recurse down to the leaf node this subquery is selecting from
		 */
		v = (Var *) arg;
		result = add_unfinalized_var(v->varno, v->varattno, rte->subquery, false, true, true, matrelid, cvattr, NULL);

		if (!result)
			return copyObject(v);

		result = copyObject(result);
		result->varno = varno;

		/*
		 * We only want to add the attribute to this Query's targetlist if there is something
		 * above it that needs it. For a top-level combine aggregate, we don't need to return
		 * the unfinalized column as part of the Query result, we just need to reference it as the
		 * aggregate's argument.
		 */
		if (!toplevel)
		{
			resno = list_length(q->targetList) + 1;
			q->targetList = lappend(q->targetList, makeTargetEntry((Expr *) result, resno, "", false));

			result = copyObject(result);
			result->varno = varno;
			result->varattno = resno;
		}

		return result;
	}
	else if (rte->rtekind == RTE_JOIN)
	{
		/*
		 * It's an attribute referencing a JOIN RTE, so we need to follow the reference to the RTE
		 * that it ultimately belongs to.
		 */
		Var *jv = list_nth(rte->joinaliasvars, attr - 1);
		Var *v = add_unfinalized_var(jv->varno, jv->varattno, q, true, false, true, matrelid, cvattr, NULL);

		if (v)
		{
			Var *result;

			/*
			 * Add the unfinalized column to the list of JOIN columns.
			 *
			 * Note that this attribute refers to the actual relation that the attribute belongs to.
			 */
			v->varno = jv->varno;
			rte->joinaliasvars = lappend(rte->joinaliasvars, v);

			/*
			 * The attribute we ultimately return refers to the position in the JOIN RTE
			 * columns list where the new reference can be found. The attribute found there will
			 * then refer to the relation the column ultimately belongs to.
			 */
			result = copyObject(v);
			result->varattno = list_length(rte->joinaliasvars);
			result->varno = varno;

			return result;
		}
	}
	else
	{
		/*
		 * Other RTE types are currently unsupported, but there is no reason they can't be supported.
		 */
		elog(ERROR, "combine aggregates are only supported on continuous views");
	}

	return NULL;
}

/*
 * search_for_combine_aggregate
 */
static Oid
search_for_combine_aggregate(Oid combinefn, Oid finalfn, Oid serialfn, Oid deserialfn)
{
	HeapTuple	tup;
	Oid oid = InvalidOid;
	Form_pg_aggregate row;

	tup = PipelineCatalogLookup(PGAGGCOMBINEFN, 4, ObjectIdGetDatum(combinefn), ObjectIdGetDatum(finalfn), ObjectIdGetDatum(serialfn), ObjectIdGetDatum(deserialfn));

	if (HeapTupleIsValid(tup))
	{
		row = (Form_pg_aggregate) GETSTRUCT(tup);
		oid = row->aggfnoid;
	}

	return oid;
}

static HTAB *combine_oids = NULL;

/*
 * cache_combine_oid
 */
static void
cache_combine_oid(Oid oid)
{
	bool found;

	if (combine_oids == NULL)
	{
		HASHCTL ctl;
		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(Oid);
		ctl.hcxt = TopMemoryContext;
		combine_oids = hash_create("CombineAggOids", 64, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	Assert(combine_oids);
	hash_search(combine_oids, &oid, HASH_ENTER, &found);
}

/*
 * is_combine_aggref
 *
 * Given an Aggref, determine if it is a reference to our combine dummy aggregate,
 * which we use to expand into an actual combine aggregate.
 */
static bool
is_combine_aggref(Aggref *agg, bool *sw)
{
	bool isnull;
	HeapTuple tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid));
	Datum d = SysCacheGetAttr(AGGFNOID, tup, Anum_pg_aggregate_agginitval, &isnull);
	char *magic = NULL;

	if (!isnull)
		magic = TextDatumGetCString(d);

	ReleaseSysCache(tup);

	if (magic == NULL)
		return false;

	Assert(sw);
	*sw = false;
	if (!pg_strcasecmp(magic, SW_COMBINE_AGG_DUMMY))
	{
		*sw = true;
		cache_combine_oid(agg->aggfnoid);
		return true;
	}

	if (!pg_strcasecmp(magic, COMBINE_AGG_DUMMY))
	{
		cache_combine_oid(agg->aggfnoid);
		return true;
	}

	return false;
}

/*
 * OidIsCombineAgg
 */
bool
OidIsCombineAgg(Oid oid)
{
	bool found;

	if (!combine_oids)
		return false;

	hash_search(combine_oids, &oid, HASH_FIND, &found);

	return found;
}

/*
 * get_combine_aggfnoid
 */
static Oid
get_combine_aggfnoid(Oid aggfnoid, Oid *rettype)
{
	HeapTuple tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(aggfnoid));
	Form_pg_aggregate aggrow;
	Form_pg_proc procrow;
	Oid result;

	Assert(rettype);
	*rettype = InvalidOid;

	Assert(HeapTupleIsValid(tup));
	aggrow = (Form_pg_aggregate) GETSTRUCT(tup);

	result = search_for_combine_aggregate(aggrow->aggcombinefn, aggrow->aggfinalfn, aggrow->aggserialfn, aggrow->aggdeserialfn);
	ReleaseSysCache(tup);

	if (!OidIsValid(result))
	{
		/*
		 * If we couldn't resolve the aggregate's combine aggregate from the signature alone,
		 * check pipeline_combine catalog for an explicit mapping.
		 */
		HeapTuple tup = PipelineCatalogLookup(PIPELINECOMBINEAGGFN, 1, ObjectIdGetDatum(aggfnoid));
		Form_pipeline_combine pc;

		if (!HeapTupleIsValid(tup))
			return aggfnoid;

		pc = (Form_pipeline_combine) GETSTRUCT(tup);
		cache_combine_oid(pc->combineaggfn);

		return pc->combineaggfn;
	}

	tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(result));
	Assert(HeapTupleIsValid(tup));
	procrow = (Form_pg_proc) GETSTRUCT(tup);

	*rettype = procrow->prorettype;

	ReleaseSysCache(tup);

	cache_combine_oid(result);

	return result;
}

/*
 * analyze_osrel_combine
 */
static Oid
analyze_osrel_combine(Var *target, Query *q)
{
	RangeTblEntry *rte = rt_fetch(target->varno, q->rtable);
	HeapTuple tup;
	Form_pipeline_query pq;
	Relation osrel;
	TupleDesc desc;

	if (!RelidIsOutputStream(rte->relid))
		elog(ERROR, "relation is not an output stream");

	osrel = heap_open(rte->relid, AccessShareLock);
	desc = RelationGetDescr(osrel);

	if (target->varattno > desc->natts || pg_strcasecmp(OSREL_DELTA_ROW, NameStr(TupleDescAttr(desc, target->varattno - 1)->attname)))
		elog(ERROR, "column is not a delta column");

	heap_close(osrel, NoLock);
	tup = PipelineCatalogLookup(PIPELINEQUERYOSRELID, 1, ObjectIdGetDatum(rte->relid));
	Assert(HeapTupleIsValid(tup));

	pq = (Form_pipeline_query) GETSTRUCT(tup);

	return pq->matrelid;
}

/*
 * get_combine_target_aggref
 *
 * Given a combine target column, return the underlying Aggref used to build that column
 */
static Aggref *
get_combine_target_aggref(Oid matrelid, AttrNumber attr)
{
	TargetEntry *cvte = NULL;
	Query *wq;
	HeapTuple pqtup;
	Form_pipeline_query pqrow;
	ContQuery *cq;
	Relation rel;
	ListCell *lc;
	Aggref *agg;
	bool sw;

	Assert(OidIsValid(matrelid));
	pqtup = PipelineCatalogLookup(PIPELINEQUERYMATRELID, 1, ObjectIdGetDatum(matrelid));
	Assert(HeapTupleIsValid(pqtup));
	pqrow = (Form_pipeline_query) GETSTRUCT(pqtup);

	/*
	 * We get the target Aggref that's being combined from the worker query's targetlist,
	 * since it will contain all hoisted columns. The original CV definition query doesn't
	 * so we can't use that here.
	 */
	cq = GetContQueryForId(pqrow->id);
	wq = GetContWorkerQuery(cq);
	rel = heap_open(matrelid, AccessShareLock);

	foreach(lc, wq->targetList)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		if (!pg_strcasecmp(te->resname, NameStr(TupleDescAttr(rel->rd_att, attr - 1)->attname)))
		{
			cvte = te;
			break;
		}
	}

	heap_close(rel, AccessShareLock);

	if (!cvte)
		elog(ERROR, "Aggref not found in worker targetlist");

	if (!IsA(cvte->expr, Aggref))
		elog(ERROR, "combine argument is not an aggregate column");

	agg = (Aggref *) cvte->expr;

	if (is_combine_aggref(agg, &sw))
	{
		Var *v;
		Node *n;
		TargetEntry *te;
		FieldSelect *fs;

		/*
		 * The only thing this combine could be is on output stream combine, which have a very specific form
		 */
		Assert(list_length(agg->args) == 1);
		n = linitial(agg->args);

		Assert(IsA(n, TargetEntry));
		te = (TargetEntry *) n;

		Assert(IsA(te->expr, FieldSelect));
		fs = (FieldSelect *) te->expr;

		Assert(IsA(fs->arg, Var));
		v = (Var *) fs->arg;

		matrelid = analyze_osrel_combine(v, wq);

		return get_combine_target_aggref(matrelid, fs->fieldnum);
	}


	return (Aggref *) cvte->expr;
}

/*
 * rewrite_nodes
 */
static void
rewrite_nodes(Query *q, List *nodes)
{
	ListCell *lc;

	foreach(lc, nodes)
	{
		Node *n = (Node *) lfirst(lc);
		Aggref *agg;
		Var *combine_var;
		Var *v;
		HeapTuple tup;
		Form_pg_aggregate row;
		Node *arg;
		Expr *combine_expr;
		Oid matrelid;
		AttrNumber cvattr;
		Aggref *cvagg;
		TargetEntry *aggte;
		Oid combineoid;
		bool sw = false;
		Oid rettype;

		if (!IsA(n, Aggref))
			continue;

		agg = (Aggref *) n;
		if (!is_combine_aggref(agg, &sw))
			continue;

		if (list_length(agg->args) != 1)
			elog(ERROR, "combine argument must be a single aggregate column");

		arg = linitial(agg->args);
		if (!IsA(arg, TargetEntry))
			elog(ERROR, "combine argument must be a single aggregate column");

		aggte = (TargetEntry *) arg;
		arg = (Node *) aggte->expr;

		/*
		 * If it's a SW combine, a final function as an argument is expected.
		 * Otherwise we only allow a single Var reference.
		 */
		if (!sw && !IsA(arg, Var) && !IsA(arg, FieldSelect))
			elog(ERROR, "combine argument must be a single aggregate column");

		if (sw)
		{
			/*
			 * SW combines may have a column reference wrapped in a final function,
			 * so we just pull the one Var out.
			 */
			List *args = pull_var_clause(arg, 0);
			Assert(list_length(args) == 1);
			combine_var = (Var *) linitial(args);
		}
		else
		{
			if (IsA(arg, FieldSelect))
			{
				FieldSelect *fs = (FieldSelect *) arg;
				if (!IsA(fs->arg, Var))
					elog(ERROR, "combine argument must be a single aggregate column");

				combine_var = (Var *) fs->arg;
			}
			else
			{
				/*
				 * We only want to rewrite combines for CQs combining delta streams and for user combine queries
				 */
				if (IsContQueryProcess())
					return;

				/*
				 * User combines only allow a single column reference, which we've already
				 * enforced so this is it.
				 */
				combine_var = (Var *) arg;
			}
		}

		/*
		 * We're being called on an overlay view, so all of the target columns will appear as
		 * finalized aggregate values. Since combine aggregates operate on the unfinalized transition
		 * states, we rewrite the lower target lists to include an unfinalized column reference if necessary.
		 *
		 * We'll also return some other useful values here, such as which matrel/CV this combine aggregate
		 * ultimately references.
		 */
		if (IsA(arg, FieldSelect))
		{
			FieldSelect *fs = (FieldSelect *) arg;

			cvattr = fs->fieldnum;
			matrelid = analyze_osrel_combine((Var *) fs->arg, q);
		}
		else
		{
			v = add_unfinalized_var(combine_var->varno, combine_var->varattno, q, true, false, true, &matrelid, &cvattr, NULL);
			if (v)
				combine_var = v;
			arg = (Node *) combine_var;
		}

		/*
		 * Next we want to get the actual Aggref corresponding to the target aggregate column,
		 * which we can get from the CV's query definition. We'll use it to get the underlying
		 * combine aggregate.
		 */
		cvagg = get_combine_target_aggref(matrelid, cvattr);
		combineoid = get_combine_aggfnoid(cvagg->aggfnoid, &rettype);

		Assert(OidIsValid(combineoid));

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(combineoid));
		row = (Form_pg_aggregate) GETSTRUCT(tup);

		/*
		 * If the underlying aggregate has an internal transition state, we'll need to
		 * deserialize it since it's stored in its serialized (bytea) form.
		 */
		if (row->aggtranstype == INTERNALOID)
		{
			Assert(OidIsValid(row->aggdeserialfn));
			combine_expr = make_deserialization_call(arg, row->aggdeserialfn);
		}
		else
		{
			combine_expr = (Expr *) arg;
		}

		ReleaseSysCache(tup);

		/*
		 * Finally, modify the dummy combine aggregate to use the actual combine functions
		 */
		if (PipelineContextIsDDL())
		{
			/*
			 * If we're defining a CV with an output stream combine, we want to use bytea here
			 * for internal transition states since that's how they're handled within matrels/osrels.
			 */
			agg->aggtype = row->aggtranstype == INTERNALOID ? BYTEAOID : row->aggtranstype;
		}
		else
		{
			if (OidIsValid(rettype))
				agg->aggtype = rettype;
		}
		agg->aggfnoid = combineoid;
		agg->args = list_make1(makeTargetEntry(combine_expr, 1, NULL, false));

		tup = SearchSysCache1(AGGFNOID, ObjectIdGetDatum(agg->aggfnoid));
		Assert(HeapTupleIsValid(tup));
		row = (Form_pg_aggregate) GETSTRUCT(tup);

		agg->aggtranstype = row->aggtranstype;
		agg->aggargtypes = list_make1_oid(row->aggtranstype);
		agg->aggdirectargs = NIL;

		ReleaseSysCache(tup);
	}
}

/*
 * sublink_walker
 */
static bool
sublink_walker(Node *n, void *context)
{
	if (n == NULL)
		return false;

	if (IsA(n, SubLink))
	{
		SubLink *sub = (SubLink *) n;
		if (IsA(sub->subselect, Query))
			RewriteCombineAggs((Query *) sub->subselect);
	}

	return expression_tree_walker(n, sublink_walker, NULL);
}

/*
 * pull_aggrefs_walker
 */
static bool
pull_aggrefs_walker(Node *node, List **context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		*context = lappend(*context, node);
		return false;
	}

	return expression_tree_walker(node, pull_aggrefs_walker, context);
}

/*
 * pull_aggrefs
 */
static List *
pull_aggrefs(Node *node)
{
	List *result = NIL;

	expression_tree_walker(node, pull_aggrefs_walker, &result);

	return result;
}

/*
 * RewriteCombineAggs
 *
 * Expands any combine aggregate references into correct Aggref expressions, adding any
 * unfinalized columns to the input target list if necessary.
 */
void
RewriteCombineAggs(Query *q)
{
	ListCell *qlc;
	List *nodes;

	if (q->hasSubLinks)
		query_tree_walker(q, sublink_walker, NULL, QTW_DONT_COPY_QUERY);

	/*
	 * Recurse for CTEs
	 */
	foreach(qlc, q->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(qlc);
		if (IsA(cte->ctequery, Query))
			RewriteCombineAggs((Query *) cte->ctequery);
	}

	/*
	 * First recursively perform the combine aggregate rewrite on any subqueries
	 */
	foreach(qlc, q->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(qlc);
		if (rte->rtekind == RTE_SUBQUERY)
			RewriteCombineAggs(rte->subquery);
	}
	/*
	 * Now rewrite top-level combine aggregates in this Query's target list
	 */
	nodes = pull_aggrefs((Node *) q->targetList);
	rewrite_nodes(q, nodes);

	/*
	 * If we have a HAVING clause, rewrite any combine aggregates within it
	 */
	if (q->havingQual)
	{
		nodes = pull_aggrefs((Node *) q->havingQual);
		rewrite_nodes(q, nodes);
	}
}
