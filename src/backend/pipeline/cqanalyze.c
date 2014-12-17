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
#include "catalog/pipeline_combine_fn.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
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
		{"count", "hll_count_distinct"}
};

static void replace_hypothetical_set_aggs(SelectStmt *stmt);
static char *get_streaming_agg(FuncCall *fn);
static bool associate_types_to_colrefs(Node *node, CQAnalyzeContext *context);
static bool type_cast_all_column_refs(Node *node, CQAnalyzeContext *context);

#define INTERNAL_COLNAME_PREFIX "_"

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
 * FindColNames
 */
static bool
FindColNames(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL)
			context->colNames = lappend(context->colNames, res->name);
	}
	else if (IsA(node, ColumnRef))
	{
		context->colNames = lappend(context->colNames, FigureColname(node));
	}

	return raw_expression_tree_walker(node, FindColNames, (void *) context);
}

/*
 * InitializeCQAnalyzeContext
 */
void
InitializeCQAnalyzeContext(SelectStmt *stmt, ParseState *pstate, CQAnalyzeContext *context)
{
	memset(context, 0, sizeof(CQAnalyzeContext));
	context->location = -1;
	context->pstate = pstate;

	FindColNames((Node *) stmt, context);

	context->cols = NIL;
}

/*
 * GetUniqueInternalColname
 */
char *
GetUniqueInternalColname(CQAnalyzeContext *context)
{
	StringInfoData colname;

	initStringInfo(&colname);

	while (1)
	{
		ListCell *lc;
		bool alreadyExists = false;

		appendStringInfo(&colname, "%s%d", INTERNAL_COLNAME_PREFIX, context->colNum);
		context->colNum++;

		foreach(lc, context->colNames)
		{
			char *colname2 = lfirst(lc);
			if (strcmp(colname.data, colname2) == 0)
			{
				alreadyExists = true;
				break;
			}
		}

		if (!alreadyExists)
			break;

		resetStringInfo(&colname);
	}

	context->colNames = lappend(context->colNames, colname.data);

	return colname.data;
}

/*
 * is_column_ref
 */
bool
IsAColumnRef(Node *node)
{
	TypeCast *tc = (TypeCast *) node;

	return (IsA(node, ColumnRef) ||
			(IsA(node, TypeCast) && (IsA(tc->arg, ColumnRef))));
}

/*
 * FindColRefsWithTypeCasts
 */
bool
FindColumnRefsWithTypeCasts(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, FindColumnRefsWithTypeCasts, (void *) context);
}

/*
 * AreColumnRefsEqual
 */
bool
AreColumnRefsEqual(Node *cr1, Node *cr2)
{
	if (IsA(cr1, TypeCast))
	{
		cr1 = ((TypeCast *) cr1)->arg;
	}
	if (IsA(cr2, TypeCast))
	{
		cr2 = ((TypeCast *) cr2)->arg;
	}

	Assert(IsA(cr1, ColumnRef) && IsA(cr2, ColumnRef));

	/* In our land ColumnRef locations don't matter */
	return equal(((ColumnRef *) cr1)->fields, ((ColumnRef *) cr2)->fields);
}

/*
 * ContainsColumnRef
 */
bool
ContainsColumnRef(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, WindowDef))
		return false;

	if (IsA(node, ColumnRef))
		return AreColumnRefsEqual((Node *) cref, node);

	return raw_expression_tree_walker(node, ContainsColumnRef, (void *) cref);
}

/*
 * GetColumnRef
 */
ColumnRef *
GetColumnRef(Node *node)
{
	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		node = tc->arg;
	}

	Assert(IsA(node, ColumnRef));

	return (ColumnRef *) node;
}

/*
 * IsResTargetForColumnRef
 */
static bool
IsResTargetForColumnRef(Node *node, ColumnRef *cref)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		if (res->name != NULL &&
				strcmp(res->name, FigureColname((Node *) cref)) == 0)
		{
			/*
			 * Is this ResTarget overriding a column it references?
			 * Example: substring(key::text, 1, 2) AS key
			 */
			if (ContainsColumnRef((Node *) res, cref))
				return false;
			return true;
		}
	}
	else if (IsA(node, ColumnRef))
	{
		ColumnRef *cref2 = (ColumnRef *) node;
		return AreColumnRefsEqual((Node *) cref, (Node *) cref2);
	}
	else if (IsA(node, FuncCall) || IsA(node, Expr))
	{
		/*
		 * Even if a FuncCall has cref as an argument, its value
		 * can be different from the column, so we need to project
		 * the column.
		 */
		return false;
	}

	return raw_expression_tree_walker(node, IsResTargetForColumnRef, (void *) cref);
}

/*
 * IsColumnRefInTargetList
 */
ResTarget *
IsColumnRefInTargetList(SelectStmt *stmt, Node *node)
{
	ColumnRef *cref;
	ListCell *lc;

	if (IsA(node, TypeCast))
		node = ((TypeCast *) node)->arg;

	Assert(IsA(node, ColumnRef));

	cref = (ColumnRef *) node;

	foreach(lc, stmt->targetList)
	{
		node = lfirst(lc);

		if (IsResTargetForColumnRef(node, cref))
			return (ResTarget *) node;
	}

	return NULL;
}

/*
 * CollectAggFuncs
 *
 * Does the node contain an aggregate function?
 */
bool
CollectAggFuncs(Node *node, CQAnalyzeContext *context)
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

		clist = FuncnameGetCandidates(fn->funcname, list_length(fn->args), NIL, true, false, true);

		while (clist != NULL)
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

		if (is_agg && context)
		{
			context->funcCalls = lappend(context->funcCalls, fn);
		}
	}

	return raw_expression_tree_walker(node, CollectAggFuncs, context);
}

/*
 * CreateResTargetForNode
 */
ResTarget *
CreateResTargetForNode(Node *node)
{
	ResTarget *res = makeNode(ResTarget);
	res->name = NULL;
	res->indirection = NIL;
	res->val = copyObject(node);
	res->location = -1;
	return res;
}

/*
 * CreateUniqueResTargetForNode
 */
ResTarget *
CreateUniqueResTargetForNode(Node *node, CQAnalyzeContext *context)
{
	/*
	 * Any new node we add to the targetList should have a unique
	 * name. This isn't always necessary, but its always safe.
	 * Safety over everything else yo.
	 */
	char *name = GetUniqueInternalColname(context);
	ResTarget *res = makeNode(ResTarget);
	res->name = name;
	res->indirection = NIL;
	res->val = copyObject(node);
	res->location = -1;

	return res;
}

/*
 * CreateColumnRefFromResTarget
 */
ColumnRef *
CreateColumnRefFromResTarget(ResTarget *res)
{
	ColumnRef *cref;
	char *name = res->name;

	if (name == NULL)
		name = FigureColname(res->val);

	cref = makeNode(ColumnRef);
	cref->fields = list_make1(makeString(name));
	cref->location = -1;
	return cref;
}

/*
 * HasAggOrGroupBy
 */
bool
HasAggOrGroupBy(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	ListCell *tlc;
	ResTarget *res;

	context.funcCalls = NIL;

	if (list_length(stmt->groupClause) > 0)
		return true;

	/* Do we have any aggregates? */
	foreach(tlc, stmt->targetList)
	{
		res = (ResTarget *) lfirst(tlc);
		CollectAggFuncs(res->val, &context);

		if (list_length(context.funcCalls) > 0)
			return true;
	}

	return false;
}

/*
 * add_res_target_to_view
 */
static void
add_res_target_to_view(SelectStmt *viewstmt, ResTarget *res)
{
	char *colName = res->name;
	ResTarget *newRes;

	if (colName == NULL)
		colName = FigureColname(res->val);

	newRes = makeNode(ResTarget);
	newRes->name = colName;
	newRes->val = res->val;
	newRes->location = res->location;
	newRes->indirection = NIL;

	viewstmt->targetList = lappend(viewstmt->targetList, newRes);
}

/*
 * HoistNode
 */
Node *
HoistNode(SelectStmt *stmt, Node *node, CQAnalyzeContext *context)
{
	ResTarget *res;

	if (IsAColumnRef(node) && IsColumnRefInTargetList(stmt, node))
		return node;

	res = CreateUniqueResTargetForNode(node, context);
	stmt->targetList = lappend(stmt->targetList, res);

	return (Node *) CreateColumnRefFromResTarget(res);
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

	/* if the agg is count, we only need a streaming variant if it's DISTINCT */
	if (strcmp(name, "count") == 0 && fn->agg_distinct == false)
		return NULL;

	for (i=0; i<len; i++)
	{
		char *k = StreamingVariants[i][0];
		char *v = StreamingVariants[i][1];

		if (strcmp(k, name) == 0)
			return v;
	}

	return NULL;
}

/*
 * replace_hypothetical_set_aggs
 *
 * Replaces hypothetical set aggregate functions with their streaming
 * variants if possible, since we can't use the sorting approach that
 * the standard functions use.
 */
static void
replace_hypothetical_set_aggs(SelectStmt *stmt)
{
	ListCell *lc;

	foreach(lc, stmt->targetList)
	{
		ResTarget *res = (ResTarget *) lfirst(lc);
		CQAnalyzeContext context;
		ListCell *fnlc;
		char *prevname = NULL;
		char *newname = NULL;

		context.funcCalls = NIL;
		CollectAggFuncs((Node *) res, &context);

		/*
		 * For each agg in this ResTarget, replace any function names
		 * with the names of their streaming variants.
		 */
		foreach(fnlc, context.funcCalls)
		{
			FuncCall *fn = (FuncCall *) lfirst(fnlc);

			prevname = NameListToString(fn->funcname);
			newname = get_streaming_agg(fn);
			if (newname != NULL)
				fn->funcname = list_make1(makeString(newname));
		}

		/*
		 * If there is only one function call and the ResTarget
		 * wasn't already named, give it the name of the replaced
		 * function because that's the expected name of the column
		 * in the CV.
		 */
		if (res->name == NULL && prevname != NULL &&
				list_length(context.funcCalls) == 1)
		{
			res->name = prevname;
		}
	}
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
GetSelectStmtForCQWorker(SelectStmt *stmt, SelectStmt **viewstmtptr)
{
	CQAnalyzeContext context;
	SelectStmt *viewstmt;
	SelectStmt *workerstmt;
	List *workerGroups;
	List *workerTargetList;
	bool isSlidingWindow;
	bool doesViewAggregate;
	int origNumTargets;
	int origNumGroups;
	int i;
	ListCell *lc;

	InitializeCQAnalyzeContext(stmt, NULL, &context);
	associate_types_to_colrefs((Node *) stmt, &context);
	type_cast_all_column_refs((Node *) stmt, &context);

	workerstmt = (SelectStmt *) copyObject(stmt);
	viewstmt = (SelectStmt *) makeNode(SelectStmt);
	viewstmt->targetList = NIL;
	viewstmt->groupClause = NIL;

	origNumTargets = list_length(workerstmt->targetList);
	origNumGroups = list_length(workerstmt->groupClause);

	isSlidingWindow = IsSlidingWindowSelectStmt(workerstmt);
	doesViewAggregate = DoesViewAggregate(workerstmt, &context);

	/*
	 * Check to see if we need to project any expressions/columns
	 * so that we can perform the necessary windowing functionality.
	 */
	if (isSlidingWindow || doesViewAggregate)
		AddProjectionsAndGroupBysForWindows(workerstmt, viewstmt, doesViewAggregate, &context);

	/*
	 * We can't use the standard hypothetical set aggregate functions because
	 * they require sorting the input set, so replace them with their
	 * streaming variants if possible.
	 */
	replace_hypothetical_set_aggs(workerstmt);

	/*
	 * Rewrite the groupClause and project any group expressions that
	 * are not already in the targetList.
	 */
	workerGroups = NIL;
	for (i = 0; i < origNumGroups; i++)
	{
		Node *node = (Node *) list_nth(workerstmt->groupClause, i);
		node = HoistNode(workerstmt, node, &context);

		workerGroups = lappend(workerGroups, node);
		if(doesViewAggregate)
			viewstmt->groupClause = lappend(viewstmt->groupClause, node);
	}

	for (; i < list_length(workerstmt->groupClause); i++)
		workerGroups = lappend(workerGroups, list_nth(workerstmt->groupClause, i));

	workerstmt->groupClause = workerGroups;

	/*
	 * Hoist aggregates out of expressions for workers.
	 */
	workerTargetList = NIL;
	for (i = 0; i < origNumTargets; i++)
	{
		ResTarget *res = (ResTarget *) list_nth(workerstmt->targetList, i);
		FuncCall *agg;
		ListCell *agglc;

		/*
		 * Any ResTarget with a ResTarget as a val should be ignore for the worker
		 * stmt and the val should be taken for the view stmt.
		 */
		if (IsA(res->val, ResTarget))
		{
			viewstmt->targetList = lappend(viewstmt->targetList, res->val);
			continue;
		}

		context.funcCalls = NIL;
		CollectAggFuncs(res->val, &context);

		if (list_length(context.funcCalls) == 0)
		{
			workerTargetList = lappend(workerTargetList, res);
			add_res_target_to_view(viewstmt,
					CreateResTargetForNode((Node *) CreateColumnRefFromResTarget(res)));
			continue;
		}

		agg = (FuncCall *) linitial(context.funcCalls);

		/* No need to rewrite top level agg funcs */
		if (equal(res->val, agg))
		{
			Assert(list_length(context.funcCalls) == 1);

			workerTargetList = lappend(workerTargetList, copyObject(res));
			TransformAggNodeForCQView(viewstmt, res->val, res, doesViewAggregate);

			add_res_target_to_view(viewstmt, res);

			continue;
		}

		/*
		 * Hoist each agg function call into a new hidden column
		 * and reference that hidden column in the expression which
		 * is evaluated by the view.
		 */
		foreach(agglc, context.funcCalls)
		{
			Node *node = (Node *) lfirst(agglc);
			ResTarget *aggres = CreateUniqueResTargetForNode(node, &context);
			workerTargetList = lappend(workerTargetList, aggres);

			TransformAggNodeForCQView(viewstmt, node, aggres, doesViewAggregate);
		}

		add_res_target_to_view(viewstmt, res);
	}

	for (; i < list_length(workerstmt->targetList); i++)
		workerTargetList = lappend(workerTargetList, list_nth(workerstmt->targetList, i));

	/*
	 * Any WINDOWing cruft should be removed from the worker statement
	 * and only left in the view stmt.
	 */
	workerstmt->targetList = NIL;
	foreach(lc, workerTargetList)
	{
		ResTarget *res = (ResTarget *) copyObject(lfirst(lc));

		if (IsA(res->val, FuncCall))
		{
			FuncCall *fcall = (FuncCall *) res->val;
			fcall->over = NULL;
		}

		workerstmt->targetList = lappend(workerstmt->targetList, res);
	}

	viewstmt->windowClause = workerstmt->windowClause;
	workerstmt->windowClause = NIL;

	if (viewstmtptr != NULL)
		*viewstmtptr = viewstmt;

	return workerstmt;
}

/*
 * GetSelectStmtForCQCombiner
 */
SelectStmt *
GetSelectStmtForCQCombiner(SelectStmt *stmt)
{
	stmt = GetSelectStmtForCQWorker(stmt, NULL);

	/*
	 * Combiner shouldn't have to check for the
	 * whereClause conditionals. The worker has already
	 * done that.
	 */
	stmt->whereClause = NULL;

	return stmt;
}

/*
 * associate_types_to_colrefs
 *
 * Walk the parse tree and associate a single type with each inferred column
 */
static bool
associate_types_to_colrefs(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, associate_types_to_colrefs, (void *) context);
}

static TypeCast *
find_type_cast(ColumnRef *cref, CQAnalyzeContext *context)
{
	ListCell *lc;
	foreach(lc, context->types)
	{
		TypeCast *tc = (TypeCast *) lfirst(lc);
		if (AreColumnRefsEqual((Node *) tc, (Node *) cref))
			return copyObject(tc);
	}

	return NULL;
}

static void
replace_column_ref_with_type_cast(Node **nodeptr, CQAnalyzeContext *context)
{
	TypeCast *tc;

	if (*nodeptr == NULL || !IsA(*nodeptr, ColumnRef))
		return;

	tc = find_type_cast((ColumnRef *) *nodeptr, context);
	if (tc == NULL)
		return;

	*nodeptr = (Node *) tc;
}

static bool
type_cast_all_column_refs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, ResTarget))
	{
		ResTarget *res = (ResTarget *) node;
		replace_column_ref_with_type_cast(&res->val, context);
	}
	else if (IsA(node, FuncCall))
	{
		FuncCall *fcall = (FuncCall *) node;
		ListCell *lc;

		foreach(lc, fcall->args)
			replace_column_ref_with_type_cast((Node **) &lfirst(lc), context);
	}
	else if (IsA(node, SortBy))
	{
		SortBy *sort = (SortBy *) node;
		replace_column_ref_with_type_cast(&sort->node, context);
	}
	else if (IsA(node, A_Expr))
	{
		A_Expr *expr = (A_Expr *) node;
		replace_column_ref_with_type_cast(&expr->lexpr, context);
		replace_column_ref_with_type_cast(&expr->rexpr, context);

	}
	else if (IsA(node, WindowDef))
	{
		WindowDef *wdef = (WindowDef *) node;
		ListCell *lc;

		foreach(lc, wdef->partitionClause)
			replace_column_ref_with_type_cast((Node **) &lfirst(lc), context);

		foreach(lc, wdef->orderClause)
			replace_column_ref_with_type_cast((Node **) &lfirst(lc), context);
	}
	else if (IsA(node, SelectStmt))
	{
		SelectStmt *select = (SelectStmt *) node;
		ListCell *lc;

		foreach(lc, select->groupClause)
			replace_column_ref_with_type_cast((Node **) &lfirst(lc), context);
	}

	return raw_expression_tree_walker(node, type_cast_all_column_refs, (void *) context);
}

/*
 * AddStreams
 *
 * Figure out which relations are streams that we'll need to infer types for
 */
bool
AddStreams(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, AddStreams, (void *) context);
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
make_streamdesc(RangeVar *rv, CQAnalyzeContext *context)
{
	List *attrs = NIL;
	ListCell *lc;
	StreamDesc *desc = makeNode(StreamDesc);
	TupleDesc tupdesc;
	bool onestream = list_length(context->streams) == 1 && list_length(context->tables) == 0;
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
replace_stream_rangevar_with_streamdesc(Node *node, CQAnalyzeContext *context)
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
	{
		return node;
	}
}

/*
 * CollectFuncs
 */
bool
CollectFuncs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncCall))
		context->funcCalls = lappend(context->funcCalls, node);

	return raw_expression_tree_walker(node, CollectFuncs, (void *) context);
}

/*
 * AnalyzeAndValidateContinuousSelectStmt
 *
 * This is mainly to prepare a CV SELECT's FROM clause, which may involve streams
 */
void
AnalyzeAndValidateContinuousSelectStmt(ParseState *pstate, SelectStmt **topselect)
{
	CQAnalyzeContext context;
	SelectStmt *stmt = *topselect;
	ListCell *lc;
	List *newfrom = NIL;

	InitializeCQAnalyzeContext(stmt, pstate, &context);

	if (list_length(stmt->sortClause) > 0)
	{
		SortBy *sortby = (SortBy *) linitial(stmt->sortClause);
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("continuous queries don't support ORDER BY"),
						parser_errposition(pstate, sortby->location)));
	}

	/* make sure that we can infer types for every column that appears anywhere in the statement */
	associate_types_to_colrefs((Node *) stmt, &context);

	/* now indicate which relations are actually streams */
	AddStreams((Node *) stmt->fromClause, &context);

	foreach(lc, context.cols)
	{
		ListCell *tlc;
		ColumnRef *cref = lfirst(lc);
		bool needsType = true;
		bool hasType = false;
		bool matchesResTarget = false;
		char *colName = FigureColname((Node *) cref);

		/*
		 * arrival_timestamp doesn't require an explicit TypeCast
		 */
		if (pg_strcasecmp(colName, ARRIVAL_TIMESTAMP) == 0)
			continue;

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

		/*
		 * If the ColumnRef is for a relation, we don't need any explicit
		 * TypeCast.
		 */
		if (list_length(cref->fields) == 2)
		{
			char *colRelname = strVal(linitial(cref->fields));
			needsType = false;

			foreach(tlc, context.streams)
			{
				RangeVar *rv = (RangeVar *) lfirst(tlc);
				char *streamName = rv->alias ? rv->alias->aliasname : rv->relname;

				if (strcmp(colRelname, streamName) == 0)
				{
					needsType = true;
					break;
				}
			}

			if (!needsType)
				continue;
		}

		/* Do we have a TypeCast for this ColumnRef? */
		foreach(tlc, context.types)
		{
			TypeCast *tc = (TypeCast *) lfirst(tlc);
			if (equal(tc->arg, cref))
			{
				hasType = true;
				break;
			}
		}

		if (hasType)
			continue;

		/*
		 * If the ColumnRef refers to a named ResTarget then it doesn't need an explicit type.
		 *
		 * This doesn't work in cases where a ResTarget contains the ColumnRef
		 * being checked against. For example:
		 *
		 *   SELECT date_trunc('hour', x) AS x FROM stream
		 *   SELECT substring(y::text, 1, 2) as x, substring(x, 1, 2) FROM stream
		 *
		 * In both these examples we need an explicit TypeCast for `x`.
		 */
		needsType = false;

		foreach(tlc, context.targets)
		{
			ResTarget *rt = (ResTarget *) lfirst(tlc);
			if (ContainsColumnRef((Node *) rt, cref))
			{
				needsType = true;
				break;
			}

			if (rt->name != NULL && strcmp(strVal(linitial(cref->fields)), rt->name) == 0)
			{
				matchesResTarget = true;
			}
		}

		if (matchesResTarget && !needsType)
			continue;

		needsType = needsType || !matchesResTarget;

		if (needsType && !hasType)
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
		Node *newnode = replace_stream_rangevar_with_streamdesc(n, &context);

		newfrom = lappend(newfrom, newnode);
	}

	stmt->fromClause = newfrom;

	ValidateSlidingWindowExpr(stmt, &context);

	ValidateWindows(stmt, &context);
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

/*
 * pipeline_rewrite
 *
 * Take the list of parsetrees returned by `pg_parse_query` and
 * output a new list of parsetrees where any ActivateContinuousViewStmt
 * or DeactivateContinuousViewStmt Node with multiple `views` is broken down
 * into singular view Nodes.
 */
List *
pipeline_rewrite(List *raw_parsetree_list)
{
	ListCell *lc;

	foreach(lc, raw_parsetree_list)
	{
		Node *node = lfirst(lc);

		if (IsA(node, ActivateContinuousViewStmt) ||
				IsA(node, DeactivateContinuousViewStmt))
		{
			BaseContinuousViewStmt *stmt = (BaseContinuousViewStmt *) node;

			if (stmt->views && stmt->whereClause)
				elog(ERROR, "can't specify target list and a WHERE clause");
		}
		else if (IsA(node, IndexStmt))
		{
			IndexStmt *istmt = (IndexStmt *) node;

			/*
			 * If the user is trying to create an index on a CV, what they're really
			 * trying to do is create it on the CV's materialization table, so rewrite
			 * the name of the target relation if we need to.
			 */
			if (IsAContinuousView(istmt->relation))
			{
				char *s = GetMatRelationName(istmt->relation->relname);
				istmt->relation = makeRangeVar(NULL, s, -1);
			}
		}
		else if (IsA(node, VacuumStmt))
		{
			VacuumStmt *vstmt = (VacuumStmt *) node;
			/*
			 * If the user is trying to vacuum a CV, what they're really
			 * trying to do is create it on the CV's materialization table, so rewrite
			 * the name of the target relation if we need to.
			 */
			if (vstmt->relation && IsAContinuousView(vstmt->relation))
			{
				char *s = GetMatRelationName(vstmt->relation->relname);
				vstmt->relation = makeRangeVar(NULL, s, -1);
			}
		}
	}

	return raw_parsetree_list;
}
