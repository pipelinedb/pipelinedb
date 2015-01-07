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

#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_proc.h"
#include "catalog/pipeline_combine_fn.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/var.h"
#include "parser/parse_agg.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "parser/parse_target.h"
#include "parser/parse_type.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqwindow.h"
#include "pipeline/stream.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
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

static char *get_streaming_agg(FuncCall *fn);
static bool associate_types_to_colrefs(Node *node, CQAnalyzeContext *context);
static bool type_cast_all_column_refs(Node *node, CQAnalyzeContext *context);

#define INTERNAL_COLNAME_PREFIX "_"

/*
 * GetHiddenAttrNumber
 *
 * Returns the hidden attribute associated with the given
 * visible column, if any
 */
AttrNumber
GetHiddenAttrNumber(char *attname, TupleDesc matrel)
{
	int i;
	for (i=0; i<matrel->natts; i++)
	{
		if (strcmp(NameStr(matrel->attrs[i]->attname), attname) == 0)
			return i + 1 + 1;
	}

	return InvalidAttrNumber;
}

/*
 * GetCombineStateColumnType
 *
 * Retrieves the additional hidden columns that will be
 * required to store transition states for the given target entry
 */
Oid
GetCombineStateColumnType(Expr *expr)
{
	Oid result = InvalidOid;

	if (IsA(expr, Aggref))
		result = GetCombineStateType(((Aggref *) expr)->aggfnoid);
	else if (IsA(expr, WindowFunc))
		result = GetCombineStateType(((WindowFunc *) expr)->winfnoid);

	return result;
}

/*
 * IsUserCombine
 *
 * Determines if the given pg_proc entry is the user combine function
 */
bool
IsUserCombine(NameData proname)
{
	return strcmp(NameStr(proname), USER_COMBINE) == 0;
}

/*
 * locate_aggs
 *
 * Given an analyzed targetlist, maps aggregate nodes to their attributes
 * in a matrel. This is necessary because aggregates can be hoisted out of
 * CV expressions and given their own columns, so we can't just look at the
 * aggregate's position in the targetlist to get its matrel attribute position.
 *
 * The mapping is encoding within two parallel lists
 */
static void
locate_aggs(List *targetlist, List **aggs, List **atts)
{
	ListCell *lc;
	AttrNumber pos = 1;

	*aggs = NIL;
	*atts = NIL;

	foreach(lc, targetlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Expr *expr = te->expr;

		if (IsA(expr, Aggref) || IsA(expr, WindowFunc))
		{
			*aggs = lappend(*aggs, expr);
			*atts = lappend_int(*atts, pos);
			pos++;
			if (OidIsValid(GetCombineStateColumnType(expr)))
				pos++;
		}
		else
		{
			List *nodes = pull_var_clause((Node *) expr,
						PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
			ListCell *nlc;
			AttrNumber startpos = pos;

			foreach(nlc, nodes)
			{
				Expr *e = (Expr *) lfirst(nlc);
				if (!IsA(e, Aggref) && !IsA(e, WindowFunc))
					continue;

				*aggs = lappend(*aggs, e);
				*atts = lappend_int(*atts, pos);
				pos++;
				if (OidIsValid(GetCombineStateColumnType(e)))
					pos++;
			}

			if (startpos == pos)
				pos++;
		}
	}
}

/*
 * agg_to_attr
 *
 * Given an aggregate node, returns the matrel attribute that the node belongs to
 */
static AttrNumber
agg_to_attr(Node *agg, List *aggrefs, List *attrs)
{
	ListCell *agglc;
	ListCell *attlc;

	forboth(agglc, aggrefs, attlc, attrs)
	{
		if (lfirst(agglc) == agg)
			return lfirst_int(attlc);
	}

	elog(ERROR, "aggregate attribute not found for node of type %d", nodeTag(agg));

	return InvalidAttrNumber;
}

/*
 * attr_to_agg
 *
 * Given a matrel attribute, returns the aggregate node that it corresponds to
 */
static Node *
attr_to_agg(AttrNumber attr, List *aggs, List *atts)
{
	ListCell *agglc;
	ListCell *attlc;

	forboth(agglc, aggs, attlc, atts)
	{
		if (lfirst_int(attlc) == attr)
			return lfirst(agglc);
	}

	elog(ERROR, "no aggregate node found for attribute %d", attr);

	return NULL;
}

/*
 * make_combine_args
 *
 * Possibly wraps the target argument in a recv function call to deserialize
 * hidden state before calling combine()
 */
static List *
make_combine_args(ParseState *pstate, Oid combineinfn, Var *var)
{
	List *args;

	if (OidIsValid(combineinfn))
	{
		FuncCall *fn = makeNode(FuncCall);
		fn->funcname = list_make1(makeString(get_func_name(combineinfn)));
		fn->args = list_make1(var);
		args = list_make1(transformFuncCall(pstate, fn));
	}
	else
	{
		/* transition state is stored as a first-class type, no deserialization needed */
		args = list_make1(var);
	}

	return args;
}

/*
 * make_combine_agg_for_viewdef
 *
 * Creates a combine aggregate for a view definition against a matrel
 */
static Node *
make_combine_agg_for_viewdef(ParseState *pstate, RangeVar *cvrv, Var *var,
		 List *order, WindowDef *over)
{
	List *aggs;
	List *atts;
	List *args;
	Node *result;
	Oid fnoid;
	Oid combinefn;
	Oid combineinfn;
	Oid statetype;
	Query *q = GetContinuousQuery(cvrv);

	locate_aggs(q->targetList, &aggs, &atts);

	result = attr_to_agg(var->varattno, aggs, atts);

	if (IsA(result, Aggref))
	{
		Aggref *agg = (Aggref *) result;

		agg->aggkind = AGGKIND_USER_COMBINE;
		fnoid = agg->aggfnoid;
	}
	else
	{
		WindowFunc *w = (WindowFunc *) result;

		w->winaggkind = AGGKIND_USER_COMBINE;
		fnoid = w->winfnoid;
	}

	GetCombineInfo(fnoid, &combinefn, &combineinfn, &statetype);

	/* combine state is always adjacent to its visible column */
	if (OidIsValid(statetype))
	{
		var->varattno++;
		var->vartype = statetype;
	}

	args = make_combine_args(pstate, combineinfn, var);

	if (IsA(result, Aggref))
	{
		transformAggregateCall(pstate, (Aggref *) result, args, order, false);
	}
	else
	{
		((WindowFunc *) result)->args = args;
		transformWindowFuncCall(pstate, (WindowFunc *) result, over);
	}

	return (Node *) result;
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

		if (rte->relkind != RELKIND_VIEW)
			continue;

		foreach(clc, rte->eref->colnames)
		{
			Value *c = (Value *) lfirst(clc);
			Relation rel;
			RangeVar *rv;
			char *cvname;

			if (!equal(colname, c))
				continue;

			/*
			 * The view contains a column name that matches the target column,
			 * so we just need to verify that it's actually a continuous view.
			 */
			rel = heap_open(rte->relid, NoLock);
			cvname = RelationGetRelationName(rel);
			relation_close(rel, NoLock);
			rv = makeRangeVar(NULL, cvname, -1);

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
find_attr_from_joinlist(ParseState *pstate, RangeVar *cvrv, RangeTblEntry *joinrte, Var *var)
{
	RangeTblEntry *cvrte;
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
	ListCell *lc;
	Var *var;
	List *args;
	Oid combinefn;
	Oid combineinfn;
	Oid statetype;
	RangeVar *rv;
	bool ismatrel;
	Query *q;
	TargetEntry *target;
	List *nodes;
	List *aggs;
	List *atts;
	RangeTblEntry *rte;
	AttrNumber cvatt = InvalidAttrNumber;

	if (list_length(fargs) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with %d arguments", list_length(fargs)),
				 errhint("combine must be called with a single column reference as its argument.")));
	}

	arg = linitial(fargs);

	if (!IsA(arg, Var))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
				 errmsg("combine called with an invalid expression"),
				 errhint("combine must be called with a single column reference as its argument.")));
	}

	var = (Var *) arg;

	if (!combine_target_belongs_to_cv(var, pstate->p_rtable, &rv))
	{
		RangeVar *matrelrv;
		RangeVar *cvrv;
		Relation rel;
		RangeTblEntry *rte = list_nth(pstate->p_rtable, var->varno - 1);

		rel = heap_open(rte->relid, NoLock);
		matrelrv = makeRangeVar(NULL, RelationGetRelationName(rel), -1);
		relation_close(rel, NoLock);

		/*
		 * Sliding-window CQ's use combine aggregates in their
		 * view definition, so when they're created we can also
		 * end up here. We do this check second because it's slow.
		 */
		ismatrel = IsAMatRel(matrelrv, &cvrv);
		if (!ismatrel)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
					 errmsg("\"%s\" is not a continuous view", matrelrv->relname),
					 errhint("Only aggregate continuous view columns can be combined.")));
		}

		return make_combine_agg_for_viewdef(pstate, cvrv, var, order, over);
	}

	/* ok, it's a user combine query against an existing continuous view */
	q = GetContinuousQuery(rv);
	locate_aggs(q->targetList, &aggs, &atts);

	rte = (RangeTblEntry *) list_nth(pstate->p_rtable, var->varno - 1);
	cvatt = var->varattno;
	/*
	 * If this is a join, our varattno will point to the position of the target
	 * combine column within the joined target list, so we need to pull out the
	 * table-level var that will point us to the CQ's target list
	 */
	if (rte->rtekind == RTE_JOIN)
		cvatt = find_attr_from_joinlist(pstate, rv, rte, var);

	/*
	 * Find the aggregate node in the CQ that corresponds
	 * to the target combine column
	 */
	target = (TargetEntry *) list_nth(q->targetList, cvatt - 1);
	nodes = pull_var_clause((Node *) target->expr,
			PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

	foreach(lc, nodes)
	{
		Node *n = (Node *) lfirst(lc);
		Var *v;
		AttrNumber att;
		Oid fnoid;

		if (IsA(n, Aggref))
			fnoid = ((Aggref *) n)->aggfnoid;
		else if (IsA(n, WindowFunc))
			fnoid = ((WindowFunc *) n)->winfnoid;
		else
			continue;

		GetCombineInfo(fnoid, &combinefn, &combineinfn, &statetype);

		/* combine state is always adjacent to its visible column */
		att = agg_to_attr(n, aggs, atts);
		if (OidIsValid(statetype))
			att++;

		v = makeVar(var->varno, att, statetype, InvalidOid, InvalidOid, InvalidOid);
		args = make_combine_args(pstate, combineinfn, v);

		if (IsA(n, Aggref))
		{
			Aggref *agg = (Aggref *) n;
			agg->aggkind = AGGKIND_USER_COMBINE;
			transformAggregateCall(pstate, agg, args, order, false);
		}
		else if (IsA(n, WindowFunc))
		{
			WindowFunc *w = (WindowFunc *) n;
			w->args = args;
			w->winaggkind = AGGKIND_USER_COMBINE;
			transformWindowFuncCall(pstate, w, over);
		}
	}

	return copyObject((Node *) target->expr);
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
	context->tables = NIL;
	context->streams = NIL;
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
 * IsAColumnRef
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
 * RewriteStreamingAggs
 *
 * Replaces hypothetical set aggregate functions with their streaming
 * variants if possible, since we can't use the sorting approach that
 * the standard functions use.
 */
void
RewriteStreamingAggs(SelectStmt *stmt)
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
	ResTarget *timeRes;
	AttrNumber timeAttr;
	AttrNumber curAttr;

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
		timeRes = AddProjectionsAndGroupBysForWindows(workerstmt, viewstmt, doesViewAggregate, &context, &timeAttr);

	/*
	 * We can't use the standard hypothetical set aggregate functions because
	 * they require sorting the input set, so replace them with their
	 * streaming variants if possible.
	 */
	RewriteStreamingAggs(workerstmt);

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
	curAttr = 1;
	workerstmt->targetList = NIL;
	foreach(lc, workerTargetList)
	{
		ResTarget *res = (ResTarget *) copyObject(lfirst(lc));

		if (IsA(res->val, FuncCall))
		{
			FuncCall *fcall = (FuncCall *) res->val;
			fcall->over = NULL;
		}

		/*
		 * If applicable, put the time-based ResTarget where it
		 * originated in the original target list to maintain order
		 * between the input target list and the columns of the matrel
		 * we'll ultimately create.
		 */
		if (curAttr == timeAttr)
		{
			workerstmt->targetList = lappend(workerstmt->targetList, timeRes);
			curAttr++;
		}

		workerstmt->targetList = lappend(workerstmt->targetList, res);
		curAttr++;
	}

	/* time ResTarget wasn't in the target list, so just add it on to the end */
	if (!AttributeNumberIsValid(timeAttr))
		workerstmt->targetList = lappend(workerstmt->targetList, timeRes);

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
 * CollectAggrefs
 */
bool
CollectAggrefs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
		context->funcCalls = lappend(context->funcCalls, node);

	return raw_expression_tree_walker(node, CollectAggrefs, (void *) context);
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

	if (context.tables != NIL && context.streams == NIL)
	{
		RangeVar *t = (RangeVar *) linitial(context.tables);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("continuous queries must include a stream in the FROM clause"),
						errhint("To include a table in a continuous query, JOIN it with a stream."),
						parser_errposition(pstate, t->location)));
	}

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

/*
 * RewriteContinuousViewSelect
 *
 * Possibly modify a continuous view's SELECT rule before it's applied.
 * This is mainly used for including hidden columns in the view's subselect
 * query when they are needed by functions in the target list.
 */
Query *
RewriteContinuousViewSelect(Query *query, Query *rule, Relation cv)
{
	RangeVar *rv = makeRangeVar(NULL, RelationGetRelationName(cv), -1);
	ListCell *lc;
	List *targets = NIL;
	List *targetlist = NIL;
	AttrNumber matrelvarno = InvalidAttrNumber;
	TupleDesc matreldesc;
	bool needshidden = false;
	char *matrelname;
	int i;

	/* try to bail early because this gets called from a hot path */
	if (!IsAContinuousView(rv))
		return rule;

	matrelname = GetMatRelationName(RelationGetRelationName(cv));
	matreldesc = RelationNameGetTupleDesc(matrelname);

	targets = pull_var_clause((Node *) rule->targetList,
			PVC_RECURSE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

	foreach(lc, targets)
	{
		Node *n = (Node *) lfirst(lc);
		if (IsA(n, Var))
		{
			matrelvarno = ((Var *) n)->varno;
			break;
		}
	}

	/* is there anything to do? */
	if (!AttributeNumberIsValid(matrelvarno))
		return rule;

	targets = pull_var_clause((Node *) query->targetList,
			PVC_INCLUDE_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);

	/* pull out all the user combine aggregates */
	foreach(lc, targets)
	{
		Node *n = (Node *) lfirst(lc);
		if (IsA(n, Aggref) && AGGKIND_IS_USER_COMBINE(((Aggref *) n)->aggkind))
		{
			needshidden = true;
			break;
		}
	}

	if (!needshidden)
		return rule;

	/* we have user combines, so expose hidden columns */
	for (i=0; i<matreldesc->natts; i++)
	{
		Var *tev;
		TargetEntry *te;
		Form_pg_attribute attr = matreldesc->attrs[i];

		tev = makeVar(matrelvarno, attr->attnum, attr->atttypid,
				attr->atttypmod, attr->attcollation, 0);

		te = makeTargetEntry((Expr *) tev, tev->varattno, NULL, false);
		targetlist = lappend(targetlist, te);
	}

	rule->targetList = targetlist;

	return rule;
}
