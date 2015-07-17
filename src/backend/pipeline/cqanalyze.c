/*-------------------------------------------------------------------------
 *
 * cqanalyze.c
 *	  Support for analyzing continuous view statements, mainly to support
 *	  schema inference
 *
 * Copyright (c) 2013-2015, PipelineDB
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
#include "parser/parsetree.h"
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
		{"count", "hll_count_distinct"},
		{"percentile_cont", "cq_percentile_cont"}
};

static char *get_streaming_agg(FuncCall *fn);
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
		agg->aggfilter = NULL;
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
		matrelrv = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel), -1);
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
					 errhint("only aggregate continuous view columns can be combined.")));
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

			if (over)
			{
				/*
				 * We're doing a windowed combine over a non-windowed aggregate, so
				 * we need to transform the regular agg into a windowed agg.
				 */
				WindowFunc *wfunc = makeNode(WindowFunc);

				wfunc->winfnoid = agg->aggfnoid;
				wfunc->wintype = agg->aggtype;
				wfunc->args = args;
				wfunc->winstar = agg->aggstar;
				wfunc->winagg = true;
				wfunc->aggfilter = agg->aggfilter;
				wfunc->winaggkind = AGGKIND_USER_COMBINE;

				transformWindowFuncCall(pstate, wfunc, over);

				return (Node *) wfunc;
			}
			else
			{
				agg->aggkind = AGGKIND_USER_COMBINE;
				transformAggregateCall(pstate, agg, args, order, false);
			}
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

static void
find_top_level_column_refs(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		if (IsA(tc->arg, ColumnRef))
			context->cols = lappend(context->cols, node);
	}
	else if (IsA(node, ColumnRef))
		context->cols = lappend(context->cols, node);
	else if (IsA(node, A_Expr))
	{
		A_Expr *expr = (A_Expr *) node;
		find_top_level_column_refs(expr->lexpr, context);
		find_top_level_column_refs(expr->rexpr, context);
	}
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
		if (res->name != NULL && pg_strcasecmp(res->name, FigureColname((Node *) cref)) == 0)
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
	else if (IsA(node, FuncCall) || IsA(node, Expr) || (IsA(node, A_Expr)))
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
IsColumnRefInTargetList(List *targetList, Node *node)
{
	ColumnRef *cref;
	ListCell *lc;

	if (IsA(node, TypeCast))
		node = ((TypeCast *) node)->arg;

	Assert(IsA(node, ColumnRef));

	cref = (ColumnRef *) node;

	foreach(lc, targetList)
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
		bool is_agg = fn->agg_within_group;
		FuncCandidateList clist;

		if (!fn->agg_within_group)
		{
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
		}

		if (fn->agg_within_group || (is_agg && context))
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
 * CollectUserCombines
 *
 * Collect all combine aggregate calls
 */
bool
CollectUserCombines(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		if (AGGKIND_IS_USER_COMBINE(((Aggref *) node)->aggkind))
			context->combines = lappend(context->combines, node);
	}
	else if (IsA(node, WindowFunc))
	{
		if (AGGKIND_IS_USER_COMBINE(((WindowFunc *) node)->winaggkind))
			context->combines = lappend(context->combines, node);
	}

	return expression_tree_walker(node, CollectUserCombines, context);
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
 * IsNodeInTargetList
 */
bool
IsNodeInTargetList(List *targetlist, Node *node)
{
	ListCell *lc;

	foreach(lc, targetlist)
	{
		ResTarget *res = lfirst(lc);
		if (equal(res->val, node))
			return true;
	}

	return false;
}

/*
 * HoistNode
 */
Node *
HoistNode(SelectStmt *stmt, Node *node, CQAnalyzeContext *context)
{
	ResTarget *res;

	if (IsAColumnRef(node) && IsColumnRefInTargetList(stmt->targetList, node))
		return node;
	else if (IsNodeInTargetList(stmt->targetList, node))
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
	if (pg_strcasecmp(name, "count") == 0 && fn->agg_distinct == false)
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

static void
add_forced_types(SelectStmt *stmt, CQAnalyzeContext *context)
{
	ListCell *lc;

	context->funcCalls = NIL;
	CollectAggFuncs((Node *) stmt->targetList, context);

	foreach(lc, context->funcCalls)
	{
		FuncCall *fn = (FuncCall *) lfirst(lc);
		char *name = NameListToString(fn->funcname);

		if (pg_strcasecmp(name, "tdigest_agg") == 0)
		{
			FuncCall *cast = makeNode(FuncCall);
			cast->funcname = list_make1(makeString("float8"));
			cast->args = fn->args;
			fn->args = list_make1(cast);
		}
	}
}

/*
 * name_res_targets
 *
 * Give each ResTarget in the targetlist a name if it doesn't already have one
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
 * MakeSelectsContinuous
 *
 * Mark all SELECT queries as continuous. This is necessary for subqueries to be recognized
 * as continuous, as the grammar can't determine that.
 */
bool
MakeSelectsContinuous(Node *node, CQAnalyzeContext *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, SelectStmt))
		((SelectStmt *) node)->forContinuousView = true;

	return raw_expression_tree_walker(node, MakeSelectsContinuous, (void *) context);
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
	List *workerDistinct;
	bool isSlidingWindow;
	bool doesViewAggregate;
	int origNumTargets;
	int origNumGroups;
	int i;
	ListCell *lc;
	ResTarget *timeRes = NULL;
	AttrNumber timeAttr = InvalidAttrNumber;
	AttrNumber curAttr;

	InitializeCQAnalyzeContext(stmt, NULL, &context);
	AssociateTypesToColumRefs((Node *) stmt, &context);
	type_cast_all_column_refs((Node *) stmt, &context);
	name_res_targets(stmt->targetList);
	MakeSelectsContinuous((Node *) stmt, &context);

	workerstmt = (SelectStmt *) copyObject(stmt);
	viewstmt = (SelectStmt *) makeNode(SelectStmt);
	viewstmt->targetList = NIL;
	viewstmt->groupClause = NIL;

	origNumTargets = list_length(workerstmt->targetList);
	origNumGroups = list_length(workerstmt->groupClause);

	isSlidingWindow = IsSlidingWindowSelectStmt(workerstmt);
	doesViewAggregate = DoesViewAggregate(workerstmt, &context);

	/* SELECT DISTINCT x, ... => SELECT DISTINCT ON (x, ...) x, ... */
	if (equal(workerstmt->distinctClause, lcons(NIL, NIL)))
	{
		ListCell *lc;
		workerstmt->distinctClause = NIL;

		foreach(lc, workerstmt->targetList)
		{
			ResTarget *res = (ResTarget *) lfirst(lc);
			workerstmt->distinctClause = lappend(workerstmt->distinctClause, res->val);
		}
	}

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
		{
			/*
			 * In the view over the matrel, we need to make sure we use matrel column names and not
			 * the column names in the target list. A cref may be qualified here, whereas the corresponding
			 * matrel column name will not be, so a qualified cref will not refer to a valid matrel column.
			 */
			if (IsA(node, ColumnRef))
			{
				ColumnRef *cref = (ColumnRef *) copyObject(node);
				char *name = FigureColname(node);
				cref->fields = list_make1(makeString(name));
				node = (Node *) cref;
			}
			viewstmt->groupClause = lappend(viewstmt->groupClause, node);
		}
	}

	for (; i < list_length(workerstmt->groupClause); i++)
		workerGroups = lappend(workerGroups, list_nth(workerstmt->groupClause, i));

	workerstmt->groupClause = workerGroups;

	/*
	 * Rewrite the distinctClause and project and distinct expressions that
	 * are not in the targetList
	 */
	workerDistinct = NIL;
	foreach(lc, workerstmt->distinctClause)
	{
		Node *node = (Node *) lfirst(lc);
		node = HoistNode(workerstmt, node, &context);
		workerDistinct = lappend(workerDistinct, node);
	}
	workerstmt->distinctClause = workerDistinct;

	/*
	 * Hoist aggregates out of expressions for workers.
	 */
	workerTargetList = NIL;
	for (i = 0; i < origNumTargets; i++)
	{
		ResTarget *res = (ResTarget *) list_nth(workerstmt->targetList, i);
		FuncCall *agg;
		ListCell *lc;

		/*
		 * Any ResTarget with a ResTarget as a val should be ignored for the worker
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
		 * Hoist columns out of expressions.
		 */
		context.cols = NIL;
		find_top_level_column_refs(res->val, &context);
		foreach(lc, context.cols)
		{
			Node *node = (Node *) lfirst(lc);
			if (!IsColumnRefInTargetList(workerTargetList, node))
				workerTargetList = lappend(workerTargetList, CreateResTargetForNode(node));
		}

		/*
		 * Hoist each agg function call into a new hidden column
		 * and reference that hidden column in the expression which
		 * is evaluated by the view.
		 */
		foreach(lc, context.funcCalls)
		{
			Node *node = (Node *) lfirst(lc);
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
	if (!AttributeNumberIsValid(timeAttr) && timeRes != NULL)
		workerstmt->targetList = lappend(workerstmt->targetList, timeRes);

	/*
	 * Copy over WINDOW and LIMIT clauses to the view and remove from the
	 * worker.
	 */
	viewstmt->windowClause = workerstmt->windowClause;
	workerstmt->windowClause = NIL;

	viewstmt->limitCount = workerstmt->limitCount;
	viewstmt->limitOffset = workerstmt->limitOffset;
	workerstmt->limitCount = NULL;
	workerstmt->limitOffset = NULL;

	viewstmt->forContinuousView = false;

	if (viewstmtptr != NULL)
		*viewstmtptr = viewstmt;

	add_forced_types(workerstmt, &context);

	return workerstmt;
}

/*
 * GetSelectStmtForCQCombiner
 */
SelectStmt *
GetSelectStmtForCQCombiner(SelectStmt *stmt)
{
	CQAnalyzeContext context;
	ListCell *lc;

	InitializeCQAnalyzeContext(stmt, NULL, &context);
	stmt = GetSelectStmtForCQWorker(stmt, NULL);

	/*
	 * Combiner shouldn't have to re-do the filtering work
	 * of the WHERE clause.
	 */
	stmt->whereClause = NULL;

	/* Remove any FILTER clauses */
	context.funcCalls = NIL;
	CollectAggFuncs((Node *) stmt->targetList, &context);

	foreach(lc, context.funcCalls)
	{
		FuncCall *fcall = (FuncCall *) lfirst(lc);
		fcall->agg_filter = NULL;
	}

	return stmt;
}

/*
 * associate_types_to_colrefs
 *
 * Walk the parse tree and associate a single type with each inferred column
 */
bool
AssociateTypesToColumRefs(Node *node, CQAnalyzeContext *context)
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

	return raw_expression_tree_walker(node, AssociateTypesToColumRefs, (void *) context);
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
 * pipeline_rewrite
 *
 * Take the list of parsetrees returned by `pg_parse_query` and
 * output a new list of parsetrees.
 */
List *
pipeline_rewrite(List *raw_parsetree_list)
{
	ListCell *lc;

	foreach(lc, raw_parsetree_list)
	{
		Node *node = lfirst(lc);

		if (IsA(node, VacuumStmt))
		{
			VacuumStmt *vstmt = (VacuumStmt *) node;
			/*
			 * If the user is trying to vacuum a CV, what they're really
			 * trying to do is create it on the CV's materialization table, so rewrite
			 * the name of the target relation if we need to.
			 */
			if (vstmt->relation && IsAContinuousView(vstmt->relation))
				vstmt->relation = GetMatRelationName(vstmt->relation);
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
RewriteContinuousViewSelect(Query *query, Query *rule, Relation cv, int rtindex)
{
	RangeVar *rv = makeRangeVar(get_namespace_name(RelationGetNamespace(cv)), RelationGetRelationName(cv), -1);
	ListCell *lc;
	List *targets = NIL;
	List *targetlist = NIL;
	AttrNumber matrelvarno = InvalidAttrNumber;
	TupleDesc matreldesc;
	RangeVar *matrelname;
	Relation matrel;
	int i;
	RangeTblEntry *rte;
	List *colnames = NIL;
	CQAnalyzeContext context;

	/* try to bail early because this gets called from a hot path */
	if (!IsAContinuousView(rv))
		return rule;

	matrelname = GetMatRelationName(rv);
	matrel = heap_openrv(matrelname, NoLock);
	matreldesc = CreateTupleDescCopyConstr(RelationGetDescr(matrel));
	heap_close(matrel, NoLock);

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

	context.combines = NIL;
	CollectUserCombines((Node *) query->targetList, &context);

	if (!context.combines)
		return rule;

	/* we're scanning a matrel, so include hidden columns */
	for (i=0; i<matreldesc->natts; i++)
	{
		Var *tev;
		TargetEntry *te;
		Form_pg_attribute attr = matreldesc->attrs[i];
		ListCell *tlc;

		tev = makeVar(matrelvarno, attr->attnum, attr->atttypid,
				attr->atttypmod, attr->attcollation, 0);

		te = makeTargetEntry((Expr *) tev, tev->varattno, NULL, false);
		te->resname = NameStr(attr->attname);
		colnames = lappend(colnames, makeString(te->resname));

		/*
		 * Preserve the sortgrouprefs. Note that nonzero sortgrouprefs
		 * don't correspond to anything in the target list. They just
		 * need to be unique so we can just copy the old one into the
		 * new target entry.
		 */
		foreach(tlc, rule->targetList)
		{
			TargetEntry *rte = (TargetEntry *) lfirst(tlc);
			if (pg_strcasecmp(rte->resname, te->resname) == 0)
				te->ressortgroupref = rte->ressortgroupref;
		}

		targetlist = lappend(targetlist, te);
	}

	rte = rt_fetch(rtindex, query->rtable);
	rte->eref->colnames = colnames;
	rule->targetList = targetlist;

	return rule;
}

/*
 * SelectsFromStreamOnly
 */
bool
SelectsFromStreamOnly(SelectStmt *stmt)
{
	CQAnalyzeContext context;

	context.tables = NIL;
	context.streams = NIL;
	AddStreams((Node *) stmt->fromClause, &context);

	return context.tables == NIL;
}
