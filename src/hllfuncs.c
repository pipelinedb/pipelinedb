/*-------------------------------------------------------------------------
 *
 * hllfuncs.c
 *		HyperLogLog-based functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "analyzer.h"
#include "executor/executor.h"
#include "hll.h"
#include "hllfuncs.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscutils.h"
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/tlist.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "scheduler.h"
#include "tdigest.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/sortsupport.h"
#include "utils/typcache.h"

typedef struct CQHSPerQueryState
{
	/* slot for inserting elements into the HLL */
	TupleTableSlot *curslot;
	/* slot containing the tuple representing the direct args to this HS agg */
	TupleTableSlot *directslot;
	SortSupport sort;
	int direct_offset;
} CQHSPerQueryState;

/*
 * hll_in
 *
 * hyperloglog type support function
 */
PG_FUNCTION_INFO_V1(hll_in);
Datum
hll_in(PG_FUNCTION_ARGS)
{
	char *raw = PG_GETARG_CSTRING(0);
	Datum result = DirectFunctionCall1(byteain, (Datum) raw);

	PG_RETURN_POINTER(result);
}

/*
 * hll_out
 *
 * hyperloglog type support function
 */
PG_FUNCTION_INFO_V1(hll_out);
Datum
hll_out(PG_FUNCTION_ARGS)
{
	HyperLogLog	*hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
	char *s = (char *) DirectFunctionCall1(byteaout, (Datum) hll);

	PG_RETURN_CSTRING(s);
}

/*
 * hll_send
 *
 * hyperloglog type support function
 */
PG_FUNCTION_INFO_V1(hll_send);
Datum
hll_send(PG_FUNCTION_ARGS)
{
	HyperLogLog	*hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
	bytea *result = (bytea *) DirectFunctionCall1(byteasend, (Datum) hll);

	PG_RETURN_BYTEA_P(result);
}

/*
 * hll_recv
 *
 * hyperloglog type support function
 */
PG_FUNCTION_INFO_V1(hll_recv);
Datum
hll_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea *result = (bytea *) DirectFunctionCall1(bytearecv, (Datum) buf);

	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(hll_print);
Datum
hll_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ p = %d, cardinality = %ld, size = %dkB }", hll->p, HLLCardinality(hll), hll->mlen / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
}

static HyperLogLog *
hll_create(int p)
{
	if (p > 14 || p < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("p must be in [1, 14]")));

	return HLLCreateWithP(p);
}

static HyperLogLog *
hll_startup(FunctionCallInfo fcinfo, int p)
{
	HyperLogLog *hll;
	Oid type = AggGetInitialArgType(fcinfo);
	MemoryContext old;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);
	MemoryContextSwitchTo(old);

	if (p > 0)
		hll = hll_create(p);
	else
		hll = HLLCreate();

	return hll;
}

static HyperLogLog *
hll_add_datum(FunctionCallInfo fcinfo, HyperLogLog *hll, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	StringInfo buf;
	int result;

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);
	hll = HLLAdd(hll, buf->data, buf->len, &result);

	SET_VARSIZE(hll, HLLSize(hll));

	pfree(buf->data);
	pfree(buf);

	return hll;
}

/*
 * hll_agg transition function -
 * 	adds the given element to the transition HLL
 */
PG_FUNCTION_INFO_V1(hll_agg_trans);
Datum
hll_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = hll_startup(fcinfo, 0);
	else
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = hll_add_datum(fcinfo, state, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * hll_agg transition function -
 *
 * 	adds the given element to the transition HLL using the given value for p
 */
PG_FUNCTION_INFO_V1(hll_agg_transp);
Datum
hll_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;
	int p = PG_GETARG_INT32(2);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = hll_startup(fcinfo, p);
	else
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = hll_add_datum(fcinfo, state, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * hll_union_agg transition function -
 *
 * 	returns the union of the transition state and the given HLL
 */
PG_FUNCTION_INFO_V1(hll_union_agg_trans);
Datum
hll_union_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;
	HyperLogLog *incoming;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_union_agg_trans called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		incoming = (HyperLogLog *) PG_GETARG_VARLENA_P(1);
		state = HLLCopy(incoming);
		MemoryContextSwitchTo(old);
	}
	else if (PG_ARGISNULL(1))
	{
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
	}
	else
	{
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
		incoming = (HyperLogLog *) PG_GETARG_VARLENA_P(1);

		if (IsContQueryProcess())
			state = HLLUnion(state, incoming);
		else
			state = HLLUnionAdd(state, incoming);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * hll_union
 *
 * Returns the union of the given HLLs
 */
PG_FUNCTION_INFO_V1(hll_union);
Datum
hll_union(PG_FUNCTION_ARGS)
{
	Oid hll_type = GetTypeOid(HLL_TYPENAME);
	HyperLogLog *result = NULL;
	int i;

	for (i = 0; i < PG_NARGS(); i++)
	{
		HyperLogLog *hll;
		if (PG_ARGISNULL(i))
			continue;

		if (get_fn_expr_argtype(fcinfo->flinfo, i) != hll_type)
			elog(ERROR, "argument %d is not of type \"%s\"", i + 1, HLL_TYPENAME);

		hll = (HyperLogLog *) PG_GETARG_VARLENA_P(i);
		if (result)
		{
			if (hll->p != result->p)
				elog(ERROR, "hyperloglogs must have the same p");
		}

		if (result == NULL)
			result = hll;
		else
			result = HLLUnion(result, hll);
	}

	if (result == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(result);
}

/*
 * Returns the cardinality of the given HLL
 */
PG_FUNCTION_INFO_V1(hll_cardinality);
Datum
hll_cardinality(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
	if (HLL_IS_UNPACKED(hll))
		hll = HLLPack(hll);

	PG_RETURN_INT64(HLLCardinality(hll));
}

/*
 * hll_cache_cardinality
 *
 * Computes and stores the cardinality of the given HLL
 */
PG_FUNCTION_INFO_V1(hll_cache_cardinality);
Datum
hll_cache_cardinality(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);
	/* Calling this will cache the cardinality */

	/* We need to pack this first since we need to return a pointer to the HLL */
	if (HLL_IS_UNPACKED(hll))
		hll = HLLPack(hll);

	HLLCardinality(hll);

	PG_RETURN_POINTER(hll);
}

PG_FUNCTION_INFO_V1(hll_empty);
Datum
hll_empty(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(HLLCreate());
}

PG_FUNCTION_INFO_V1(hll_emptyp);
Datum
hll_emptyp(PG_FUNCTION_ARGS)
{
	int p = PG_GETARG_INT32(0);
	HyperLogLog *hll = hll_create(p);
	PG_RETURN_POINTER(hll);
}

PG_FUNCTION_INFO_V1(hll_add);
Datum
hll_add(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		hll = HLLCreate();
	else
		hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	/* Non-dense representations can be repalloc'd so create a copy */
	if (!HLL_IS_DENSE(hll))
		hll = HLLCopy(hll);

	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	hll = hll_add_datum(fcinfo, hll, PG_GETARG_DATUM(1));
	PG_RETURN_POINTER(hll);
}


static HyperLogLog *
hll_count_distinct_startup(PG_FUNCTION_ARGS)
{
	Oid type = AggGetInitialArgType(fcinfo);
	MemoryContext old;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);
	MemoryContextSwitchTo(old);

	return HLLCreate();
}

/*
 * COUNT DISTINCT support functions for streaming inputs,
 * using HyperLogLog to determine input uniqueness
 */
PG_FUNCTION_INFO_V1(hll_count_distinct_transition);
Datum
hll_count_distinct_transition(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *hll;
	int result;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		hll = hll_count_distinct_startup(fcinfo);
	else
		hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
	{
		StringInfo buf = makeStringInfo();
		TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;

		DatumToBytes(PG_GETARG_DATUM(1), typ, buf);

		hll = HLLAdd(hll, buf->data, buf->len, &result);
		SET_VARSIZE(hll, HLLSize(hll));

		pfree(buf->data);
		pfree(buf);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(hll);
}

/*
 * Returns the cardinality of the final HyperLogLog
 */
PG_FUNCTION_INFO_V1(hll_count_distinct_final);
Datum
hll_count_distinct_final(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(HLLCardinality(hll));
}

/*
 * noop deserialization function
 */
PG_FUNCTION_INFO_V1(hll_serialize);
Datum
hll_serialize(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	/* We need to pack this first since we need to return a pointer to the HLL */
	if (HLL_IS_UNPACKED(hll))
		hll = HLLPack(hll);

	PG_RETURN_BYTEA_P(hll);
}

/*
 * noop deserialization function
 */
PG_FUNCTION_INFO_V1(hll_deserialize);
Datum
hll_deserialize(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_POINTER(hll);
}

/*
 * Set up query-level working state for continuous hypothetical-set aggregates
 */
static CQHSPerQueryState *
cq_hypothetical_set_per_query_startup(FunctionCallInfo fcinfo)
{
	CQHSPerQueryState *qstate;
	MemoryContext old;
	Aggref *aggref = AggGetAggref(fcinfo);
	ExprContext *econtext = CreateStandaloneExprContext();
	TupleDesc directdesc;
	TupleDesc sortdesc;
	ListCell *lc;
	List *sortlist;
	List *directargs;
	Datum *values;
	bool *nulls;
	List *directexprs;
	List *sortargs;
	int i;

	/* our state needs to live for the duration of the query */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	qstate = palloc0(sizeof(CQHSPerQueryState));

	/*
	 * OS/HS aggregates are not combinable in PG, so we just can't use them in CQs right now.
	 * The standard OS/HS aggregate interface looks something like this:
	 *
	 * ... agg(a, b) WITHIN GROUP (ORDER BY x, y) ...
	 *
	 * In this case, a and b (constants) would be in the Aggref's direct args, and x and y (variables)
	 * would be given as an ORDER BY arg.
	 *
	 * Since streaming OS/HS aggregates don't require actual sorting anyways, we provide streaming
	 * variants of OS/HS aggregates that just use regular aggregates:
	 *
	 * ... agg(a, b, x, y) ...
	 *
	 * And then we pull out the first n constants, with the remaining arguments assumed to be ORDER BY
	 * variables. We also perform some validation to ensure the aggregate's argument list is valid,
	 * in particular the type descriptors of the "direct" and ORDER BY arguments are equivalent.
	 */

	/*
	 * The first n constants are considered our "direct" arguments
	 */
	directargs = NIL;
	directexprs = NIL;

	foreach(lc, aggref->args)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Expr *expr = copyObject(te->expr);

		if (!IsA(expr, Const))
			break;

		directexprs = lappend(directexprs, ExecInitExpr(expr, NULL));
		directargs = lappend(directargs, te);
	}

	if (!directargs)
		elog(ERROR, "expected constant expressions at the beginning of the argument list");

	directdesc = ExecTypeFromTL(directargs, false);
	values = palloc0(directdesc->natts * sizeof(Datum));
	nulls = palloc0(directdesc->natts * sizeof(bool));

	i = 0;
	foreach(lc, directexprs)
	{
		ExprState  *expr = (ExprState *) lfirst(lc);

		values[i] = ExecEvalExpr(expr, econtext, &nulls[i]);
		i++;
	}

	qstate->directslot = MakeSingleTupleTableSlot(directdesc);
	qstate->directslot->tts_values = values;
	qstate->directslot->tts_isnull = nulls;

	ExecStoreVirtualTuple(qstate->directslot);

	/*
	 * During aggregation, the "direct" arguments are ignored since they're constant.
	 * We just want to look at the ORDER BY expressions, which come after the first
	 * n constant arguments. This offset will tell us where that is.
	 */
	qstate->direct_offset = i;

	/*
	 * The remaining arguments given to the Aggref are considered our ORDER BY expressions
	 */
	sortlist = NIL;
	sortargs = NIL;

	for (; i < list_length(aggref->args); i++)
	{
		TargetEntry *te = (TargetEntry *) list_nth(aggref->args, i);
		SortGroupClause *sortcl = makeNode(SortGroupClause);

		if (IsA(te->expr, Const))
			elog(ERROR, "constant in argument list where not expected");

		sortcl->tleSortGroupRef = assignSortGroupRef(te, aggref->args);
		get_sort_group_operators(exprType((Node *) te->expr),
				true, true, false, &sortcl->sortop, &sortcl->eqop, NULL, &sortcl->hashable);

		sortlist = lappend(sortlist, sortcl);
		sortargs = lappend(sortargs, te);
	}

	sortdesc = ExecTypeFromTL(sortargs, false);

	if (!equalTupleDescs(directdesc, sortdesc))
		elog(ERROR, "sort expressions must have the same type as input expressions");

	qstate->curslot = MakeSingleTupleTableSlot(sortdesc);

	/*
	 * Build our comparator for determining how an input tuple compares to our
	 * direct argument tuple
	 */
	qstate->sort = (SortSupport) palloc0(sortdesc->natts * sizeof(SortSupportData));
	i = 0;
	foreach(lc, sortlist)
	{
		SortSupport sortkey = qstate->sort + i;
		SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl,
												   aggref->args);

		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = exprCollation((Node *) tle->expr);
		/*
		 * Since streaming HS aggregates can't use WITHIN GROUP, sort ordering will always be ascending
		 * and users can use signs to determine sort order.
		 */
		sortkey->ssup_nulls_first = false;
		sortkey->ssup_attno = tle->resno;

		PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);
		i++;
	}

	fcinfo->flinfo->fn_extra = (void *) qstate;
	MemoryContextSwitchTo(old);

	FreeExprContext(econtext, false);

	return qstate;
}

static HyperLogLog *
hll_hypothetical_set_startup(FunctionCallInfo fcinfo)
{
	CQHSPerQueryState *qstate;

	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	if (qstate == NULL)
		qstate = cq_hypothetical_set_per_query_startup(fcinfo);

	return HLLCreate();
}

static int
compare_slots(TupleTableSlot *s0, TupleTableSlot *s1, SortSupport sort)
{
	int i;
	for (i=0; i<s0->tts_tupleDescriptor->natts; i++)
	{
		bool n0;
		bool n1;
		Datum d0 = slot_getattr(s0, i + 1, &n0);
		Datum d1 = slot_getattr(s1, i + 1, &n1);
		SortSupport sortkey = sort + i;

		int result = ApplySortComparator(d0, n0, d1, n1, sortkey);

		if (result != 0)
			return result;
	}

	/* they must be equal */
	return 0;
}

/*
 * Transition function for streaming ordered-set aggregates
 * with (potentially) multiple input columns. It uses HyperLogLog
 * instead of actually sorting the input to determine input value uniqueness.
 */
PG_FUNCTION_INFO_V1(hll_hypothetical_set_transition_multi);
Datum
hll_hypothetical_set_transition_multi(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *hll;
	CQHSPerQueryState *qstate;
	int nargs;
	int i;
	int result;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		hll = hll_hypothetical_set_startup(fcinfo);
	else
		hll = (HyperLogLog *)  PG_GETARG_POINTER(0);

	/* this is created once by the first call to cq_hypothetical_set_startup */
	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	/* load the input values into our slot and add it to our HLL */
	ExecClearTuple(qstate->curslot);
	nargs = PG_NARGS() - 1;

	for (i = qstate->direct_offset; i < nargs; i++)
	{
		qstate->curslot->tts_values[i - qstate->direct_offset] = PG_GETARG_DATUM(i + 1);
		qstate->curslot->tts_isnull[i - qstate->direct_offset] = PG_ARGISNULL(i + 1);
	}
	ExecStoreVirtualTuple(qstate->curslot);

	result = compare_slots(qstate->curslot, qstate->directslot, qstate->sort);

	if (result < 0)
	{
		int unique;
		StringInfo buf = makeStringInfo();

		SlotAttrsToBytes(qstate->curslot, -1, NULL, buf);

		/*
		 * dense rank is only increased once for each lower-ranking tuple we see,
		 * so that the step size to the next highest-ranking tuple is always 1.
		 */
		hll = HLLAdd(hll, buf->data, buf->len, &unique);
	}

	SET_VARSIZE(hll, sizeof(HyperLogLog) + hll->mlen);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(hll);
}

/*
 * hll_dense_rank()  - dense rank of hypothetical row using HyperLogLog
 */
PG_FUNCTION_INFO_V1(hll_hypothetical_dense_rank_final);
Datum
hll_hypothetical_dense_rank_final(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(1);

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(HLLCardinality(hll) + 1);
}

typedef struct CQOSAAggState
{
	TDigest *tdigest;
	bool is_multiple;
	bool is_descending;
	int num_percentiles;
	float8 *percentiles;
	bool *nulls;
} CQOSAAggState;

static CQOSAAggState *
cq_percentile_cont_float8_startup(PG_FUNCTION_ARGS, bool is_multiple)
{
	CQOSAAggState *aggstate;
	MemoryContext old;
	Aggref *aggref = AggGetAggref(fcinfo);
	Datum value;
	bool isnull;
	ExprContext *econtext;
	ExprState *expr;
	SortGroupClause *sortcl;
	List *directargs;
	List *directexprs;
	ListCell *lc;
	TargetEntry *te;

	/*
	 * OS/HS aggregates are not combinable in PG, so we just can't use them in CQs right now.
	 * The standard OS/HS aggregate interface looks something like this:
	 *
	 * ... agg(a, b) WITHIN GROUP (ORDER BY x, y) ...
	 *
	 * In this case, a and b (constants) would be in the Aggref's direct args, and x and y (variables)
	 * would be given as an ORDER BY arg.
	 *
	 * Since streaming OS/HS aggregates don't require actual sorting anyways, we provide streaming
	 * variants of OS/HS aggregates that just use regular aggregates:
	 *
	 * ... agg(a, b, x, y) ...
	 *
	 * And then we pull out the first n constants, with the remaining arguments assumed to be ORDER BY
	 * variables. We also perform some validation to ensure the aggregate's argument list is valid,
	 * in particular the type descriptors of the "direct" and ORDER BY arguments are equivalent.
	 */
	directargs = NIL;
	directexprs = NIL;

	foreach(lc, aggref->args)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		Expr *expr = copyObject(te->expr);

		if (!IsA(expr, Const))
			break;

		directexprs = lappend(directexprs, ExecInitExpr(expr, NULL));
		directargs = lappend(directargs, te);
	}

	if (!directargs)
		elog(ERROR, "expected constant expressions at the beginning of the argument list");

	if (list_length(directargs) != 1)
		elog(ERROR, "expected variable expression following initial constant argument");

	/*
	 * Percentiles only allow a single ORDER BY expression
	 */
	Assert(list_length(directargs) == 1);
	te = list_nth(aggref->args, 1);

	sortcl = makeNode(SortGroupClause);
	sortcl->tleSortGroupRef = assignSortGroupRef(te, aggref->args);
	get_sort_group_operators(exprType((Node *) te->expr),
			true, true, false, &sortcl->sortop, &sortcl->eqop, NULL, &sortcl->hashable);

	/* our state needs to live for the duration of the query */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	/*
	 * We need to store the arguments to this aggregate function in the AggState because
	 * only the transition state is passed to the final_fn for CQ aggregates. Unlike regular
	 * aggregates where we can just read the incoming directargs in the final_fn.
	 */
	aggstate = palloc0(sizeof(CQOSAAggState));
	aggstate->tdigest = TDigestCreate();
	aggstate->is_multiple = is_multiple;

	/*
	 * Since CQ percentiles can't use WITHIN GROUP, sort ordering will always be ascending
	 * and users can use signs to determine sort order.
	 */
	aggstate->is_descending = false;

	econtext = CreateStandaloneExprContext();
	expr = (ExprState *) linitial(directexprs);
	value = ExecEvalExpr(expr, econtext, &isnull);

	if (isnull)
		aggstate->num_percentiles = 0;
	else if (is_multiple)
	{
		ArrayType *param = DatumGetArrayTypeP(value);
		Datum *percentiles_datum;
		int i;

		deconstruct_array(param, FLOAT8OID,
				/* hard-wired info on type float8 */
				8, FLOAT8PASSBYVAL, 'd',
				&percentiles_datum,
				&aggstate->nulls,
				&aggstate->num_percentiles);

		aggstate->percentiles = (float8 *) palloc(sizeof(float8) * aggstate->num_percentiles);

		for (i = 0; i < aggstate->num_percentiles; i++)
			aggstate->percentiles[i] = DatumGetFloat8(percentiles_datum[i]);
	}
	else
	{
		aggstate->num_percentiles = 1;
		aggstate->percentiles = (float8 *) palloc(sizeof(float8));
		aggstate->nulls = (bool *) palloc(sizeof(bool));
		aggstate->percentiles[0] = DatumGetFloat8(value);
		aggstate->nulls[0] = false;
	}

	MemoryContextSwitchTo(old);

	FreeExprContext(econtext, false);

	return aggstate;
}

/*
 * Transition function for percentile_cont aggregates with a single
 * numeric column. It uses t-digest to estimate quantiles.
 */
static Datum
cq_percentile_cont_float8_transition_common(PG_FUNCTION_ARGS, bool is_multiple)
{
	MemoryContext old;
	MemoryContext context;
	CQOSAAggState *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cq_percentile_cont_float8_startup(fcinfo, is_multiple);
	else
		state = (CQOSAAggState *)  PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(2))
	{
		Datum d = PG_GETARG_DATUM(2);
		state->tdigest = TDigestAdd(state->tdigest, DatumGetFloat8(d), 1);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

PG_FUNCTION_INFO_V1(cq_percentile_cont_float8_transition);
Datum
cq_percentile_cont_float8_transition(PG_FUNCTION_ARGS)
{
	return cq_percentile_cont_float8_transition_common(fcinfo, false);
}

PG_FUNCTION_INFO_V1(cq_percentile_cont_float8_transition_multi);
Datum
cq_percentile_cont_float8_transition_multi(PG_FUNCTION_ARGS)
{
	return cq_percentile_cont_float8_transition_common(fcinfo, true);
}

/*
 * combines multiple t-digest transition states into one
 */
PG_FUNCTION_INFO_V1(cq_percentile_cont_float8_combine);
Datum
cq_percentile_cont_float8_combine(PG_FUNCTION_ARGS)
{
	MemoryContext context;
	MemoryContext old;
	CQOSAAggState *state;
	CQOSAAggState *incoming = PG_ARGISNULL(1) ? NULL : (CQOSAAggState *) PG_GETARG_POINTER(1);

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	if (!incoming)
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = (CQOSAAggState *) palloc0(sizeof(CQOSAAggState));
		memcpy(state, incoming, sizeof(CQOSAAggState));
		state->tdigest = TDigestCopy(incoming->tdigest);
		state->percentiles = (float8 *) palloc(sizeof(float8) * state->num_percentiles);
		state->nulls = (bool *) palloc(sizeof(bool) * state->num_percentiles);
		memcpy(state->percentiles, incoming->percentiles, sizeof(float8) * state->num_percentiles);
		memcpy(state->nulls, incoming->nulls, sizeof(bool) * state->num_percentiles);
	}
	else
	{
		state = (CQOSAAggState *) PG_GETARG_POINTER(0);
		state->tdigest = TDigestMerge(state->tdigest, incoming->tdigest);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
  * percentile_cont(float8) within group (float8) - streaming continuous percentile
 */
PG_FUNCTION_INFO_V1(cq_percentile_cont_float8_final);
Datum
cq_percentile_cont_float8_final(PG_FUNCTION_ARGS)
{
	CQOSAAggState *state;
	float8 percentile;
	int i;
	Datum *result_datum;

	/* If there were no regular rows, the result is NULL */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	state = (CQOSAAggState *) PG_GETARG_POINTER(0);
	state->tdigest = TDigestCompress(state->tdigest);

	/* number_of_rows could be zero if we only saw NULL input values */
	if (state->tdigest->total_weight == 0)
		PG_RETURN_NULL();

	if (state->num_percentiles == 0)
	{
		if (state->is_multiple)
			PG_RETURN_POINTER(construct_empty_array(FLOAT8OID));
		else
			PG_RETURN_NULL();
	}

	result_datum = (Datum *) palloc(state->num_percentiles * sizeof(Datum));

	for (i = 0; i < state->num_percentiles; i++)
	{
		percentile = state->percentiles[i];

		if (percentile < 0 || percentile > 1 || isnan(percentile))
			ereport(ERROR,
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							errmsg("percentile value %g is not between 0 and 1",
									percentile)));

		if (state->is_descending)
			percentile = 1.0 - percentile;

		if (state->nulls[i])
			continue;

		result_datum[i] = Float8GetDatum(TDigestQuantile(state->tdigest, percentile));
	}

	if (state->is_multiple)
	{
		/* We make the output array the same shape as the input;
		 * hard-wired info on type float8 */
		int dims[1];
		int lbs[1];
		dims[0] = state->num_percentiles;
		lbs[0] = 1;
		PG_RETURN_POINTER(construct_md_array(result_datum, state->nulls,
											 1,
											 dims,
											 lbs,
											 FLOAT8OID,
											 8,
											 FLOAT8PASSBYVAL,
											 'd'));
	}

	PG_RETURN_DATUM(result_datum[0]);
}

static ArrayType *
cq_hypothetical_set_startup(FunctionCallInfo fcinfo)
{
	ArrayType *state;
	CQHSPerQueryState *qstate;
	Datum *values = palloc0(3 * sizeof(Datum));

	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	if (qstate == NULL)
		qstate = cq_hypothetical_set_per_query_startup(fcinfo);

	state = construct_array(values, 3, 20, 8, true, 'i');

	return state;
}

/*
 * Generic transition function for streaming ordered-set aggregates
 * with (potentially) multiple input columns
 */
PG_FUNCTION_INFO_V1(cq_hypothetical_set_transition_multi);
Datum
cq_hypothetical_set_transition_multi(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	ArrayType *state;
	CQHSPerQueryState *qstate;
	uint64 *transvalues;
	int nargs;
	int i;
	int result;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cq_hypothetical_set_startup(fcinfo);
	else
		state = (ArrayType *)  PG_GETARG_POINTER(0);

	/* this is created once by the first call to cq_hypothetical_set_startup */
	qstate = (CQHSPerQueryState *) fcinfo->flinfo->fn_extra;

	/* load the input values into our slot and add it to our HLL */
	ExecClearTuple(qstate->curslot);
	nargs = PG_NARGS() - 1;
	for (i = qstate->direct_offset; i < nargs; i++)
	{
		qstate->curslot->tts_values[i - qstate->direct_offset] = PG_GETARG_DATUM(i + 1);
		qstate->curslot->tts_isnull[i - qstate->direct_offset] = PG_ARGISNULL(i + 1);
	}
	ExecStoreVirtualTuple(qstate->curslot);

	MemoryContextSwitchTo(old);

	result = compare_slots(qstate->curslot, qstate->directslot, qstate->sort);
	transvalues = (uint64 *) ARR_DATA_PTR(state);

	/* row count */
	transvalues[0]++;

	/* rank */
	if (result < 0)
		transvalues[1]++;
	else if (result == 0)
		transvalues[2]++;

	PG_RETURN_ARRAYTYPE_P(state);
}

/*
 * combines multiple continuous hypothetical-set transition states into one
 */
PG_FUNCTION_INFO_V1(cq_hypothetical_set_combine_multi);
Datum
cq_hypothetical_set_combine_multi(PG_FUNCTION_ARGS)
{
	ArrayType *statearr;
	uint64 *state;
	ArrayType *incomingarr = (ArrayType *) PG_GETARG_ARRAYTYPE_P(1);
	uint64 *incoming = (uint64 *) ARR_DATA_PTR(incomingarr);

	if (!AggCheckCallContext(fcinfo, NULL))
			elog(ERROR, "aggregate function called in non-aggregate context");

	if (PG_ARGISNULL(0))
	{
		statearr = (ArrayType *) PG_GETARG_ARRAYTYPE_P_COPY(1);

		PG_RETURN_POINTER(statearr);
	}

	statearr = (ArrayType *) PG_GETARG_ARRAYTYPE_P(0);
	state = (uint64 *) ARR_DATA_PTR(statearr);

	state[0] += incoming[0];
	state[1] += incoming[1];
	state[2] += incoming[2];

	PG_RETURN_POINTER(statearr);
}

/*
 * cq_rank()  - streaming rank of hypothetical row
 */
PG_FUNCTION_INFO_V1(cq_hypothetical_rank_final);
Datum
cq_hypothetical_rank_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(1);

	state = (ArrayType *) PG_GETARG_ARRAYTYPE_P(0);
	values = (uint64 *) ARR_DATA_PTR(state);

	/* minimum rank is always 1 */
	PG_RETURN_INT64(values[1] + 1);
}

/*
 * cq_percent_rank()  - streaming percent_rank of hypothetical row
 */
PG_FUNCTION_INFO_V1(cq_hypothetical_percent_rank_final);
Datum
cq_hypothetical_percent_rank_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;
	uint64 rowcount;
	uint64 rank;
	double result;

	if (PG_ARGISNULL(0))
		PG_RETURN_FLOAT8(0);

	state = (ArrayType *) PG_GETARG_ARRAYTYPE_P(0);
	values = (uint64 *) ARR_DATA_PTR(state);
	rowcount = values[0];
	rank = values[1] + 1;

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	result = (double) (rank - 1) / (double) (rowcount);

	PG_RETURN_FLOAT8(result);
}

/*
 * cume_dist()  - streaming cume_dist of hypothetical row
 */
PG_FUNCTION_INFO_V1(cq_hypothetical_cume_dist_final);
Datum
cq_hypothetical_cume_dist_final(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	uint64 *values;
	uint64 rowcount;
	uint64 rank;
	uint64 peers;
	double result;

	if (PG_ARGISNULL(0))
		PG_RETURN_FLOAT8(0);

	state = (ArrayType *) PG_GETARG_ARRAYTYPE_P(0);
	values = (uint64 *) ARR_DATA_PTR(state);
	rowcount = values[0];
	rank = values[1] + 1;
	peers = values[2];

	if (rowcount == 0)
		PG_RETURN_FLOAT8(0);

	/* cume_dist counts peer values as increases in rank */
	rank += peers;
	result = (double) (rank) / (double) (rowcount + 1);

	PG_RETURN_FLOAT8(result);
}

/*
 * cq_os_serialize
 */
PG_FUNCTION_INFO_V1(cq_os_serialize);
Datum
cq_os_serialize(PG_FUNCTION_ARGS)
{
	CQOSAAggState *state = (CQOSAAggState *) PG_GETARG_POINTER(0);
	TDigest *t;
	bytea *result;
	int nbytes;
	char *pos;

	t = state->tdigest;
	t = TDigestCompress(t);

	nbytes = (sizeof(CQOSAAggState) + sizeof(float8) * state->num_percentiles + sizeof(bool) * state->num_percentiles +
			TDigestSize(t));

	result = (bytea *) palloc0(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pos = VARDATA(result);
	memcpy(pos, state, sizeof(CQOSAAggState));
	pos += sizeof(CQOSAAggState);

	memcpy(pos, state->percentiles, sizeof(float8) * state->num_percentiles);
	pos += sizeof(float8) * state->num_percentiles;
	memcpy(pos, state->nulls, sizeof(bool) * state->num_percentiles);
	pos += sizeof(bool) * state->num_percentiles;

	memcpy(pos, t, TDigestSize(t));

	PG_RETURN_BYTEA_P(result);
}

/*
 * cq_os_deserialize
 */
PG_FUNCTION_INFO_V1(cq_os_deserialize);
Datum
cq_os_deserialize(PG_FUNCTION_ARGS)
{
	bytea *bytes = (bytea *) PG_GETARG_BYTEA_P(0);
	char *pos;
	CQOSAAggState *state;
	TDigest *t;

	pos = VARDATA(bytes);
	state = palloc(sizeof(CQOSAAggState));

	memcpy(state, pos, sizeof(CQOSAAggState));
	pos += sizeof(CQOSAAggState);

	state->percentiles = palloc(sizeof(float8) * state->num_percentiles);
	memcpy(state->percentiles, pos, sizeof(float8) * state->num_percentiles);
	pos += sizeof(float8) * state->num_percentiles;
	state->nulls = palloc(sizeof(bool) * state->num_percentiles);
	memcpy(state->nulls, pos, sizeof(bool) * state->num_percentiles);
	pos += sizeof(bool) * state->num_percentiles;

	t = (TDigest *) pos;
	state->tdigest = palloc(TDigestSize(t));
	memcpy(state->tdigest, t, TDigestSize(t));

	PG_RETURN_POINTER(state);
}
