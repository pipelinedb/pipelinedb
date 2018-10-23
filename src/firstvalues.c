/*-------------------------------------------------------------------------
 *
 * firstvalues.c
 *		first_values aggregate and supporting functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "aggfuncs.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parse_clause.h"
#include "parser/parse_oper.h"
#include "optimizer/tlist.h"
#include "utils/array.h"
#include "utils/fmgrprotos.h"
#include "utils/sortsupport.h"

typedef struct FirstValuesQueryState
{
	int num_sort;
	int num_values;
	int offset;
	SortSupport sortkey;
	Oid *sortop;
	Oid	type;
	/* These fields are used only when accumulating tuples */
	TupleDesc tup_desc;
	TupleTableSlot *tup_slot1;
	TupleTableSlot *tup_slot2;
} FirstValuesQueryState;

typedef struct FirstValuesPerGroupState
{
	int n;
	int num_sort;
	Oid *types;
	ArrayBuildState *array;
} FirstValuesPerGroupState;

/*
 * first_values_serialize
 */
PG_FUNCTION_INFO_V1(first_values_serialize);
Datum
first_values_serialize(PG_FUNCTION_ARGS)
{
	FirstValuesPerGroupState *state = (FirstValuesPerGroupState *) PG_GETARG_POINTER(0);
	MemoryContext old;
	MemoryContext context;
	bytea *result;
	int nbytes;
	char *pos;
	bytea *array = NULL;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	if (state->array)
		array = DatumGetByteaP(DirectFunctionCall1(array_agg_serialize, PointerGetDatum(state->array)));

	nbytes = sizeof(FirstValuesPerGroupState) + (sizeof(Oid) * state->num_sort);
	if (array)
		nbytes += VARSIZE(array);

	result = (bytea *) palloc0(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pos = VARDATA(result);
	memcpy(pos, state, sizeof(FirstValuesPerGroupState));
	pos += sizeof(FirstValuesPerGroupState);

	memcpy(pos, state->types, sizeof(Oid) * state->num_sort);
	pos += sizeof(Oid) * state->num_sort;

	if (array)
		memcpy(pos, array, VARSIZE(array));

	MemoryContextSwitchTo(old);

	PG_RETURN_BYTEA_P(result);
}

PG_FUNCTION_INFO_V1(first_values_deserialize);
Datum
first_values_deserialize(PG_FUNCTION_ARGS)
{
	bytea *bytes;
	char *pos;
	FirstValuesPerGroupState *state;
	MemoryContext old;
	MemoryContext context;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	bytes = (bytea *) PG_GETARG_BYTEA_P(0);
	pos = VARDATA(bytes);

	state = (FirstValuesPerGroupState *) palloc(sizeof(FirstValuesPerGroupState));
	memcpy(state, pos, sizeof(FirstValuesPerGroupState));
	pos += sizeof(FirstValuesPerGroupState);

	state->types = palloc(sizeof(Oid) * state->num_sort);
	memcpy(state->types, pos, sizeof(Oid) * state->num_sort);
	pos += sizeof(Oid) * state->num_sort;

	if (state->array != NULL)
	{
		fcinfo->arg[0] = PointerGetDatum(pos);
		state->array = (ArrayBuildState *) DatumGetPointer(array_agg_deserialize(fcinfo));
		fcinfo->arg[0] = PointerGetDatum(bytes);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * first_values_final
 */
PG_FUNCTION_INFO_V1(first_values_final);
Datum
first_values_final(PG_FUNCTION_ARGS)
{
	FirstValuesPerGroupState *fvstate;
	FirstValuesQueryState *qstate;
	ArrayBuildState *array;
	Datum result;
	int dims[1];
	int lbs[1];

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	fvstate = (FirstValuesPerGroupState *) PG_GETARG_POINTER(0);
	array = fvstate->array;

	if (array == NULL)
		PG_RETURN_NULL();

	qstate = (FirstValuesQueryState *) fcinfo->flinfo->fn_extra;
	if (array->nelems && array->element_type == RECORDOID)
	{
		MemoryContext old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		int i;

		if (qstate == NULL)
		{
			qstate = palloc0(sizeof(FirstValuesQueryState));
			qstate->tup_desc = CreateTemplateTupleDesc(fvstate->num_sort, false);

			for (i = 0; i < fvstate->num_sort; i++)
			{
				TupleDescInitEntry(qstate->tup_desc, i + 1, NULL, fvstate->types[i], -1, 0);
			}

			BlessTupleDesc(qstate->tup_desc);
			fcinfo->flinfo->fn_extra = (void *) qstate;
		}

		/*
		 * The typmod assigned to each tuple will eventually be used to look up
		 * the TupleDesc we just created
		 */
		for (i = 0; i < array->nelems; i++)
		{
			HeapTupleHeader td = DatumGetHeapTupleHeader(array->dvalues[i]);
			HeapTupleHeaderSetTypMod(td, qstate->tup_desc->tdtypmod);
		}

		MemoryContextSwitchTo(old);
	}

	if (fvstate->array == NULL)
		PG_RETURN_NULL();

	dims[0] = fvstate->array->nelems;
	lbs[0] = 1;

	result = makeMdArrayResult(fvstate->array, 1, dims, lbs,
							   CurrentMemoryContext,
							   false);

	PG_RETURN_DATUM(result);
}

/*
 * init_first_values_query_state
 */
static FirstValuesQueryState *
init_first_values_query_state(PG_FUNCTION_ARGS)
{
	FirstValuesQueryState *qstate;
	Aggref *aggref;
	bool isnull;
	ExprContext *econtext;
	ExprState *expr;
	MemoryContext old;
	ListCell *lc;
	List *directexprs;
	List *directargs;
	List *sortlist;
	int i;

	int num_values;
	int num_sort;
	Oid *sort_types;

	/* Get the Aggref so we can examine aggregate's arguments */
	aggref = AggGetAggref(fcinfo);
	if (!aggref)
		elog(ERROR, "first_values aggregate called in non-aggregate context");

	/* Create in long term per query context */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	qstate = (FirstValuesQueryState *) palloc0(sizeof(FirstValuesQueryState));

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

	/*
	 * first_values only supports a single initial constant
	 */
	if (!directargs)
		elog(ERROR, "expected constant expressions at the beginning of the argument list");
	if (list_length(directargs) > 1)
		elog(ERROR, "first_values takes a single constant as its first argument");

	sortlist = NIL;

	/* Our sort columns are everything following the initial n constant */
	num_sort = list_length(aggref->args) - 1;

	for (i = 1; i < list_length(aggref->args); i++)
	{
		TargetEntry *te = (TargetEntry *) list_nth(aggref->args, i);
		SortGroupClause *sortcl = makeNode(SortGroupClause);

		if (IsA(te->expr, Const))
			elog(ERROR, "constant in argument list where not expected");

		sortcl->tleSortGroupRef = assignSortGroupRef(te, aggref->args);
		get_sort_group_operators(exprType((Node *) te->expr),
				true, true, false, &sortcl->sortop, &sortcl->eqop, NULL, &sortcl->hashable);

		sortlist = lappend(sortlist, sortcl);
	}

	econtext = CreateStandaloneExprContext();
	expr = (ExprState *) linitial(directexprs);
	num_values = DatumGetInt32(ExecEvalExpr(expr, econtext, &isnull));

	qstate->num_sort = num_sort;
	qstate->num_values = num_values;
	qstate->offset = 1;

	sort_types = palloc0(sizeof(Oid) * num_sort);

	Assert(!isnull);

	if (num_sort > 1)
	{
		ListCell *lc;
		int i;

		qstate->type = RECORDOID;
		qstate->tup_desc = CreateTemplateTupleDesc(num_sort, false);
		qstate->sortkey = (SortSupport) palloc0(num_sort * sizeof(SortSupportData));
		qstate->sortop = palloc0(sizeof(Oid) * num_sort);

		i = 0;
		foreach(lc, sortlist)
		{
			SortGroupClause *sortcl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);
			SortSupport sortkey = &qstate->sortkey[i];
			Oid type = exprType((Node *) tle->expr);

			/* the parser should have made sure of this */
			Assert(OidIsValid(sortcl->sortop));

			TupleDescInitEntry(qstate->tup_desc, i + 1, NULL, type, -1, 0);

			sortkey->ssup_cxt = CurrentMemoryContext;
			/*
			 * TODO(derekjn) Is it worth supporting collations and nulls_first? We'd have to store these
			 * within the serialized transition state
			 */
			sortkey->ssup_collation = DEFAULT_COLLATION_OID;
			sortkey->ssup_nulls_first = false;
			sortkey->ssup_attno = tle->resno;
			PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);

			qstate->sortop[i] = sortcl->sortop;
			sort_types[i] = type;
			i++;
		}

		BlessTupleDesc(qstate->tup_desc);

		qstate->tup_slot1 = MakeSingleTupleTableSlot(qstate->tup_desc);
		qstate->tup_slot2 = MakeSingleTupleTableSlot(qstate->tup_desc);
	}
	else
	{
		SortGroupClause *sortcl = (SortGroupClause *) linitial(sortlist);
		TargetEntry *tle = get_sortgroupclause_tle(sortcl, aggref->args);
		SortSupport sortkey;

		/* the parser should have made sure of this */
		Assert(OidIsValid(sortcl->sortop));

		/* Save sort ordering info */
		sortkey = palloc0(sizeof(SortSupportData));
		sortkey->ssup_cxt = CurrentMemoryContext;
		/*
		 * TODO(derekjn) Is it worth supporting collations and nulls_first? We'd have to store these
		 * within the serialized transition state
		 */
		sortkey->ssup_collation = DEFAULT_COLLATION_OID;
		sortkey->ssup_nulls_first = false;
		sortkey->ssup_attno = tle->resno;

		PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);
		qstate->sortkey = sortkey;
		qstate->sortop = palloc0(sizeof(Oid));
		*qstate->sortop = sortcl->sortop;

		/* Save datatype info */
		qstate->type = exprType((Node *) tle->expr);
	}

	MemoryContextSwitchTo(old);

	return qstate;
}

/*
 * extract_types
 */
static Oid *
extract_types(FirstValuesQueryState *qstate)
{
	int i;
	int n = qstate->tup_desc ? qstate->tup_desc->natts : 1;
	Oid *result = palloc0(sizeof(Oid) * n);

	if (n == 1)
	{
		result[0] = qstate->type;
		return result;
	}

	Assert(qstate->tup_desc);

	for (i=0; i<qstate->tup_desc->natts; i++)
	{
		result[i] = TupleDescAttr(qstate->tup_desc, i)->atttypid;
	}

	return result;
}

/*
 * first_values_startup
 */
static FirstValuesPerGroupState *
first_values_startup(PG_FUNCTION_ARGS)
{
	FirstValuesQueryState *qstate;
	Aggref *aggref;
	MemoryContext old;
	FirstValuesPerGroupState *fvstate;

	fvstate = (FirstValuesPerGroupState *) palloc0(sizeof(FirstValuesPerGroupState));

	/* Get the Aggref so we can examine aggregate's arguments */
	aggref = AggGetAggref(fcinfo);
	if (!aggref)
		elog(ERROR, "first_values aggregate called in non-aggregate context");

	/* Create in long term per query context */
	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	/* Is per-query state already initialized? */
	if (fcinfo->flinfo->fn_extra)
	{
		qstate = (FirstValuesQueryState *) fcinfo->flinfo->fn_extra;
		fvstate->types = extract_types(qstate);
		fvstate->num_sort = qstate->num_sort;
		fvstate->n = qstate->num_values;

		MemoryContextSwitchTo(old);
		return fvstate;
	}

	qstate = init_first_values_query_state(fcinfo);
	fcinfo->flinfo->fn_extra = (void *) qstate;

	Assert(qstate->num_values);

	fvstate->types = extract_types(qstate);
	fvstate->num_sort = qstate->num_sort;
	fvstate->n = qstate->num_values;

	MemoryContextSwitchTo(old);

	return fvstate;
}

/*
 * compare_values
 */
static int
compare_values(FirstValuesQueryState *qstate, Datum d1, bool isnull1, Datum d2, bool isnull2)
{
	int i;
	HeapTuple tup1;
	HeapTuple tup2;
	int natts;

	if (qstate->num_sort == 1)
		return ApplySortComparator(d1, isnull1, d2, isnull2, qstate->sortkey);

	Assert(!isnull1 && !isnull2);

	natts = qstate->tup_desc->natts;

	tup1 = palloc0(sizeof(HeapTupleData));
	tup1->t_data = DatumGetHeapTupleHeader(d1);
	tup1->t_len = HeapTupleHeaderGetDatumLength(tup1->t_data);

	tup2 = palloc0(sizeof(HeapTupleData));
	tup2->t_data = DatumGetHeapTupleHeader(d2);
	tup2->t_len = HeapTupleHeaderGetDatumLength(tup1->t_data);

	ExecClearTuple(qstate->tup_slot1);
	ExecStoreTuple(tup1, qstate->tup_slot1, InvalidBuffer, false);
	ExecClearTuple(qstate->tup_slot2);
	ExecStoreTuple(tup2, qstate->tup_slot2, InvalidBuffer, false);

	for (i = 0; i < natts; i++)
	{
		bool n0;
		bool n1;
		Datum d0 = slot_getattr(qstate->tup_slot1, i + 1, &n0);
		Datum d1 = slot_getattr(qstate->tup_slot2, i + 1, &n1);
		SortSupport sortkey = qstate->sortkey + i;

		int result = ApplySortComparator(d0, n0, d1, n1, sortkey);

		if (result != 0)
			return result;
	}

	ExecClearTuple(qstate->tup_slot1);
	ExecClearTuple(qstate->tup_slot2);

	/* they must be equal */
	return 0;
}

/*
 * first_values_trans
 */
PG_FUNCTION_INFO_V1(first_values_trans);
Datum
first_values_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FirstValuesPerGroupState *fvstate = PG_ARGISNULL(0) ? NULL : (FirstValuesPerGroupState *) PG_GETARG_POINTER(0);
	FirstValuesQueryState *qstate;
	ArrayBuildState *array;
	Datum d;
	bool isnull;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (fvstate == NULL)
		fvstate = first_values_startup(fcinfo);

	Assert(fvstate->n);

	qstate = (FirstValuesQueryState *) fcinfo->flinfo->fn_extra;
	array = fvstate->array;

	if (qstate->num_sort > 1)
	{
		int i;
		int nargs = PG_NARGS() - 1 - qstate->offset;

		Assert(nargs == qstate->num_sort);

		ExecClearTuple(qstate->tup_slot1);
		for (i = 0; i < nargs; i++)
		{
			qstate->tup_slot1->tts_values[i] = PG_GETARG_DATUM(i + 1 + qstate->offset);
			qstate->tup_slot1->tts_isnull[i] = PG_ARGISNULL(i + 1 + qstate->offset);
		}
		ExecStoreVirtualTuple(qstate->tup_slot1);

		d = ExecFetchSlotTupleDatum(qstate->tup_slot1);
		isnull = false; /* We will never have a NULL tuple here */
	}
	else
	{
		d = PG_GETARG_DATUM(1 + qstate->offset);
		isnull = PG_ARGISNULL(1 + qstate->offset);
	}

	if (array == NULL)
		array = accumArrayResult(array, d, isnull, qstate->type, context);
	else
	{
		bool needs_sort = false;

		/* Insert in sorted order */
		if (array->nelems < qstate->num_values)
		{
			array = accumArrayResult(array, d, isnull, qstate->type, context);
			needs_sort = true;
		}
		else
		{
			/* Value should only be inserted if its smaller than the last element */
			if (compare_values(qstate, d, isnull, array->dvalues[array->nelems - 1], array->dnulls[array->nelems - 1]) < 0)
			{
				array->dvalues[array->nelems - 1] = d;
				array->dnulls[array->nelems - 1] = isnull;
				needs_sort = true;
			}
		}

		/* Do we need to fix the position of the last element in the list? */
		if (needs_sort)
		{
			int i;
			for (i = 0; i < array->nelems - 1; i++)
			{
				if (compare_values(qstate, d, isnull, array->dvalues[i], array->dnulls[i]) < 0)
				{
					memmove(&array->dvalues[i + 1], &array->dvalues[i], sizeof(Datum) * (array->nelems - 1 - i));
					memmove(&array->dnulls[i + 1], &array->dnulls[i], sizeof(bool) * (array->nelems - 1 - i));
					array->dvalues[i] = d;
					array->dnulls[i] = isnull;
					break;
				}
			}
		}
	}

	MemoryContextSwitchTo(old);

	Assert(fvstate);

	if (array == NULL)
		PG_RETURN_NULL();

	fvstate->array = array;

	PG_RETURN_POINTER(fvstate);
}

/*
 * init_first_values_query_state_for_combine
 *
 * We don't have the original Aggref and arguments during a combine call, so the per-query state
 * initialization is slightly different from init_first_values_query_state
 */
static FirstValuesQueryState *
init_first_values_query_state_for_combine(FunctionCallInfo fcinfo, FirstValuesPerGroupState *pg)
{
	MemoryContext old;
	FirstValuesQueryState *qstate;
	int i;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	qstate = palloc0(sizeof(FirstValuesQueryState));
	qstate->tup_desc = CreateTemplateTupleDesc(pg->num_sort, false);
	qstate->num_sort = pg->num_sort;
	qstate->num_values = pg->n;

	if (qstate->num_sort > 1)
	{
		qstate->type = RECORDOID;
		qstate->sortkey = (SortSupport) palloc0(pg->num_sort * sizeof(SortSupportData));
		qstate->sortop = palloc0(sizeof(Oid) * pg->num_sort);

		for (i = 0; i < pg->num_sort; i++)
		{
			SortSupport sortkey = &qstate->sortkey[i];
			Oid type = pg->types[i];
			SortGroupClause *sortcl = makeNode(SortGroupClause);

			TupleDescInitEntry(qstate->tup_desc, i + 1, NULL, type, -1, 0);

			sortkey->ssup_cxt = CurrentMemoryContext;
			sortkey->ssup_collation = DEFAULT_COLLATION_OID;
			sortkey->ssup_nulls_first = false;
			sortkey->ssup_attno = i + 1;

			get_sort_group_operators(type,
					true, true, false, &sortcl->sortop, &sortcl->eqop, NULL, &sortcl->hashable);

			PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);
			qstate->sortop[i] = sortcl->sortop;
		}

		BlessTupleDesc(qstate->tup_desc);

		qstate->tup_slot1 = MakeSingleTupleTableSlot(qstate->tup_desc);
		qstate->tup_slot2 = MakeSingleTupleTableSlot(qstate->tup_desc);
	}
	else
	{
		SortSupport sortkey = &qstate->sortkey[0];
		SortGroupClause *sortcl = makeNode(SortGroupClause);
		Oid type = pg->types[0];

		get_sort_group_operators(type,
				true, true, false, &sortcl->sortop, &sortcl->eqop, NULL, &sortcl->hashable);

		/* Save sort ordering info */
		sortkey = palloc0(sizeof(SortSupportData));
		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = DEFAULT_COLLATION_OID;
		sortkey->ssup_nulls_first = false;
		sortkey->ssup_attno = 1;

		PrepareSortSupportFromOrderingOp(sortcl->sortop, sortkey);
		qstate->sortkey = sortkey;
		qstate->sortop = palloc0(sizeof(Oid));
		*qstate->sortop = sortcl->sortop;

		/* Save datatype info */
		qstate->type = type;
	}

	MemoryContextSwitchTo(old);

	return qstate;
}

/*
 * first_values_combine
 */
PG_FUNCTION_INFO_V1(first_values_combine);
Datum
first_values_combine(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	FirstValuesPerGroupState *fvstate = PG_ARGISNULL(0) ? NULL : (FirstValuesPerGroupState *) PG_GETARG_POINTER(0);
	FirstValuesPerGroupState *incoming = PG_ARGISNULL(1) ? NULL : (FirstValuesPerGroupState *) PG_GETARG_POINTER(1);
	ArrayBuildState *astate;
	ArrayBuildState *incoming_astate;
	ArrayBuildState *merged = NULL;
	FirstValuesQueryState *qstate;
	int i;
	int j;

	if (!AggCheckCallContext(fcinfo, &context))
			elog(ERROR, "aggregate function called in non-aggregate context");

	Assert(incoming->num_sort > 0);

	if (fcinfo->flinfo->fn_extra == NULL)
		fcinfo->flinfo->fn_extra = init_first_values_query_state_for_combine(fcinfo, incoming);

	if (fvstate == NULL)
	{
		if (incoming == NULL)
			PG_RETURN_NULL();

		PG_RETURN_POINTER(incoming);
	}

	if (incoming == NULL)
		PG_RETURN_POINTER(fvstate);

	Assert(fvstate->array && incoming->array);

	qstate = (FirstValuesQueryState *) fcinfo->flinfo->fn_extra;
	old = MemoryContextSwitchTo(context);

	/* This is basically a merge routine */
	astate = fvstate->array;
	incoming_astate = incoming->array;

	for (i = 0, j = 0; i < astate->nelems || j < incoming_astate->nelems; )
	{
		if ((merged && merged->nelems == qstate->num_values) || (i == astate->nelems && j == incoming_astate->nelems))
			break;

		if (i == astate->nelems)
		{
			merged = accumArrayResult(merged, incoming_astate->dvalues[j], incoming_astate->dnulls[j], qstate->type, context);
			j++;
		}
		else if (j == incoming_astate->nelems)
		{
			merged = accumArrayResult(merged, astate->dvalues[i], astate->dnulls[i], qstate->type, context);
			i++;
		}
		else
		{
			Datum d1 = astate->dvalues[i];
			bool n1 = astate->dnulls[i];
			Datum d2 = incoming_astate->dvalues[j];
			bool n2 = incoming_astate->dnulls[j];

			if (compare_values(qstate, d1, n1, d2, n2) < 0)
			{
				merged = accumArrayResult(merged, d1, n1, qstate->type, context);
				i++;
			}
			else
			{
				merged = accumArrayResult(merged, d2, n2, qstate->type, context);
				j++;
			}
		}
	}

	MemoryContextSwitchTo(old);

	fvstate->array = merged;

	PG_RETURN_POINTER(fvstate);
}
