/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cmsketchfuncs.c
 *		Count-Min Sketch Filter functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cmsketchfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/cmsketch.h"
#include "utils/datum.h"
#include "utils/cmsketchfuncs.h"
#include "utils/typcache.h"

Datum
cmsketch_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "user-specified count-min sketches are not supported");
	PG_RETURN_NULL();
}

Datum
cmsketch_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	CountMinSketch *cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ d = %d, w = %d, count = %d, size = %ldkB }", cms->d, cms->w, cms->count, CountMinSketchSize(cms) / 1024);

	PG_RETURN_CSTRING(buf.data);
}

static CountMinSketch *
cmsketch_startup(FunctionCallInfo fcinfo, float8 eps, float8 p)
{
	Aggref *aggref = AggGetAggref(fcinfo);
	TargetEntry *te = (TargetEntry *) linitial(aggref->args);
	CountMinSketch *cms;
	Oid type = exprType((Node *) te->expr);

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p && eps)
		cms = CountMinSketchCreateWithEpsAndP(eps, p);
	else
		cms = CountMinSketchCreate();

	SET_VARSIZE(cms, CountMinSketchSize(cms));

	return cms;
}

static CountMinSketch *
cmsketch_add(FunctionCallInfo fcinfo, CountMinSketch *cms, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size;

	if (!typ->typbyval && !elem)
		return cms;

	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		CountMinSketchAdd(cms, (char *) &elem, size, 1);
	else
		CountMinSketchAdd(cms, DatumGetPointer(elem), size, 1);

	return cms;
}

/*
 * cmsketch_agg transition function -
 * 	adds the given element to the transition cmsketch
 */
Datum
cmsketch_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cmsketch_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cmsketch_startup(fcinfo, 0, 0);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	state = cmsketch_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * cmsketch_agg transition function -
 *
 * 	adds the given element to the transition cmsketch using the given value for p and n
 */
Datum
cmsketch_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	Datum incoming = PG_GETARG_DATUM(1);
	float8 eps = PG_GETARG_FLOAT8(2);
	float8 p = PG_GETARG_FLOAT8(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cmsketch_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cmsketch_startup(fcinfo, eps, p);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	state = cmsketch_add(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * cmsketch_merge_agg transition function -
 *
 * 	returns the merge of the transition state and the given cmsketch
 */
Datum
cmsketch_merge_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	CountMinSketch *incoming = (CountMinSketch *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cmsketch_merge_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = CountMinSketchCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	state = CountMinSketchMerge(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * Returns the estimate count of the item
 */
Datum
cmsketch_count(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	Datum elem = PG_GETARG_DATUM(1);
	uint32_t count = false;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	Size size;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(count);

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);
	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		count = CountMinSketchEstimateCount(cms, (char *) &elem, size);
	else
		count = CountMinSketchEstimateCount(cms, DatumGetPointer(elem), size);

	PG_RETURN_INT32(count);
}
