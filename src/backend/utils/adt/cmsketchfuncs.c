/*-------------------------------------------------------------------------
 *
 * cmsketchfuncs.c
 *		Count-Min Sketch Filter functions
 *
 * Copyright (c) 2013-2015, PipelineDB
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
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			errmsg("user-specified count-min sketches are not supported")));
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
cmsketch_create(float8 eps, float8 p)
{
	if (p <= 0 || p >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("p must be in [0, 1]")));
	if (eps <= 0 || eps >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("eps must be in [0, 1]")));

	return CountMinSketchCreateWithEpsAndP(eps, p);
}

static CountMinSketch *
cmsketch_startup(FunctionCallInfo fcinfo, float8 eps, float8 p)
{
	CountMinSketch *cms;
	Oid type = AggGetInitialArgType(fcinfo);

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p && eps)
		cms = cmsketch_create(eps, p);
	else
		cms = CountMinSketchCreate();

	SET_VARSIZE(cms, CountMinSketchSize(cms));

	return cms;
}

static CountMinSketch *
cmsketch_add_datum(FunctionCallInfo fcinfo, CountMinSketch *cms, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size;

	if (!typ->typbyval && !elem)
		return cms;

	make_datum_hashable(elem, typ);
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

	state = cmsketch_add_datum(fcinfo, state, incoming);

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

	state = cmsketch_add_datum(fcinfo, state, incoming);

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
	make_datum_hashable(elem, typ);
	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		count = CountMinSketchEstimateCount(cms, (char *) &elem, size);
	else
		count = CountMinSketchEstimateCount(cms, DatumGetPointer(elem), size);

	PG_RETURN_INT32(count);
}

Datum
cmsketch_empty(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = CountMinSketchCreate();
	SET_VARSIZE(cms, CountMinSketchSize(cms));
	PG_RETURN_POINTER(cms);
}

Datum
cmsketch_emptyp(PG_FUNCTION_ARGS)
{
	float8 eps = PG_GETARG_FLOAT8(0);
	float8 p = PG_GETARG_FLOAT8(1);
	CountMinSketch *cms = cmsketch_create(eps, p);
	SET_VARSIZE(cms, CountMinSketchSize(cms));
	PG_RETURN_POINTER(cms);
}

Datum
cmsketch_add(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	cms = cmsketch_add_datum(fcinfo, cms, PG_GETARG_DATUM(1));
	PG_RETURN_POINTER(cms);
}
