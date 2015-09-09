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
#include "pipeline/miscutils.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/cmsketchfuncs.h"
#include "utils/typcache.h"

Datum
cmsketch_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	CountMinSketch *cms;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ d = %d, w = %d, count = %ld, size = %ldkB }", cms->d, cms->w, cms->count, CountMinSketchSize(cms) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
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

	return cms;
}

static CountMinSketch *
cmsketch_add_datum(FunctionCallInfo fcinfo, CountMinSketch *cms, Datum elem, int32 n)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	StringInfo buf;

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);
	CountMinSketchAdd(cms, buf->data, buf->len, n);

	pfree(buf->data);
	pfree(buf);

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

	state = cmsketch_add_datum(fcinfo, state, incoming, 1);

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

	state = cmsketch_add_datum(fcinfo, state, incoming, 1);

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
 * Returns the estimate frequency of the item
 */
Datum
cmsketch_frequency(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	Datum elem = PG_GETARG_DATUM(1);
	uint32_t count = 0;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	StringInfo buf;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_INT32(count);

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);

	count = CountMinSketchEstimateFrequency(cms, buf->data, buf->len);

	pfree(buf->data);
	pfree(buf);

	PG_RETURN_INT32(count);
}

/*
 * Returns the estimate frequency of the item
 */
Datum
cmsketch_total(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(cms->count);
}

/*
 * Returns the estimate normalized frequency of the item
 */
Datum
cmsketch_norm_frequency(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	Datum elem = PG_GETARG_DATUM(1);
	float8 freq = 0;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	StringInfo buf;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_FLOAT8(freq);

	cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);

	freq = CountMinSketchEstimateNormFrequency(cms, buf->data, buf->len);

	pfree(buf->data);
	pfree(buf);

	PG_RETURN_FLOAT8(freq);
}

Datum
cmsketch_empty(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = CountMinSketchCreate();
	PG_RETURN_POINTER(cms);
}

Datum
cmsketch_emptyp(PG_FUNCTION_ARGS)
{
	float8 eps = PG_GETARG_FLOAT8(0);
	float8 p = PG_GETARG_FLOAT8(1);
	CountMinSketch *cms = cmsketch_create(eps, p);
	PG_RETURN_POINTER(cms);
}

Datum
cmsketch_add(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;

	if (PG_ARGISNULL(0))
		cms = CountMinSketchCreate();
	else
		cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	cms = cmsketch_add_datum(fcinfo, cms, PG_GETARG_DATUM(1), 1);
	PG_RETURN_POINTER(cms);
}

Datum
cmsketch_addn(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	int32 n = PG_GETARG_INT32(2);

	if (PG_ARGISNULL(0))
		cms = CountMinSketchCreate();
	else
		cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	if (n < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("cmsketch type doesn't support negative increments")));

	if (n)
	{
		fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
		cms = cmsketch_add_datum(fcinfo, cms, PG_GETARG_DATUM(1), n);
	}

	PG_RETURN_POINTER(cms);
}
