/*-------------------------------------------------------------------------
 *
 * freqfuncs.c
 *		Count-Min Sketch based functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "analyzer.h"
#include "cmsketch.h"
#include "freqfuncs.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscutils.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/typcache.h"

/*
 * cmsketch_print
 */
PG_FUNCTION_INFO_V1(cmsketch_print);
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

/*
 * cmsketch_create
 */
static CountMinSketch *
cmsketch_create(float8 eps, float8 p)
{
	if (p <= 0 || p >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("p must be in (0, 1)")));
	if (eps <= 0 || eps >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("eps must be in (0, 1)")));

	return CountMinSketchCreateWithEpsAndP(eps, p);
}

/*
 * cmsketch_startup
 */
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

/*
 * cmsketch_add_datum
 */
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
 * freq_agg transition function -
 * 	adds the given element to the transition cmsketch
 */
PG_FUNCTION_INFO_V1(freq_agg_trans);
Datum
freq_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "freq_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cmsketch_startup(fcinfo, 0, 0);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = cmsketch_add_datum(fcinfo, state, PG_GETARG_DATUM(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * freq_agg transition function -
 *
 * 	adds the given element to the transition cmsketch using the given value for p and n
 */
PG_FUNCTION_INFO_V1(freq_agg_transp);
Datum
freq_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	float8 eps = PG_GETARG_FLOAT8(2);
	float8 p = PG_GETARG_FLOAT8(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cmsketch_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = cmsketch_startup(fcinfo, eps, p);
	else
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = cmsketch_add_datum(fcinfo, state, PG_GETARG_DATUM(1), 1);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * cmsketch combine function -
 *
 * 	returns the merge of the transition state and the given cmsketch
 */
PG_FUNCTION_INFO_V1(cmsketch_combine);
Datum
cmsketch_combine(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	CountMinSketch *state;
	CountMinSketch *incoming;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "cmsketch_combine called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		incoming = (CountMinSketch *) PG_GETARG_VARLENA_P(1);
		state = CountMinSketchCopy(incoming);
	}
	else if (PG_ARGISNULL(1))
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	else
	{
		state = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
		incoming = (CountMinSketch *) PG_GETARG_VARLENA_P(1);
		state = CountMinSketchMerge(state, incoming);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * Returns the estimate frequency of the item
 */
PG_FUNCTION_INFO_V1(cmsketch_frequency);
Datum
cmsketch_frequency(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms;
	Datum elem;
	uint32_t count = 0;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	StringInfo buf;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_INT32(count);

	elem = PG_GETARG_DATUM(1);

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
PG_FUNCTION_INFO_V1(cmsketch_total);
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
PG_FUNCTION_INFO_V1(cmsketch_norm_frequency);
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

/*
 * cmsketch_empty
 */
PG_FUNCTION_INFO_V1(cmsketch_empty);
Datum
cmsketch_empty(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = CountMinSketchCreate();
	PG_RETURN_POINTER(cms);
}

/*
 * cmsketch_emptyp
 */
PG_FUNCTION_INFO_V1(cmsketch_emptyp);
Datum
cmsketch_emptyp(PG_FUNCTION_ARGS)
{
	float8 eps = PG_GETARG_FLOAT8(0);
	float8 p = PG_GETARG_FLOAT8(1);
	CountMinSketch *cms = cmsketch_create(eps, p);
	PG_RETURN_POINTER(cms);
}

/*
 * cmsketch_add
 */
PG_FUNCTION_INFO_V1(cmsketch_add);
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

/*
 * cmsketch_addn
 */
PG_FUNCTION_INFO_V1(cmsketch_addn);
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

/*
 * cmsketch_in
 */
PG_FUNCTION_INFO_V1(cmsketch_in);
Datum
cmsketch_in(PG_FUNCTION_ARGS)
{
	char *raw = PG_GETARG_CSTRING(0);
	Datum result = DirectFunctionCall1(byteain, (Datum) raw);

	PG_RETURN_POINTER(result);
}

/*
 * cmsketch_out
 */
PG_FUNCTION_INFO_V1(cmsketch_out);
Datum
cmsketch_out(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	char *s = (char *) DirectFunctionCall1(byteaout, (Datum) cms);

	PG_RETURN_CSTRING(s);
}

/*
 * cmsketch_send
 */
PG_FUNCTION_INFO_V1(cmsketch_send);
Datum
cmsketch_send(PG_FUNCTION_ARGS)
{
	CountMinSketch *cms = (CountMinSketch *) PG_GETARG_VARLENA_P(0);
	bytea *result = (bytea *) DirectFunctionCall1(byteasend, (Datum) cms);

	PG_RETURN_BYTEA_P(result);
}

/*
 * cmsketch_recv
 */
PG_FUNCTION_INFO_V1(cmsketch_recv);
Datum
cmsketch_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea *result = (bytea *) DirectFunctionCall1(bytearecv, (Datum) buf);

	PG_RETURN_BYTEA_P(result);
}
