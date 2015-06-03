/*-------------------------------------------------------------------------
 *
 * hllfuncs.c
 *		HyperLogLog-based functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/hllfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/hll.h"
#include "utils/datum.h"
#include "utils/hllfuncs.h"
#include "utils/typcache.h"

Datum
hll_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			errmsg("user-specified hyperloglogs are not supported")));
	PG_RETURN_NULL();
}

Datum
hll_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	HyperLogLog *hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ p = %d, cardinality = %ld, size = %dkB }", hll->p, HLLCardinality(hll), hll->mlen / 1024);

	PG_RETURN_CSTRING(buf.data);
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

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p > 0)
		hll = hll_create(p);
	else
		hll = HLLCreate();

	SET_VARSIZE(hll, HLLSize(hll));

	return hll;
}

static HyperLogLog *
hll_add_datum(FunctionCallInfo fcinfo, HyperLogLog *hll, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size;
	int result;

	if (!typ->typbyval && !elem)
		return hll;

	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		hll = HLLAdd(hll, (char *) &elem, size, &result);
	else
		hll = HLLAdd(hll, DatumGetPointer(elem), size, &result);

	SET_VARSIZE(hll, HLLSize(hll));

	return hll;
}

/*
 * hll_agg transition function -
 * 	adds the given element to the transition HLL
 */
Datum
hll_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = hll_startup(fcinfo, 0);
	else
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	state = hll_add_datum(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * hll_agg transition function -
 *
 * 	adds the given element to the transition HLL using the given value for p
 */
Datum
hll_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;
	Datum incoming = PG_GETARG_DATUM(1);
	int p = PG_GETARG_INT32(2);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = hll_startup(fcinfo, p);
	else
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	state = hll_add_datum(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * hll_union_agg transition function -
 *
 * 	returns the union of the transition state and the given HLL
 */
Datum
hll_union_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	HyperLogLog *state;
	HyperLogLog *incoming = (HyperLogLog *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "hll_union_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = hll_startup(fcinfo, incoming->p);
	else
		state = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	state = HLLUnion(state, incoming);

	MemoryContextSwitchTo(old);

	SET_VARSIZE(state, HLLSize(state));

	PG_RETURN_POINTER(state);
}

/*
 * Returns the cardinality of the given HLL
 */
Datum
hll_cardinality(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(HLLCardinality(hll));
}

Datum
hll_empty(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll = HLLCreate();
	SET_VARSIZE(hll, HLLSize(hll));
	PG_RETURN_POINTER(hll);
}


Datum
hll_emptyp(PG_FUNCTION_ARGS)
{
	int p = PG_GETARG_INT32(0);
	HyperLogLog *hll = hll_create(p);
	SET_VARSIZE(hll, HLLSize(hll));
	PG_RETURN_POINTER(hll);
}


Datum
hll_add(PG_FUNCTION_ARGS)
{
	HyperLogLog *hll = (HyperLogLog *) PG_GETARG_VARLENA_P(0);

	/* Sparse representation can be repalloc'd so create a copy */
	if (HLL_IS_SPARSE(hll))
	{
		HyperLogLog *cpy = palloc(HLLSize(hll));
		memcpy(cpy, hll, HLLSize(hll));
		hll = cpy;
	}

	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	hll = hll_add_datum(fcinfo, hll, PG_GETARG_DATUM(1));
	PG_RETURN_POINTER(hll);
}
