/*-------------------------------------------------------------------------
 *
 * bloomfuncs.c
 *		Bloom Filter functions
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/bloomfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "pipeline/bloom.h"
#include "utils/datum.h"
#include "utils/bloomfuncs.h"
#include "utils/typcache.h"

Datum
bloom_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
			errmsg("user-specified bloom filters are not supported")));
	PG_RETURN_NULL();
}

Datum
bloom_out(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	BloomFilter *bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ k = %d, m = %d, fill = %f, card = %ld, size = %ldkB }", bloom->k, bloom->m, BloomFilterFillRatio(bloom), BloomFilterCardinality(bloom), BloomFilterSize(bloom) / 1024);

	PG_RETURN_CSTRING(buf.data);
}

static BloomFilter *
bloom_create(float8 p, uint64_t n)
{
	if (p <= 0 || p >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("p must be in [0, 1]")));
	if (n < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("n must be non-zero")));

	return BloomFilterCreateWithPAndN(p, n);
}

static BloomFilter *
bloom_startup(FunctionCallInfo fcinfo, float8 p, uint64_t n)
{
	BloomFilter *bloom;
	Oid type = AggGetInitialArgType(fcinfo);

	fcinfo->flinfo->fn_extra = lookup_type_cache(type, 0);

	if (p && n)
	{
		bloom = bloom_create(p, n);
	}
	else
		bloom = BloomFilterCreate();

	SET_VARSIZE(bloom, BloomFilterSize(bloom));

	return bloom;
}

static BloomFilter *
bloom_add_datum(FunctionCallInfo fcinfo, BloomFilter *bloom, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	Size size;

	if (!typ->typbyval && !elem)
		return bloom;

	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		BloomFilterAdd(bloom, (char *) &elem, size);
	else
		BloomFilterAdd(bloom, DatumGetPointer(elem), size);

	return bloom;
}

/*
 * bloom_agg transition function -
 * 	adds the given element to the transition Bloom filter
 */
Datum
bloom_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	Datum incoming = PG_GETARG_DATUM(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = bloom_startup(fcinfo, 0, 0);
	else
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	state = bloom_add_datum(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_agg transition function -
 *
 * 	adds the given element to the transition Bloom Filter using the given value for p and n
 */
Datum
bloom_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	Datum incoming = PG_GETARG_DATUM(1);
	float8 p = PG_GETARG_FLOAT8(2);
	uint64_t n = PG_GETARG_INT64(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = bloom_startup(fcinfo, p, n);
	else
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	state = bloom_add_datum(fcinfo, state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_union_agg transition function -
 *
 * 	returns the union of the transition state and the given Bloom filter
 */
Datum
bloom_union_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	BloomFilter *incoming = (BloomFilter *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_union_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = BloomFilterCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	state = BloomFilterUnion(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_intersection_agg transition function -
 *
 * 	returns the intersection of the transition state and the given Bloom filter
 */
Datum
bloom_intersection_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	BloomFilter *incoming = (BloomFilter *) PG_GETARG_VARLENA_P(1);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_union_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		state = BloomFilterCopy(incoming);
		PG_RETURN_POINTER(state);
	}

	state = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	state = BloomFilterIntersection(state, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * Returns the cardinality of the given Bloom filter
 */
Datum
bloom_cardinality(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	PG_RETURN_INT64(BloomFilterCardinality(bloom));
}

/*
 * Returns whether the Bloom filter contains the item or not
 */
Datum
bloom_contains(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom;
	Datum elem = PG_GETARG_DATUM(1);
	bool contains = false;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	Size size;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(contains);

	bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);
	size = datumGetSize(elem, typ->typbyval, typ->typlen);

	if (typ->typbyval)
		contains = BloomFilterContains(bloom, (char *) &elem, size);
	else
		contains = BloomFilterContains(bloom, DatumGetPointer(elem), size);

	PG_RETURN_BOOL(contains);
}

Datum
bloom_empty(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom = BloomFilterCreate();
	SET_VARSIZE(bloom, BloomFilterSize(bloom));
	PG_RETURN_POINTER(bloom);
}

Datum
bloom_emptyp(PG_FUNCTION_ARGS)
{
	float8 p = PG_GETARG_FLOAT8(0);
	uint64_t n = PG_GETARG_INT64(1);
	BloomFilter *bloom = bloom_create(p, n);
	SET_VARSIZE(bloom, BloomFilterSize(bloom));
	PG_RETURN_POINTER(bloom);
}

Datum
bloom_add(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	bloom = bloom_add_datum(fcinfo, bloom, PG_GETARG_DATUM(1));
	PG_RETURN_POINTER(bloom);
}
