/*-------------------------------------------------------------------------
 *
 * bloomfuncs.c
 *		Bloom Filter functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "analyzer.h"
#include "bloom.h"
#include "bloomfuncs.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscutils.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_type.h"
#include "parser/parser.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/typcache.h"

/*
 * bloom_print
 */
PG_FUNCTION_INFO_V1(bloom_print);
Datum
bloom_print(PG_FUNCTION_ARGS)
{
	StringInfoData buf;
	BloomFilter *bloom;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	initStringInfo(&buf);
	appendStringInfo(&buf, "{ k = %d, m = %d, fill = %f, card = %ld, size = %ldkB }", bloom->k, bloom->m, BloomFilterFillRatio(bloom), BloomFilterCardinality(bloom), BloomFilterSize(bloom) / 1024);

	PG_RETURN_TEXT_P(CStringGetTextDatum(buf.data));
}

/*
 * bloom_create
 */
static BloomFilter *
bloom_create(float8 p, int64 n)
{
	if (p <= 0 || p >= 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("p must be in (0, 1)")));
	if (n < 1)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("n must be greater than or equal to 1")));

	return BloomFilterCreateWithPAndN(p, n);
}

/*
 * bloom_startup
 */
static BloomFilter *
bloom_startup(FunctionCallInfo fcinfo, float8 p, int64 n)
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

	return bloom;
}

/*
 * bloom_add_datum
 */
static BloomFilter *
bloom_add_datum(FunctionCallInfo fcinfo, BloomFilter *bloom, Datum elem)
{
	TypeCacheEntry *typ = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;
	StringInfo buf;

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);
	BloomFilterAdd(bloom, buf->data, buf->len);

	pfree(buf->data);
	pfree(buf);

	return bloom;
}

/*
 * bloom_agg transition function -
 * 	adds the given element to the transition Bloom filter
 */
PG_FUNCTION_INFO_V1(bloom_agg_trans);
Datum
bloom_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = bloom_startup(fcinfo, 0, 0);
	else
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = bloom_add_datum(fcinfo, state, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_agg transition function -
 *
 * 	adds the given element to the transition Bloom Filter using the given value for p and n
 */
PG_FUNCTION_INFO_V1(bloom_agg_transp);
Datum
bloom_agg_transp(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	float8 p = PG_GETARG_FLOAT8(2);
	int64 n = PG_GETARG_INT64(3);

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_agg_transp called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
		state = bloom_startup(fcinfo, p, n);
	else
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	if (!PG_ARGISNULL(1))
		state = bloom_add_datum(fcinfo, state, PG_GETARG_DATUM(1));

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_union_agg transition function -
 *
 * 	returns the union of the transition state and the given Bloom filter
 */
PG_FUNCTION_INFO_V1(bloom_union_agg_trans);
Datum
bloom_union_agg_trans(PG_FUNCTION_ARGS)
{
	MemoryContext old;
	MemoryContext context;
	BloomFilter *state;
	BloomFilter *incoming;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bloom_union_agg_trans called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		incoming = (BloomFilter *) PG_GETARG_VARLENA_P(1);
		state = BloomFilterCopy(incoming);
	}
	else if (PG_ARGISNULL(1))
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	else
	{
		state = (BloomFilter *) PG_GETARG_VARLENA_P(0);
		incoming = (BloomFilter *) PG_GETARG_VARLENA_P(1);
		state = BloomFilterUnion(state, incoming);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bloom_operation
 */
static BloomFilter *
bloom_operation(FunctionCallInfo fcinfo, bool intersection)
{
	Oid bf_type = GetTypeOid(BLOOM_TYPENAME);
	BloomFilter *result = NULL;
	int i;

	for (i = 0; i < PG_NARGS(); i++)
	{
		BloomFilter *bf;
		if (PG_ARGISNULL(i))
			continue;

		if (get_fn_expr_argtype(fcinfo->flinfo, i) != bf_type)
			elog(ERROR, "argument %d is not of type \"%s\"", i + 1, BLOOM_TYPENAME);

		bf = (BloomFilter *) PG_GETARG_VARLENA_P(i);

		if (result)
		{
			if (bf->m != result->m)
				elog(ERROR, "bloom_%s operands must all have the same p and n", intersection ? "intersection" : "union");
			else if (bf->k != result->k)
				elog(ERROR, "bloom_%s operands must all have the same p and n", intersection ? "intersection" : "union");
		}
		if (result == NULL)
			result = bf;
		else if (intersection)
			result = BloomFilterIntersection(result, bf);
		else
			result = BloomFilterUnion(result, bf);
	}

	return result;
}

/*
 * bloom_union
 *
 * Returns the union of the given Bloom filters
 */
PG_FUNCTION_INFO_V1(bloom_union);
Datum
bloom_union(PG_FUNCTION_ARGS)
{
	BloomFilter *result = bloom_operation(fcinfo, false);

	if (result == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(result);
}

/*
 * bloom_intersection
 *
 * Returns the intersection of the given Bloom filters
 */
PG_FUNCTION_INFO_V1(bloom_intersection);
Datum
bloom_intersection(PG_FUNCTION_ARGS)
{
	BloomFilter *result = bloom_operation(fcinfo, true);

	if (result == NULL)
		PG_RETURN_NULL();
	else
		PG_RETURN_POINTER(result);
}

/*
 * bloom_intersection_agg transition function -
 *
 * 	returns the intersection of the transition state and the given Bloom filter
 */
PG_FUNCTION_INFO_V1(bloom_intersection_agg_trans);
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
PG_FUNCTION_INFO_V1(bloom_cardinality);
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
PG_FUNCTION_INFO_V1(bloom_contains);
Datum
bloom_contains(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom;
	Datum elem = PG_GETARG_DATUM(1);
	bool contains = false;
	Oid	val_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	TypeCacheEntry *typ;
	StringInfo buf;

	if (val_type == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine input data type")));

	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(contains);

	bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	typ = lookup_type_cache(val_type, 0);

	buf = makeStringInfo();
	DatumToBytes(elem, typ, buf);

	contains = BloomFilterContains(bloom, buf->data, buf->len);

	pfree(buf->data);
	pfree(buf);

	PG_RETURN_BOOL(contains);
}

/*
 * bloom_empty
 */
PG_FUNCTION_INFO_V1(bloom_empty);
Datum
bloom_empty(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom = BloomFilterCreate();
	PG_RETURN_POINTER(bloom);
}

/*
 * bloom_emptyp
 */
PG_FUNCTION_INFO_V1(bloom_emptyp);
Datum
bloom_emptyp(PG_FUNCTION_ARGS)
{
	float8 p = PG_GETARG_FLOAT8(0);
	int8 n = PG_GETARG_INT64(1);
	BloomFilter *bloom = bloom_create(p, n);
	PG_RETURN_POINTER(bloom);
}

/*
 * bloom_add
 */
PG_FUNCTION_INFO_V1(bloom_add);
Datum
bloom_add(PG_FUNCTION_ARGS)
{
	BloomFilter *bloom;

	if (PG_ARGISNULL(0))
		bloom = BloomFilterCreate();
	else
		bloom = (BloomFilter *) PG_GETARG_VARLENA_P(0);

	fcinfo->flinfo->fn_extra = lookup_type_cache(get_fn_expr_argtype(fcinfo->flinfo, 1), 0);
	bloom = bloom_add_datum(fcinfo, bloom, PG_GETARG_DATUM(1));
	PG_RETURN_POINTER(bloom);
}

/*
 * bloom_in
 */
PG_FUNCTION_INFO_V1(bloom_in);
Datum
bloom_in(PG_FUNCTION_ARGS)
{
	char *raw = PG_GETARG_CSTRING(0);
	Datum result = DirectFunctionCall1(byteain, (Datum) raw);

	PG_RETURN_POINTER(result);
}

/*
 * bloom_out
 */
PG_FUNCTION_INFO_V1(bloom_out);
Datum
bloom_out(PG_FUNCTION_ARGS)
{
	BloomFilter	*bf = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	char *s = (char *) DirectFunctionCall1(byteaout, (Datum) bf);

	PG_RETURN_CSTRING(s);
}

/*
 * bloom_send
 */
PG_FUNCTION_INFO_V1(bloom_send);
Datum
bloom_send(PG_FUNCTION_ARGS)
{
	BloomFilter	*bf = (BloomFilter *) PG_GETARG_VARLENA_P(0);
	bytea *result = (bytea *) DirectFunctionCall1(byteasend, (Datum) bf);

	PG_RETURN_BYTEA_P(result);
}

/*
 * bloom_recv
 */
PG_FUNCTION_INFO_V1(bloom_recv);
Datum
bloom_recv(PG_FUNCTION_ARGS)
{
	StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
	bytea *result = (bytea *) DirectFunctionCall1(bytearecv, (Datum) buf);

	PG_RETURN_BYTEA_P(result);
}
