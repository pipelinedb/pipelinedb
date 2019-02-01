/*-------------------------------------------------------------------------
 *
 * aggfuncs.c
 *	  Aggregate support functions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "analyzer.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "microbatch.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

typedef struct NamedSet
{
	char *name;
	HTAB *htab;
	uint32 *hashes;
	int num_hashes;
} NamedSet;

typedef struct BucketAggTransState
{
	TypeCacheEntry *type;
	List *set_names;
	uint32 *hashes;
	uint16 *sets;
	uint32 *timestamps;
	int capacity;
	int count;
	uint32 bucket_id;
} BucketAggTransState;

typedef struct SetElement
{
	uint64 key;
	int index;
} SetElement;

typedef struct BucketAggState
{
	TypeCacheEntry *typ;
	HTAB *htab;
	uint32 num_buckets;
} BucketAggState;

/*
 * array_agg_serialize
 */
PG_FUNCTION_INFO_V1(array_agg_serialize);
Datum
array_agg_serialize(PG_FUNCTION_ARGS)
{
	ArrayBuildState *state = (ArrayBuildState *) PG_GETARG_POINTER(0);
	ArrayType *vals;
	int	dims[1];
	int	lbs[1];

	dims[0] = state->nelems;
	lbs[0] = 1;

	vals = construct_md_array(state->dvalues, state->dnulls, 1, dims, lbs, state->element_type,
			state->typlen, state->typbyval, state->typalign);

	PG_RETURN_ARRAYTYPE_P(vals);
}

/*
 * array_agg_deserialize
 */
PG_FUNCTION_INFO_V1(array_agg_deserialize);
Datum
array_agg_deserialize(PG_FUNCTION_ARGS)
{
	ArrayType *vals;
	ArrayBuildState *result;
	MemoryContext old;
	MemoryContext context;

	if (!AggCheckCallContext(fcinfo, &context))
		context = CurrentMemoryContext;

	old = MemoryContextSwitchTo(context);
	vals = (ArrayType *) PG_GETARG_ARRAYTYPE_P_COPY(0);

	result = (ArrayBuildState *) palloc0(sizeof(ArrayBuildState));
	result->mcontext = CurrentMemoryContext;
	result->element_type = ARR_ELEMTYPE(vals);

	get_typlenbyvalalign(result->element_type,
						 &result->typlen,
						 &result->typbyval,
						 &result->typalign);

	deconstruct_array(vals, result->element_type,
			result->typlen, result->typbyval, result->typalign,
			&result->dvalues, &result->dnulls, &result->nelems);

	result->alen = result->nelems;

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * array_agg_combine
 */
PG_FUNCTION_INFO_V1(array_agg_combine);
Datum
array_agg_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildState *result = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
	ArrayBuildState *toappend = PG_ARGISNULL(1) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(1);
	MemoryContext aggcontext;
	int i;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "array_agg_combine called in non-aggregate context");
	}

	if (toappend == NULL)
		PG_RETURN_POINTER(result);

	/*
	 * The incoming arrays will be appended to the existing transition state,
	 * but the order in which they're appended isn't predictable.
	 */
	for (i=0; i<toappend->nelems; i++)
	{
		result = accumArrayResult(result,
				toappend->dvalues[i], toappend->dnulls[i],
				toappend->element_type, aggcontext);
	}

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(combinable_array_agg_finalfn);
Datum
combinable_array_agg_finalfn(PG_FUNCTION_ARGS)
{
	Datum		result;
	ArrayBuildState *state;
	int			dims[1];
	int			lbs[1];

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();		/* returns null iff no input values */

	dims[0] = state->nelems;
	lbs[0] = 1;

	/*
	 * Make the result.  We cannot release the ArrayBuildState because
	 * sometimes aggregate final functions are re-executed.  Rather, it is
	 * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
	 * so.
	 */
	result = makeMdArrayResult(state, 1, dims, lbs,
							   CurrentMemoryContext,
							   false);

	PG_RETURN_DATUM(result);
}

/*
 * array_agg_array_combine
 */
PG_FUNCTION_INFO_V1(array_agg_array_combine);
Datum
array_agg_array_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *result = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
	ArrayBuildStateArr *toappend = PG_ARGISNULL(1) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(1);
	MemoryContext aggcontext;
	ArrayType *arr;
	ArrayIterator it;
	Datum val;
	bool isnull;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "array_agg_array_combine called in non-aggregate context");
	}

	if (toappend == NULL)
		PG_RETURN_POINTER(result);

	arr = DatumGetArrayTypeP(makeArrayResultArr(toappend, aggcontext, false));
	it = array_create_iterator(arr, 1, NULL);

	while (array_iterate(it, &val, &isnull))
		result = accumArrayResultArr(result, val, isnull, toappend->array_type, aggcontext);

	array_free_iterator(it);

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(combinable_array_agg_array_finalfn);
Datum
combinable_array_agg_array_finalfn(PG_FUNCTION_ARGS)
{
	Datum		result;
	ArrayBuildStateArr *state;

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildStateArr *) PG_GETARG_POINTER(0);

	if (state == NULL)
		PG_RETURN_NULL();		/* returns null iff no input values */

	/*
	 * Make the result.  We cannot release the ArrayBuildStateArr because
	 * sometimes aggregate final functions are re-executed.  Rather, it is
	 * nodeAgg.c's responsibility to reset the aggcontext when it's safe to do
	 * so.
	 */
	result = makeArrayResultArr(state, CurrentMemoryContext, false);

	PG_RETURN_DATUM(result);
}

/*
 * array_agg_array_serialize
 */
PG_FUNCTION_INFO_V1(array_agg_array_serialize);
Datum
array_agg_array_serialize(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *state = (ArrayBuildStateArr *) PG_GETARG_POINTER(0);
	bytea *result;
	char *pos;
	int nbytes;

	nbytes = sizeof(ArrayBuildStateArr) + state->abytes + ((state->aitems + 7) / 8);
	result = (bytea *) palloc0(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pos = VARDATA(result);
	memcpy(pos, state, sizeof(ArrayBuildStateArr));
	pos += sizeof(ArrayBuildStateArr);
	memcpy(pos, state->data, state->abytes);
	pos += state->abytes;
	if (state->aitems)
		array_bitmap_copy((bits8 *) pos, 0, state->nullbitmap, 0, state->aitems);

	PG_RETURN_BYTEA_P(result);
}

/*
 * array_agg_array_deserialize
 */
PG_FUNCTION_INFO_V1(array_agg_array_deserialize);
Datum
array_agg_array_deserialize(PG_FUNCTION_ARGS)
{
	ArrayBuildStateArr *result;
	char *pos;
	MemoryContext old;
	MemoryContext context;

	if (!AggCheckCallContext(fcinfo, &context))
		context = fcinfo->flinfo->fn_mcxt;

	old = MemoryContextSwitchTo(context);

	pos = (char *) VARDATA(PG_GETARG_BYTEA_P_COPY(0));
	result = (ArrayBuildStateArr *) pos;
	result->mcontext = CurrentMemoryContext;

	pos += sizeof(ArrayBuildStateArr);
	result->data = pos;

	pos += result->abytes;
	if (result->aitems)
		result->nullbitmap = (bits8 *) pos;
	else
		result->nullbitmap = NULL;

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

typedef struct SetAggState
{
	TypeCacheEntry *typ;
	bool has_null;
} SetAggState;

#define MAGIC_HEADER 0x5AFE

/*
 * set_agg_startup
 */
static void
set_agg_startup(FunctionCallInfo fcinfo, Oid type)
{
	MemoryContext old;
	SetAggState *state;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	state = palloc0(sizeof(SetAggState));

	state->has_null = false;
	state->typ = lookup_type_cache(type,
			TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	if (type == RECORDOID || state->typ->typtype == TYPTYPE_COMPOSITE)
		elog(ERROR, "composite types are not supported by set_agg");

	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) state;
}

/*
 * set_add
 */
static ArrayBuildState *
set_add(FunctionCallInfo fcinfo, MemoryContext aggcontext, Datum d, bool isnull, ArrayBuildState *state)
{
	SetAggState *sas;
	int i;

	sas = (SetAggState *) fcinfo->flinfo->fn_extra;

	if (isnull)
	{
		if (!sas->has_null)
		{
			state = accumArrayResult(state, 0, true, sas->typ->type_id, aggcontext);
			sas->has_null = true;
		}

		return state;
	}

	if (state)
	{
		for (i = 0; i < state->nelems; i++)
		{
			if ((isnull && state->dnulls[i]) || datumIsEqual(d, state->dvalues[i], sas->typ->typbyval, sas->typ->typlen))
			{
				/* Duplicate value, no need to add it */
				return state;
			}
		}
	}

	state = accumArrayResult(state, d, false, sas->typ->type_id, aggcontext);

	return state;
}

/*
 * set_agg_trans
 */
PG_FUNCTION_INFO_V1(set_agg_trans);
Datum
set_agg_trans(PG_FUNCTION_ARGS)
{
	ArrayBuildState *state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
	MemoryContext old;
	MemoryContext context;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "set_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (fcinfo->flinfo->fn_extra == NULL)
		set_agg_startup(fcinfo, AggGetInitialArgType(fcinfo));

	state = set_add(fcinfo, context, PG_GETARG_DATUM(1), PG_ARGISNULL(1), state);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * set_agg_combine
 */
PG_FUNCTION_INFO_V1(set_agg_combine);
Datum
set_agg_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildState *state;
	MemoryContext old;
	MemoryContext context;
	int i;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "set_agg_combine called in non-aggregate context");

	if (PG_ARGISNULL(0) && PG_ARGISNULL(1))
		PG_RETURN_NULL();

	old = MemoryContextSwitchTo(context);

	state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);

	/*
	 * The incoming arrays will be appended to the existing transition state,
	 * but the order in which they're appended isn't predictable.
	 */
	if (!PG_ARGISNULL(1))
	{
		ArrayBuildState *incoming = (ArrayBuildState *) PG_GETARG_POINTER(1);

		if (fcinfo->flinfo->fn_extra == NULL)
			set_agg_startup(fcinfo, incoming->element_type);

		for (i = 0; i < incoming->nelems; i++)
			state = set_add(fcinfo, context, incoming->dvalues[i], incoming->dnulls[i], state);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * set_cardinality
 */
PG_FUNCTION_INFO_V1(set_cardinality);
Datum
set_cardinality(PG_FUNCTION_ARGS)
{
	Datum array;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	array = DirectFunctionCall1(combinable_array_agg_finalfn, PG_GETARG_DATUM(0));
	PG_RETURN_INT32(DirectFunctionCall2(array_length, array, Int32GetDatum(1)));
}

/*
 * bucket_agg_trans_startup
 */
static BucketAggTransState *
bucket_agg_trans_startup(BucketAggState *bas, uint32 ts)
{
	BucketAggTransState *result = palloc0(sizeof(BucketAggTransState));

	/*
	 * We'll grow this as necessary
	 */
	result->capacity = 1024;
	result->hashes = palloc0(sizeof(uint32) * result->capacity);
	result->sets = palloc0(sizeof(uint16) * result->capacity);
	result->count = 0;
	result->bucket_id = bas->num_buckets++;

	if (ts)
		result->timestamps = palloc0(sizeof(uint32) * result->capacity);
	else
		result->timestamps = NULL;

	return result;
}

/*
 * bucket_agg_state_serialize
 */
PG_FUNCTION_INFO_V1(bucket_agg_state_serialize);
Datum
bucket_agg_state_serialize(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state = PG_ARGISNULL(0) ? NULL : (BucketAggTransState *) PG_GETARG_POINTER(0);
	StringInfoData buf;

	if (!state)
		PG_RETURN_NULL();

	Assert(state);

	pq_begintypsend(&buf);

	/* Magic header */
	pq_sendint(&buf, MAGIC_HEADER, 4);

	/* Number of elements in all sets */
	pq_sendint(&buf, state->count, sizeof(state->count));

	Assert(state->hashes);
	Assert(state->count);

	/* We serialize the set ids first so set cardinality lookups need not even look at the array of hashes */
	pq_sendbytes(&buf, (char *) state->sets, state->count * sizeof(uint16));

	/* Array of unique hashes */
	pq_sendbytes(&buf, (char *) state->hashes, state->count * sizeof(uint32));

	/* Optional array of timestamps */
	if (state->timestamps)
		pq_sendbytes(&buf, (char *) state->timestamps, state->count * sizeof(uint32));

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * bucket_agg_state_deserialize
 */
PG_FUNCTION_INFO_V1(bucket_agg_state_deserialize);
Datum
bucket_agg_state_deserialize(PG_FUNCTION_ARGS)
{
	bytea *bytes;
	StringInfoData buf;
	BucketAggTransState *result;
	MemoryContext context;
	MemoryContext old;
	int nbytes;
	int size;

	if (!AggCheckCallContext(fcinfo, &context))
		context = CurrentMemoryContext;

	old = MemoryContextSwitchTo(context);
	bytes = PG_GETARG_BYTEA_P(0);
	result = palloc0(sizeof(BucketAggTransState));
	nbytes = VARSIZE(bytes);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed bucket_agg transition state received");

	result->count = pq_getmsgint(&buf, sizeof(int));
	result->capacity = result->count;

	size = sizeof(uint16) * result->count;
	result->sets = palloc0(size);
	memcpy(result->sets, pq_getmsgbytes(&buf, size), size);

	size = sizeof(uint32) * result->count;
	result->hashes = palloc0(size);
	memcpy(result->hashes, pq_getmsgbytes(&buf, size), size);

	/*
	 * If there's more data, it's the optional timestamps array
	 */
	if (buf.cursor + size < buf.len)
	{
		size = sizeof(uint32) * result->count;
		result->timestamps = palloc0(size);
		memcpy(result->timestamps, pq_getmsgbytes(&buf, size), size);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * bucket_agg_startup
 */
static void
bucket_agg_startup(FunctionCallInfo fcinfo)
{
	MemoryContext old;
	BucketAggState *state;
	Oid type = AggGetInitialArgType(fcinfo);
	HASHCTL hctl;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.hcxt = CurrentMemoryContext;
	hctl.keysize = sizeof(uint64);
	hctl.entrysize = sizeof(SetElement);

	state = palloc0(sizeof(BucketAggState));
	state->num_buckets = 0;

	state->typ = lookup_type_cache(type,
			TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);
	state->htab = hash_create("BucketAggHash", continuous_query_batch_size, &hctl, HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);

	if (type == RECORDOID || state->typ->typtype == TYPTYPE_COMPOSITE)
		elog(ERROR, "composite types are not supported by bucket_agg");

	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) state;
}

static uint64
bucket_agg_hash(uint32 bucket_id, uint32 hash)
{
	uint64 result = bucket_id;

	result <<= 32;
	result |= hash;

	return result;
}

/*
 * add_hash_to_bucket
 */
static void
add_hash_to_bucket(BucketAggState *bas, BucketAggTransState *state,
		uint16 set_id, uint32 hash, uint32 ts, MemoryContext set_context)
{
	bool found;
	SetElement *elem;
	uint64 bucket_hash = bucket_agg_hash(state->bucket_id, hash);

	elem = (SetElement *) hash_search_with_hash_value(bas->htab, &bucket_hash, bucket_hash, HASH_ENTER, &found);
	if (!found)
	{
		/*
		 * New element, add the hash to the array of hashes that's used for fast serialization
		 */
		if (state->count == state->capacity)
		{
			state->capacity = 2 * state->capacity;
			state->hashes = repalloc(state->hashes, sizeof(uint32) * state->capacity);
			state->sets = repalloc(state->sets, sizeof(uint16) * state->capacity);
			if (ts)
			{
				state->timestamps = repalloc(state->timestamps, sizeof(uint32) * state->capacity);
				MemSet(state->timestamps + state->count, 0, sizeof(uint32) * (state->capacity - state->count));
			}
		}
		elem->index = state->count;
		state->hashes[elem->index] = hash;
		state->sets[elem->index] = set_id;
		state->count++;
	}
	else
	{
		Assert(state->count);
	}

	if (ts)
	{
		/*
		 * Don't overwrite the set_id for this hash unless it's more recent than the existing value
		 */
		Assert(state->timestamps);
		if (ts >= state->timestamps[elem->index])
		{
			state->sets[elem->index] = set_id;
			state->timestamps[elem->index] = ts;
			return;
		}
	}
	else
	{
		state->sets[elem->index] = set_id;
	}
}

/*
 * bucket_agg_trans_internal
 */
static BucketAggTransState *
bucket_agg_trans_internal(FunctionCallInfo fcinfo, uint32 ts)
{
	BucketAggState *bas;
	MemoryContext old;
	MemoryContext context;
	BucketAggTransState *state = PG_ARGISNULL(0) ? NULL : (BucketAggTransState *) PG_GETARG_POINTER(0);
	Datum elem;
	uint16 set_id;
	uint32 hash;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bucket_agg_trans_internal called in non-aggregate context");

	if (PG_ARGISNULL(2))
		elog(ERROR, "set ID cannot be NULL");

	set_id = PG_GETARG_UINT16(2);
	old = MemoryContextSwitchTo(context);

	if (fcinfo->flinfo->fn_extra == NULL)
		bucket_agg_startup(fcinfo);

	Assert(fcinfo->flinfo->fn_extra);
	bas = (BucketAggState *) fcinfo->flinfo->fn_extra;

	if (state == NULL)
		state = bucket_agg_trans_startup(bas, ts);

	/* We just hash NULLs to 0 */
	if (PG_ARGISNULL(1))
	{
		hash = 0;
	}
	else
	{
		elem = PG_GETARG_DATUM(1);
		hash = DatumGetUInt32(FunctionCall1(&bas->typ->hash_proc_finfo, elem));
	}

	add_hash_to_bucket(bas, state, set_id, hash, ts, fcinfo->flinfo->fn_mcxt);

	MemoryContextSwitchTo(old);

	return state;
}

/*
 * bucket_agg_trans
 */
PG_FUNCTION_INFO_V1(bucket_agg_trans);
Datum
bucket_agg_trans(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state;

	state = bucket_agg_trans_internal(fcinfo, 0);

	PG_RETURN_POINTER(state);
}

/*
 * bucket_agg_trans_ts
 */
PG_FUNCTION_INFO_V1(bucket_agg_trans_ts);
Datum
bucket_agg_trans_ts(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state;
	TimestampTz ts;

	if (PG_ARGISNULL(3))
		elog(ERROR, "timestamp cannot be NULL");

	ts = PG_GETARG_TIMESTAMPTZ(3);
	state = bucket_agg_trans_internal(fcinfo, (uint32) timestamptz_to_time_t(ts));

	PG_RETURN_POINTER(state);
}

/*
 * bucket_agg_combine
 */
PG_FUNCTION_INFO_V1(bucket_agg_combine);
Datum
bucket_agg_combine(PG_FUNCTION_ARGS)
{
	BucketAggState *bas;
	BucketAggTransState *state;
	BucketAggTransState *incoming;
	MemoryContext context;
	MemoryContext old;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bucket_agg_combine called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (fcinfo->flinfo->fn_extra == NULL)
		bucket_agg_startup(fcinfo);

	Assert(fcinfo->flinfo->fn_extra);
	bas = (BucketAggState *) fcinfo->flinfo->fn_extra;

	if (!PG_ARGISNULL(0))
	{
		state = (BucketAggTransState *) PG_GETARG_POINTER(0);
	}
	else
	{
		Assert(!PG_ARGISNULL(1));
		incoming = (BucketAggTransState *) PG_GETARG_POINTER(1);
		state = bucket_agg_trans_startup(bas, incoming->timestamps ? 1 : 0);
	}

	if (!PG_ARGISNULL(1))
	{
		int i;
		incoming = (BucketAggTransState *) PG_GETARG_POINTER(1);

		Assert(incoming);

		for (i = 0; i < incoming->count; i++)
		{
			uint32 ts = incoming->timestamps ? incoming->timestamps[i] : 0;
			add_hash_to_bucket(bas, state, incoming->sets[i], incoming->hashes[i], ts, fcinfo->flinfo->fn_mcxt);
		}
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bucket_agg_final
 */
PG_FUNCTION_INFO_V1(bucket_agg_final);
Datum
bucket_agg_final(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state = PG_ARGISNULL(0) ? NULL : (BucketAggTransState *) PG_GETARG_POINTER(0);
	bytea *bytes;

	if (!state)
		PG_RETURN_NULL();

	bytes = (bytea *) DirectFunctionCall1(bucket_agg_state_serialize, (Datum) state);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * bucket_cardinality
 */
PG_FUNCTION_INFO_V1(bucket_cardinality);
Datum
bucket_cardinality(PG_FUNCTION_ARGS)
{
	bytea *bytes;
	uint16 set_id;
	StringInfoData buf;
	int nbytes;
	int i;
	int num_hashes;
	uint16 *sets;
	int result;

	if (PG_ARGISNULL(1))
		elog(ERROR, "set ID cannot be NULL");

	set_id = PG_GETARG_UINT16(1);
	bytes = PG_GETARG_BYTEA_P(0);

	nbytes = VARSIZE(bytes);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed bucket_agg transition state received");

	num_hashes = pq_getmsgint(&buf, sizeof(int));
	sets = (uint16 *) pq_getmsgbytes(&buf, sizeof(uint16) * num_hashes);

	result = 0;
	for (i = 0; i < num_hashes; i++)
	{
		if (sets[i] == set_id)
			result++;
	}

	PG_RETURN_INT32(result);
}

/*
 * bucket_cardinalities
 */
PG_FUNCTION_INFO_V1(bucket_cardinalities);
Datum
bucket_cardinalities(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	StringInfoData buf;
	int num_hashes;
	int nbytes;
	uint16 *sets;
	ArrayType *arr;
	int i;
	Datum *values;
	List *ids = NIL;
	ListCell *lc;

	static uint16 seen_ids[USHRT_MAX];
	static int cards[USHRT_MAX];

	MemSet(&seen_ids, 0, sizeof(seen_ids));
	MemSet(&cards, 0, sizeof(cards));

	nbytes = VARSIZE(bytes);
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed bucket_agg transition state received");

	num_hashes = pq_getmsgint(&buf, sizeof(int));
	sets = (uint16 *) pq_getmsgbytes(&buf, sizeof(uint16) * num_hashes);

	for (i = 0; i < num_hashes; i++)
	{
		if (!cards[sets[i]])
			ids = lappend_int(ids, sets[i]);
		cards[sets[i]]++;
	}

	values = palloc0(sizeof(Datum) * list_length(ids));
	i = 0;
	foreach(lc, ids)
		values[i++] = Int32GetDatum(cards[lfirst_int(lc)]);

	arr = construct_array(values, list_length(ids), INT4OID, 4, true, 'i');
	PG_RETURN_ARRAYTYPE_P(arr);

	PG_RETURN_NULL();
}

/*
 * bucket_ids
 */
PG_FUNCTION_INFO_V1(bucket_ids);
Datum
bucket_ids(PG_FUNCTION_ARGS)
{
	bytea *bytes = PG_GETARG_BYTEA_P(0);
	StringInfoData buf;
	int num_hashes;
	int nbytes;
	uint16 *sets;
	ArrayType *arr;
	int i;
	Datum *values;
	List *ids = NIL;
	ListCell *lc;

	static uint16 seen_ids[1 << 16];

	MemSet(&seen_ids, 0, sizeof(seen_ids));

	nbytes = VARSIZE(bytes);
	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed bucket_agg transition state received");

	num_hashes = pq_getmsgint(&buf, sizeof(int));
	sets = (uint16 *) pq_getmsgbytes(&buf, sizeof(uint16) * num_hashes);

	for (i = 0; i < num_hashes; i++)
	{
		if (!seen_ids[sets[i]])
		{
			seen_ids[sets[i]] = 1;
			ids = lappend_int(ids, sets[i]);
		}
	}

	values = palloc0(sizeof(Datum) * list_length(ids));
	i = 0;
	foreach(lc, ids)
		values[i++] = UInt16GetDatum(lfirst_int(lc));

	arr = construct_array(values, list_length(ids), INT2OID, 2, true, 's');
	PG_RETURN_ARRAYTYPE_P(arr);
}

typedef struct StringAggState
{
	/* string being built */
	StringInfo buf;

	/* length of the first delimiter, which we'll need to trim off */
	int dlen;
} StringAggState;

/* subroutine to initialize state */
static StringAggState *
makeStringAggState(FunctionCallInfo fcinfo)
{
	StringAggState *result;
	MemoryContext aggcontext;
	MemoryContext oldcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
		aggcontext = fcinfo->flinfo->fn_mcxt;

	/*
	 * Create state in aggregate context.  It'll stay there across subsequent
	 * calls.
	 */
	oldcontext = MemoryContextSwitchTo(aggcontext);
	result = palloc0(sizeof(StringAggState));
	result->buf = makeStringInfo();
	MemoryContextSwitchTo(oldcontext);

	return result;
}

/*
 * Serializes a string aggregation transition state for transmission
 * to an external process
 */
PG_FUNCTION_INFO_V1(string_agg_serialize);
Datum
string_agg_serialize(PG_FUNCTION_ARGS)
{
	StringAggState *state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);
	StringInfoData buf;
	bytea *result;
	int nbytes;

	if (!state)
		PG_RETURN_NULL();

	initStringInfo(&buf);

	pq_sendint(&buf, state->dlen, sizeof(int));
	appendStringInfoString(&buf, state->buf->data);

	nbytes = buf.len - buf.cursor;
	result = (bytea *) palloc(nbytes + VARHDRSZ);
	SET_VARSIZE(result, nbytes + VARHDRSZ);

	pq_copymsgbytes(&buf, VARDATA(result), nbytes);

	PG_RETURN_BYTEA_P(result);
}

/*
 * Deserializes a string aggregation transition state sent by an
 * external process
 */
PG_FUNCTION_INFO_V1(string_agg_deserialize);
Datum
string_agg_deserialize(PG_FUNCTION_ARGS)
{
	bytea *bytesin = (bytea *) PG_GETARG_BYTEA_P(0);
	StringAggState *result;
	StringInfoData buf;
	int nbytes;

	nbytes = VARSIZE(bytesin) - VARHDRSZ;

	result = (StringAggState *) palloc0(sizeof(StringAggState));

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytesin), nbytes);

	result->dlen = pq_getmsgint(&buf, sizeof(int));
	result->buf = makeStringInfo();
	appendStringInfoString(result->buf, pq_getmsgbytes(&buf, nbytes - sizeof(int)));

	PG_RETURN_POINTER(result);
}

/*
 * combinable_string_agg_combine
 *
 * Concatenates two delimited strings together
 */
PG_FUNCTION_INFO_V1(combinable_string_agg_combine);
Datum
combinable_string_agg_combine(PG_FUNCTION_ARGS)
{
	StringAggState *state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);
	StringAggState *incoming = PG_ARGISNULL(1) ? NULL : (StringAggState *) PG_GETARG_POINTER(1);

	if (state == NULL)
	{
		state = makeStringAggState(fcinfo);
		state->dlen = incoming->dlen;
	}

	if (incoming != NULL)
		appendStringInfoString(state->buf, incoming->buf->data);

	PG_RETURN_POINTER(state);
}

/*
 * appendStringInfoText
 *
 * Append a text to str.
 * Like appendStringInfoString(str, text_to_cstring(t)) but faster.
 */
static void
appendStringInfoText(StringInfo str, const text *t)
{
	appendBinaryStringInfo(str, VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
}

/*
 * combinable_string_agg_transfn
 */
PG_FUNCTION_INFO_V1(combinable_string_agg_transfn);
Datum
combinable_string_agg_transfn(PG_FUNCTION_ARGS)
{
	StringAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);

	/* Append the value unless null. */
	if (!PG_ARGISNULL(1))
	{
		bool isfirst = false;
		if (state == NULL)
		{
			isfirst = true;
			state = makeStringAggState(fcinfo);
		}

		if (!PG_ARGISNULL(2))
			appendStringInfoText(state->buf, PG_GETARG_TEXT_PP(2));	/* delimiter */

		/* the delimiter preceding the first element is trimmed off when finalizing */
		if (isfirst)
			state->dlen = state->buf->len;

		appendStringInfoText(state->buf, PG_GETARG_TEXT_PP(1));		/* value */
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
}

/*
 * combinable_string_agg_finalfn
 */
PG_FUNCTION_INFO_V1(combinable_string_agg_finalfn);
Datum
combinable_string_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);

	/* trim off the delimiter of the first element */
	if (state != NULL)
		PG_RETURN_TEXT_P(cstring_to_text_with_len(
				state->buf->data + state->dlen, state->buf->len - state->dlen));
	else
		PG_RETURN_NULL();
}

/*
 * bytea_string_agg_transfn
 */
PG_FUNCTION_INFO_V1(combinable_bytea_string_agg_transfn);
Datum
combinable_bytea_string_agg_transfn(PG_FUNCTION_ARGS)
{
	StringAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);

	/* Append the value unless null. */
	if (!PG_ARGISNULL(1))
	{
		bytea *value = PG_GETARG_BYTEA_PP(1);
		bool isfirst = false;
		if (state == NULL)
		{
			isfirst = true;
			state = makeStringAggState(fcinfo);
		}

		if (!PG_ARGISNULL(2))
		{
			bytea *delim = PG_GETARG_BYTEA_PP(2);

			appendBinaryStringInfo(state->buf, VARDATA_ANY(delim), VARSIZE_ANY_EXHDR(delim));
		}

		if (isfirst)
			state->dlen = state->buf->len;

		appendBinaryStringInfo(state->buf, VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value));
	}

	/*
	 * The transition type for string_agg() is declared to be "internal",
	 * which is a pass-by-value type the same size as a pointer.
	 */
	PG_RETURN_POINTER(state);
}

/*
 * combinable_bytea_string_agg_finalfn
 */
PG_FUNCTION_INFO_V1(combinable_bytea_string_agg_finalfn);
Datum
combinable_bytea_string_agg_finalfn(PG_FUNCTION_ARGS)
{
	StringAggState *state;

	state = PG_ARGISNULL(0) ? NULL : (StringAggState *) PG_GETARG_POINTER(0);

	if (state != NULL)
	{
		bytea *result;
		int size = state->buf->len - state->dlen;
		result = (bytea *) palloc(size + VARHDRSZ);
		SET_VARSIZE(result, size + VARHDRSZ);
		memcpy(VARDATA(result), state->buf->data + state->dlen, size);
		PG_RETURN_BYTEA_P(result);
	}
	else
		PG_RETURN_NULL();
}
