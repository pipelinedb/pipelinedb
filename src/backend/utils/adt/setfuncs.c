/*-------------------------------------------------------------------------
 *
 * set.c
 *		Set functions
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/tupmacs.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/miscutils.h"
#include "pipeline/scheduler.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/setfuncs.h"
#include "utils/typcache.h"

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
Datum
set_cardinality(PG_FUNCTION_ARGS)
{
	Datum array;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	array = DirectFunctionCall1(array_agg_finalfn, PG_GETARG_DATUM(0));
	PG_RETURN_INT32(DirectFunctionCall2(array_length, array, Int32GetDatum(1)));
}

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
 * bucket_agg_trans_startup
 */
static BucketAggTransState *
bucket_agg_trans_startup(BucketAggState *bas)
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

	return result;
}

/*
 * bucket_agg_state_send
 */
Datum
bucket_agg_state_send(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state = (BucketAggTransState *) PG_GETARG_POINTER(0);
	StringInfoData buf;

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

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * bucket_agg_state_recv
 */
Datum
bucket_agg_state_recv(PG_FUNCTION_ARGS)
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

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.hcxt = CurrentMemoryContext;
	hctl.keysize = sizeof(uint64);
	hctl.entrysize = sizeof(SetElement);

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
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
add_hash_to_bucket(BucketAggState *bas, BucketAggTransState *state, uint16 set_id, uint32 hash, MemoryContext set_context)
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
	state->sets[elem->index] = set_id;
}

/*
 * bucket_agg_trans
 */
Datum
bucket_agg_trans(PG_FUNCTION_ARGS)
{
	BucketAggState *bas;
	MemoryContext old;
	MemoryContext context;
	BucketAggTransState *state = PG_ARGISNULL(0) ? NULL : (BucketAggTransState *) PG_GETARG_POINTER(0);
	Datum elem;
	uint16 set_id;
	uint32 hash;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "bucket_agg_trans called in non-aggregate context");

	if (PG_ARGISNULL(2))
		elog(ERROR, "set ID cannot be NULL");

	set_id = PG_GETARG_UINT16(2);
	old = MemoryContextSwitchTo(context);

	if (fcinfo->flinfo->fn_extra == NULL)
		bucket_agg_startup(fcinfo);

	Assert(fcinfo->flinfo->fn_extra);
	bas = (BucketAggState *) fcinfo->flinfo->fn_extra;

	if (state == NULL)
		state = bucket_agg_trans_startup(bas);

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

	add_hash_to_bucket(bas, state, set_id, hash, fcinfo->flinfo->fn_mcxt);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * bucket_agg_combine
 */
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
		state = (BucketAggTransState *) PG_GETARG_POINTER(0);
	else
		state = bucket_agg_trans_startup(bas);

	if (!PG_ARGISNULL(1))
	{
		int i;
		incoming = (BucketAggTransState *) PG_GETARG_POINTER(1);

		Assert(incoming);

		for (i = 0; i < incoming->count; i++)
			add_hash_to_bucket(bas, state, incoming->sets[i], incoming->hashes[i], fcinfo->flinfo->fn_mcxt);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
bucket_agg_final(PG_FUNCTION_ARGS)
{
	BucketAggTransState *state = (BucketAggTransState *) PG_GETARG_POINTER(0);

	bytea *bytes = (bytea *) DirectFunctionCall1(bucket_agg_state_send, (Datum) state);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * bucket_cardinality
 */
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
