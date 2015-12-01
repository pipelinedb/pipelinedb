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
#include "pipeline/miscutils.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/setfuncs.h"
#include "utils/typcache.h"

#define MURMUR_SEED 0x02cffb4c45ee1fb8L

typedef struct SetKey
{
	uint32 hash;
	Datum value;
	TypeCacheEntry *typ;
} SetKey;

typedef struct SetAggState
{
	HTAB *htab;
	TypeCacheEntry *typ;
} SetAggState;

/*
 * set_key_match
 */
static int
set_key_match(const void *kl, const void *kr, Size keysize)
{
	SetKey *l = (SetKey *) kl;
	SetKey *r = (SetKey *) kr;
	int cmp;

	Assert(l->typ->type_id == r->typ->type_id);

	cmp = datumIsEqual(l->value, r->value, l->typ->typbyval, l->typ->typlen) ? 0 : 1;

	return DatumGetInt32(cmp);
}

/*
 * set_key_hash
 */
static uint32
set_key_hash(const void *key, Size keysize)
{
	SetKey *k = (SetKey *) key;
	StringInfoData buf;
	uint64_t h;

	initStringInfo(&buf);
	DatumToBytes(k->value, k->typ, &buf);

	h = MurmurHash3_64(buf.data, buf.len, MURMUR_SEED);
	pfree(buf.data);

	return DatumGetUInt32(h);
}

/*
 * set_agg_startup
 */
static void
set_agg_startup(FunctionCallInfo fcinfo, Oid type)
{
	MemoryContext old;
	HASHCTL ctl;
	SetAggState *state;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	state = palloc0(sizeof(SetAggState));

	state->typ = lookup_type_cache(type,
			TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	if (type == RECORDOID || state->typ->typtype == TYPTYPE_COMPOSITE)
		elog(ERROR, "composite types are not supported by set_agg");

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(SetKey);
	ctl.entrysize = sizeof(SetKey);
	ctl.match = set_key_match;
	ctl.hash = set_key_hash;

	state->htab = hash_create("SetAggHashTable", 1024, &ctl, HASH_FUNCTION | HASH_ELEM | HASH_COMPARE);
	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) state;
}

/*
 * set_add
 */
static ArrayBuildState *
set_add(FunctionCallInfo fcinfo, MemoryContext aggcontext, Datum d, bool isnull, ArrayBuildState *state)
{
	SetKey key;
	bool found;
	SetAggState *sas;
	StringInfoData buf;
	uint64_t h;
	Datum copy;

	sas = (SetAggState *) fcinfo->flinfo->fn_extra;
	copy = datumCopy(d, sas->typ->typbyval, sas->typ->typlen);

	initStringInfo(&buf);
	DatumToBytes(copy, sas->typ, &buf);

	h = MurmurHash3_64(buf.data, buf.len, MURMUR_SEED);
	pfree(buf.data);

	MemSet(&key, 0, sizeof(SetKey));
	key.hash = h;
	key.value = copy;
	key.typ = sas->typ;

	hash_search_with_hash_value(sas->htab, &key, h, HASH_ENTER, &found);

	if (!found)
		state = accumArrayResult(state,
								 d,
								 PG_ARGISNULL(1),
								 sas->typ->type_id,
								 aggcontext);

	return state;
}

/*
 * set_agg_trans
 */
Datum
set_agg_trans(PG_FUNCTION_ARGS)
{
	ArrayBuildState *state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
	Datum incoming = PG_GETARG_DATUM(1);
	MemoryContext old;
	MemoryContext context;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "set_agg_trans called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (state == NULL)
		set_agg_startup(fcinfo, AggGetInitialArgType(fcinfo));

	state = set_add(fcinfo, context, incoming, PG_ARGISNULL(1), state);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * set_agg_combine
 */
Datum
set_agg_combine(PG_FUNCTION_ARGS)
{
	ArrayBuildState *state = PG_ARGISNULL(0) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(0);
	ArrayBuildState *incoming = PG_ARGISNULL(1) ? NULL : (ArrayBuildState *) PG_GETARG_POINTER(1);
	MemoryContext old;
	MemoryContext context;
	int i;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "set_agg_combine called in non-aggregate context");

	if (incoming == NULL)
		PG_RETURN_POINTER(state);

	old = MemoryContextSwitchTo(context);

	if (state == NULL)
		set_agg_startup(fcinfo, incoming->element_type);

	/*
	 * The incoming arrays will be appended to the existing transition state,
	 * but the order in which they're appended isn't predictable.
	 */
	for (i = 0; i < incoming->nelems; i++)
		state = set_add(fcinfo, context, incoming->dvalues[i], incoming->dnulls[i], state);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * set_agg_combine
 */
Datum
set_cardinality(PG_FUNCTION_ARGS)
{
	Datum array;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	array = DirectFunctionCall1(array_agg_finalfn, PG_GETARG_DATUM(0));;
	PG_RETURN_INT32(DirectFunctionCall2(array_length, array, Int32GetDatum(1)));
}

