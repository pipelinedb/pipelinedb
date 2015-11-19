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

#include "utils/array.h"
#include "utils/hsearch.h"
#include "utils/setfuncs.h"
#include "utils/typcache.h"

typedef struct SetKey
{
	Datum d;
	FmgrInfo *hash_proc_finfo;
	FmgrInfo *cmp_proc_finfo;
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

	Assert(l->cmp_proc_finfo == r->cmp_proc_finfo);
	return DatumGetInt32(FunctionCall2(l->cmp_proc_finfo, l->d, r->d));
}

/*
 * datum_hash
 */
static uint32
set_key_hash(const void *key, Size keysize)
{
	SetKey *k = (SetKey *) key;
	return DatumGetUInt32(FunctionCall1(k->hash_proc_finfo, k->d));
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
 * set_agg_trans
 */
Datum
set_agg_trans(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	SetKey key;
	Datum incoming = PG_GETARG_DATUM(1);
	bool found;
	SetAggState *sas;
	FmgrInfo *hash_opr;
	FmgrInfo *cmp_opr;
	int offset = 0;
	char *pos;

	if (PG_ARGISNULL(0))
	{
		Oid type = AggGetInitialArgType(fcinfo);

		set_agg_startup(fcinfo, type);
		state = construct_empty_array(type);
	}
	else
	{
		state = (ArrayType *) PG_GETARG_POINTER(0);
	}

	sas = (SetAggState *) fcinfo->flinfo->fn_extra;
	hash_opr = &sas->typ->hash_proc_finfo;
	cmp_opr = &sas->typ->cmp_proc_finfo;

	MemSet(&key, 0, sizeof(SetKey));
	pos = (char *) &key;

	memcpy(pos, &incoming, sizeof(Datum));
	offset += sizeof(Datum);
	memcpy(pos + offset, &hash_opr, sizeof(FmgrInfo *));
	offset += sizeof(FmgrInfo *);
	memcpy(pos + offset, &cmp_opr, sizeof(FmgrInfo *));

	hash_search(sas->htab, &key, HASH_ENTER, &found);

	if (!found)
	{
		int index = ArrayGetNItems(ARR_NDIM(state), ARR_DIMS(state));
		state = array_set(state, 1, &index, incoming, false, -1,
				sas->typ->typlen, sas->typ->typbyval, sas->typ->typalign);
	}

	PG_RETURN_POINTER(state);
}

/*
 * set_agg_combine
 */
Datum
set_agg_combine(PG_FUNCTION_ARGS)
{
	ArrayType *state;
	ArrayType *incoming = PG_GETARG_VARLENA_P(1);

	if (PG_ARGISNULL(0))
	{
		set_agg_startup(fcinfo, ARR_ELEMTYPE(incoming));
		PG_RETURN_POINTER(incoming);
	}

	// for each

	PG_RETURN_NULL();
}

/*
 * set_agg_combine
 */
Datum
set_cardinality(PG_FUNCTION_ARGS)
{
	ArrayType *state;

	if (PG_ARGISNULL(0))
		PG_RETURN_INT64(0);

	state = (ArrayType *) PG_GETARG_POINTER(0);

	PG_RETURN_INT64(ArrayGetNItems(ARR_NDIM(state), ARR_DIMS(state)));
}

