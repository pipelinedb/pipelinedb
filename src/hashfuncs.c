/*-------------------------------------------------------------------------
 *
 * hashfuncs.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parse_coerce.h"
#include "miscutils.h"
#include "scheduler.h"
#include "utils/datum.h"
#include "hashfuncs.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

typedef struct HashGroupState
{
	/* array of hash functions for each attribute in the group */
	FmgrInfo *hashfuncs;
	/* argument position of time-based value to prefix the hash value with, if any */
	int tsarg;
} HashGroupState;

/*
 * init_hash_group
 *
 * Retrieve hash functions for each argument type in the arg list
 */
static void
init_hash_group(PG_FUNCTION_ARGS)
{
	ListCell *lc;
	FuncExpr *func;
	MemoryContext old;
	HashGroupState *state;
	int i = 0;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	func = (FuncExpr *) fcinfo->flinfo->fn_expr;

	state = palloc0(sizeof(HashGroupState));
	state->tsarg = -1;
	state->hashfuncs = palloc0(sizeof(FmgrInfo) * PG_NARGS());

	foreach(lc, func->args)
	{
		Oid type = exprType((Node *) lfirst(lc));
		TypeCacheEntry *typ = lookup_type_cache(type, TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO);

		if (TypeCategory(type) == TYPCATEGORY_DATETIME && state->tsarg == -1)
			state->tsarg = i;

		if (type == UNKNOWNOID)
			elog(ERROR, "could not determine expression type of argument %d", i + 1);

		state->hashfuncs[i] = typ->hash_proc_finfo;
		i++;
	}

	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) state;
}

/*
 * hash_combine
 *
 * This is a better hash combining function that a simple XOR. Copied from the boost C++ library.
 * http://www.boost.org/doc/libs/1_35_0/doc/html/boost/hash_combine_id241013.html
 *
 * See explanation: http://stackoverflow.com/questions/5889238/why-is-xor-the-default-way-to-combine-hashes
 */
static inline uint32
hash_combine(uint32 seed, uint32 hash)
{
	return seed ^ (hash + 0x9e3779b9 + (seed << 6) + (seed >> 2));
}

/*
 * ls_hash_group
 *
 * Hashes a variadic number of arguments into a single 64-bit integer in a locality sensitive manner.
 * If there is a timestamp argument present, the first such argument is used as the first 32 bits of
 * the hash value. This gives indices based off of this hash function good locality.
 */
PG_FUNCTION_INFO_V1(ls_hash_group);
Datum
ls_hash_group(PG_FUNCTION_ARGS)
{
	int i;
	int64 result = 0;
	HashGroupState *state;
	int64 tsval = 0;
	uint32 hashed = 0;

	if (fcinfo->flinfo->fn_extra == NULL)
		init_hash_group(fcinfo);

	state = (HashGroupState *) fcinfo->flinfo->fn_extra;

	for (i=0; i<PG_NARGS(); i++)
	{
		Datum d = (Datum) PG_GETARG_DATUM(i);
		uint32 hash;

		/* all time-based types are based on 64-bit integers */
		if (i == state->tsarg)
		{
			if (PG_ARGISNULL(i))
				tsval = 0;
			else
				tsval = DatumGetInt64(d);
		}

		if (PG_ARGISNULL(i))
			hash = 0;
		else
			hash = DatumGetUInt32(FunctionCall1(&(state->hashfuncs[i]), d));

		hashed = hash_combine(hashed, hash);
	}

	/*
	 * [00:31] - 32 lowest-order bits from hash value
	 * [31:63] - 32 lowest-order bits of time-based value
	 */
	result = (tsval << 32) | (UINT32_MAX & hashed);

	PG_RETURN_INT64(result);
}

/*
 * hash_group
 *
 * Hashes a variadic number of arguments into a single 32-bit integer
 */
PG_FUNCTION_INFO_V1(hash_group);
Datum
hash_group(PG_FUNCTION_ARGS)
{
	int i;
	uint32 result = 0;
	HashGroupState *state;

	if (fcinfo->flinfo->fn_extra == NULL)
		init_hash_group(fcinfo);

	state = (HashGroupState *) fcinfo->flinfo->fn_extra;

	for (i=0; i<PG_NARGS(); i++)
	{
		Datum d = (Datum) PG_GETARG_DATUM(i);
		uint32 hash;

		if (PG_ARGISNULL(i))
			hash = 0;
		else
			hash = DatumGetUInt32(FunctionCall1(&(state->hashfuncs[i]), d));

		result = hash_combine(result, hash);
	}

	result &= UINT32_MAX;

	PG_RETURN_INT32(result);
}


/*
 * slot_hash_group_skip_attr
 */
uint64
slot_hash_group_skip_attr(TupleTableSlot *slot, AttrNumber skip_attno, FuncExpr *hash, FunctionCallInfo fcinfo)
{
	ListCell *lc;
	Datum result;
	int i = 0;

	Assert(fcinfo);
	Assert(hash);

	foreach(lc, hash->args)
	{
		AttrNumber attno = ((Var *) lfirst(lc))->varattno;
		bool isnull;
		Datum d;

		if (attno == skip_attno)
		{
			fcinfo->argnull[i] = true;
			i++;
			continue;
		}

		d = slot_getattr(slot, attno, &isnull);
		fcinfo->arg[i] = d;
		fcinfo->argnull[i] = isnull;
		i++;
	}

	result = FunctionCallInvoke(fcinfo);

	return DatumGetInt64(result);
}
