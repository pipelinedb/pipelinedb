/*-------------------------------------------------------------------------
 *
 * hashfuncs.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/hashfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "parser/parse_coerce.h"
#include "pipeline/miscutils.h"
#include "utils/datum.h"
#include "utils/hashfuncs.h"
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
 * ls_hash_group
 *
 * Hashes a variadic number of arguments into a single 64-bit integer in a locality sensitive manner.
 * If there is a timestamp argument present, the first such argument is used as the first 32 bits of
 * the hash value. This gives indices based off of this hash function good locality.
 */
extern Datum
ls_hash_group(PG_FUNCTION_ARGS)
{
	int i;
	int64 result = 0;
	HashGroupState *state;
	int64 tsval = 0;
	int32 hashed = 0;

	if (fcinfo->flinfo->fn_extra == NULL)
		init_hash_group(fcinfo);

	state = (HashGroupState *) fcinfo->flinfo->fn_extra;

	for (i=0; i<PG_NARGS(); i++)
	{
		Datum d = (Datum) PG_GETARG_DATUM(i);
		uint32 hash;

		/* all time-based types are based on 64-bit integers */
		if (i == state->tsarg)
			tsval = DatumGetInt64(d);

		hashed = (hashed << 1) | ((hashed & 0x80000000) ? 1 : 0);

		if (PG_ARGISNULL(i))
			continue;

		hash = DatumGetUInt32(FunctionCall1(&(state->hashfuncs[i]), d));
		hashed ^= hash;
	}

	/*
	 * [00:31] - 32 lowest-order bits from hash value
	 * [31:63] - 32 lowest-order bits of time-based value
	 */
	result = (tsval << 32) | (0xFFFFFFFF & hashed);

	PG_RETURN_INT64(result);
}

/*
 * hash_group
 *
 * Hashes a variadic number of arguments into a single 32-bit integer
 */
extern Datum
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

		result = (result << 1) | ((result & 0x80000000) ? 1 : 0);

		if (PG_ARGISNULL(i))
			continue;

		hash = DatumGetUInt32(FunctionCall1(&(state->hashfuncs[i]), d));
		result ^= hash;
	}

	PG_RETURN_INT32(result);
}
