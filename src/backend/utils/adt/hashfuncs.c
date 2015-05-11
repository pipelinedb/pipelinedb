/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * hashfuncs.c
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
#include "pipeline/miscutils.h"
#include "utils/datum.h"
#include "utils/hashfuncs.h"
#include "utils/memutils.h"
#include "utils/typcache.h"

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
	FmgrInfo *hashfuncs;
	int i = 0;

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	func = (FuncExpr *) fcinfo->flinfo->fn_expr;
	hashfuncs = palloc0(sizeof(FmgrInfo) * PG_NARGS());

	foreach(lc, func->args)
	{
		Oid type = exprType((Node *) lfirst(lc));
		TypeCacheEntry *typ = lookup_type_cache(type, TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO);

		if (type == UNKNOWNOID)
			elog(ERROR, "could not determine expression type of argument %d", i + 1);

		hashfuncs[i] = typ->hash_proc_finfo;
		i++;
	}

	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) hashfuncs;
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
	FmgrInfo *hashfuncs;

	if (fcinfo->flinfo->fn_extra == NULL)
		init_hash_group(fcinfo);

	hashfuncs = (FmgrInfo *) fcinfo->flinfo->fn_extra;

	for (i=0; i<PG_NARGS(); i++)
	{
		Datum d = (Datum) PG_GETARG_DATUM(i);
		uint32 hash;

		result = (result << 1) | ((result & 0x80000000) ? 1 : 0);

		if (PG_ARGISNULL(i))
			continue;

		hash = DatumGetUInt32(FunctionCall1(&hashfuncs[i], d));
		result ^= hash;
	}

	PG_RETURN_INT32(result);
}
