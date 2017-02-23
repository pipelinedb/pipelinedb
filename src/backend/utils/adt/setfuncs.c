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

typedef struct SetAggState
{
	TypeCacheEntry *typ;
	bool has_null;
} SetAggState;

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

