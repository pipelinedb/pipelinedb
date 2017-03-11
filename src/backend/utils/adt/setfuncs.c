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

typedef struct ExclusiveSetAggState
{
	TypeCacheEntry *type;
	List *named_sets;
} ExclusiveSetAggState;

/*
 * create_set
 */
static NamedSet *
create_set(ExclusiveSetAggState *state, char *name)
{
	HASHCTL hctl;
	NamedSet *set = palloc0(sizeof(NamedSet));
	set->name = pstrdup(name);

	MemSet(&hctl, 0, sizeof(hctl));
	hctl.hcxt = CurrentMemoryContext;
	hctl.keysize = sizeof(uint32);
	hctl.entrysize = sizeof(uint32);

	set->htab = hash_create(name, 1024, &hctl, HASH_CONTEXT | HASH_ELEM);
	state->named_sets = lappend(state->named_sets, set);

	return set;
}

/*
 * exclusive_set_agg_state_send
 */
Datum
exclusive_set_cardinality_agg_state_send(PG_FUNCTION_ARGS)
{
	ExclusiveSetAggState *state = (ExclusiveSetAggState *) PG_GETARG_POINTER(0);
	ListCell *lc;
	StringInfoData buf;

	Assert(state);

	pq_begintypsend(&buf);

	/* Magic header */
	pq_sendint(&buf, MAGIC_HEADER, 4);

	/* Number of sets in this agg state */
	pq_sendint(&buf, list_length(state->named_sets), sizeof(int));

	foreach(lc, state->named_sets)
	{
		HASH_SEQ_STATUS iter;
		NamedSet *set = (NamedSet *) lfirst(lc);
		int num_hashes = hash_get_num_entries(set->htab);
		int bytes;
		uint32 *hash;

		if (set->hashes)
		{
			bytes = sizeof(uint32) * set->num_hashes;
		}
		else
		{
			int i = 0;

			bytes = sizeof(uint32) * num_hashes;
			set->hashes = palloc0(bytes);
			set->num_hashes = num_hashes;

			hash_seq_init(&iter, set->htab);
			while ((hash = (uint32 *) hash_seq_search(&iter)) != NULL)
				set->hashes[i++] = *hash;
		}

		/* Name of this set */
		pq_sendstring(&buf, set->name);

		Assert(set->hashes);

		/* Length of the array of hashes */
		pq_sendint(&buf, set->num_hashes, sizeof(int));

		/* Array of unique hashes */
		pq_sendbytes(&buf, (char *) set->hashes, bytes);
	}

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

/*
 * exclusive_set_agg_state_recv
 */
Datum
exclusive_set_cardinality_agg_state_recv(PG_FUNCTION_ARGS)
{
	bytea *bytes;
	StringInfoData buf;
	ExclusiveSetAggState *result;
	MemoryContext context;
	MemoryContext old;
	int nbytes;
	int nsets;
	int i;

	if (!AggCheckCallContext(fcinfo, &context))
		context = CurrentMemoryContext;

	old = MemoryContextSwitchTo(context);
	bytes = PG_GETARG_BYTEA_P(0);
	result = palloc0(sizeof(ExclusiveSetAggState));
	nbytes = VARSIZE(bytes);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed exclusive_set_agg transition state received");

	nsets = pq_getmsgint(&buf, sizeof(int));

	for (i = 0; i < nsets; i++)
	{
		char *set_name = (char *) pq_getmsgstring(&buf);
		int num_hashes = pq_getmsgint(&buf, sizeof(int));
		int size = num_hashes * sizeof(uint32);
		NamedSet *set;

		set = create_set(result, set_name);

		/*
		 * We're just going to combine all of these hashes with the existing state, so there's
		 * no need to load them into a hashtable
		 */
		set->hashes = palloc0(size);
		set->num_hashes = num_hashes;
		memcpy(set->hashes, pq_getmsgbytes(&buf, size), size);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(result);
}

/*
 * lookup_set_element_type
 */
static void
lookup_set_element_type(FunctionCallInfo fcinfo)
{
	MemoryContext old;
	SetAggState *state;
	Oid type = AggGetInitialArgType(fcinfo);

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	state = palloc0(sizeof(SetAggState));

	state->has_null = false;
	state->typ = lookup_type_cache(type,
			TYPECACHE_HASH_PROC | TYPECACHE_HASH_PROC_FINFO | TYPECACHE_CMP_PROC | TYPECACHE_CMP_PROC_FINFO);

	if (type == RECORDOID || state->typ->typtype == TYPTYPE_COMPOSITE)
		elog(ERROR, "composite types are not supported by exclusive_set_agg");

	MemoryContextSwitchTo(old);

	fcinfo->flinfo->fn_extra = (void *) state;
}

/*
 * get_named_set
 */
static NamedSet *
get_named_set(ExclusiveSetAggState *state, char *set_name)
{
	ListCell *lc;
	NamedSet *result;

	foreach(lc, state->named_sets)
	{
		result = (NamedSet *) lfirst(lc);
		Assert(result->name);
		if (pg_strcasecmp(result->name, set_name) == 0)
			return result;
	}

	return NULL;
}

/*
 * add_exclusive_set_hash
 */
static void
add_exclusive_set_hash(ExclusiveSetAggState *state, char *set_name, uint32 hash, MemoryContext set_context)
{
	bool found;
	NamedSet *set;
	ListCell *lc;

	set = get_named_set(state, set_name);
	if (!set)
	{
		MemoryContext old;

		old = MemoryContextSwitchTo(set_context);
		set = create_set(state, set_name);
		MemoryContextSwitchTo(old);
	}

	Assert(set);

	/*
	 * Attempt to remove this hash from existing sets until we either get a removal or determine
	 * that it never previously existed in any set
	 */
	foreach(lc, state->named_sets)
	{
		NamedSet *other_set = (NamedSet *) lfirst(lc);

		if (pg_strcasecmp(set_name, other_set->name) == 0)
			continue;

		hash_search_with_hash_value(other_set->htab, &hash, hash, HASH_REMOVE, &found);
		if (found)
			break;
	}

	/*
	 * The entire entry is just the Datum's 32-bit hash, so this is all we need to do to properly create the entry
	 */
	hash_search_with_hash_value(set->htab, &hash, hash, HASH_ENTER, &found);
}

/*
 * exclusive_set_agg_trans
 */
Datum
exclusive_set_cardinality_agg_trans(PG_FUNCTION_ARGS)
{
	SetAggState *sas;
	MemoryContext old;
	MemoryContext context;
	ExclusiveSetAggState *state = PG_ARGISNULL(0) ? NULL : (ExclusiveSetAggState *) PG_GETARG_POINTER(0);
	Datum elem;
	char *set_name;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "exclusive_set_cardinality_agg_trans called in non-aggregate context");

	if (PG_ARGISNULL(2))
		elog(ERROR, "set name cannot be NULL");

	old = MemoryContextSwitchTo(context);

	if (fcinfo->flinfo->fn_extra == NULL)
		lookup_set_element_type(fcinfo);

	Assert(fcinfo->flinfo->fn_extra);
	sas = (SetAggState *) fcinfo->flinfo->fn_extra;

	if (state == NULL)
	{
		state = palloc0(sizeof(ExclusiveSetAggState));
		state->type = sas->typ;
		state->named_sets = NIL;
	}

	/* NULLs are currently just a noop */
	if (!PG_ARGISNULL(1))
	{
		uint32 hash;

		elem = PG_GETARG_DATUM(1);
		set_name = TextDatumGetCString((text *) PG_GETARG_VARLENA_P(2));

		hash = DatumGetUInt32(FunctionCall1(&sas->typ->hash_proc_finfo, elem));
		add_exclusive_set_hash(state, set_name, hash, fcinfo->flinfo->fn_mcxt);
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * exclusive_set_agg_combine
 */
Datum
exclusive_set_cardinality_agg_combine(PG_FUNCTION_ARGS)
{
	ExclusiveSetAggState *state;
	ExclusiveSetAggState *incoming;
	MemoryContext context;
	MemoryContext old;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "exclusive_set_cardinality_agg_combine called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (!PG_ARGISNULL(0))
	{
		state = (ExclusiveSetAggState *) PG_GETARG_POINTER(0);
	}
	else
	{
		state = palloc0(sizeof(ExclusiveSetAggState));
		state->named_sets = NIL;
	}

	if (!PG_ARGISNULL(1))
	{
		ListCell *lc;

		incoming = (ExclusiveSetAggState *) PG_GETARG_POINTER(1);

		foreach(lc, incoming->named_sets)
		{
			NamedSet *set = (NamedSet *) lfirst(lc);
			int i;

			Assert(set->hashes);
			Assert(set->num_hashes);

			for (i = 0; i < set->num_hashes; i++)
				add_exclusive_set_hash(state, set->name, set->hashes[i], fcinfo->flinfo->fn_mcxt);
		}
	}

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

Datum
exclusive_set_cardinality_agg_final(PG_FUNCTION_ARGS)
{
	ExclusiveSetAggState *state = (ExclusiveSetAggState *) PG_GETARG_POINTER(0);

	bytea *bytes = (bytea *) DirectFunctionCall1(exclusive_set_cardinality_agg_state_send, (Datum) state);

	PG_RETURN_BYTEA_P(bytes);
}

/*
 * exclusive_set_cardinality
 */
Datum
exclusive_set_cardinality(PG_FUNCTION_ARGS)
{
	bytea *bytes;
	char *set_name;
	StringInfoData buf;
	int nbytes;
	int nsets;
	int i;

	if (PG_ARGISNULL(1))
		elog(ERROR, "set name cannot be NULL");

	set_name = TextDatumGetCString(PG_GETARG_VARLENA_P(1));
	bytes = PG_GETARG_BYTEA_P(0);

	nbytes = VARSIZE(bytes);

	initStringInfo(&buf);
	appendBinaryStringInfo(&buf, VARDATA(bytes), nbytes);

	if (pq_getmsgint(&buf, 4) != MAGIC_HEADER)
		elog(ERROR, "malformed exclusive_set_agg transition state received");

	nsets = pq_getmsgint(&buf, sizeof(int));

	for (i = 0; i < nsets; i++)
	{
		char *sname = (char *) pq_getmsgstring(&buf);
		int num_hashes = pq_getmsgint(&buf, sizeof(int));
		int size = num_hashes * sizeof(uint32);

		if (pg_strcasecmp(set_name, sname) == 0)
			PG_RETURN_INT64(num_hashes);

		/* consume */
		pq_getmsgbytes(&buf, size);
	}

	PG_RETURN_NULL();
}
