/*-------------------------------------------------------------------------
 *
 * kv.c
 *	  Simple key-value pair type implementation.
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "kv.h"
#include "nodes/nodeFuncs.h"
#include "nodes/primnodes.h"
#include "utils/datum.h"
#include "utils/builtins.h"
#include "utils/typcache.h"

typedef struct KeyedAggState
{
	TypeCacheEntry *key_type;
	TypeCacheEntry *value_type;
} KeyedAggState;

/*
 * point_to_self
 */
static KeyValue *
point_to_self(KeyedAggState *state, struct varlena *raw)
{
	KeyValue *kv = (KeyValue *) raw;
	char *pos = (char *) kv + sizeof(KeyValue);

	if (!state->key_type->typbyval)
	{
		kv->key = (Datum) pos;
		pos += kv->klen;
	}

	if (!state->value_type->typbyval)
	{
		kv->value = (Datum) pos;
		pos += kv->vlen;
	}

	return kv;
}

/*
 * copy_kv
 */
static KeyValue *
copy_kv(KeyedAggState *state, KeyValue *kv)
{
	Size size = sizeof(KeyValue) + kv->klen + kv->vlen;
	KeyValue *copy = palloc0(size);

	memcpy(copy, kv, size);

	return point_to_self(state, (struct varlena *) copy);
}

/*
 * compare_keys
 */
static int
compare_keys(KeyedAggState *state, KeyValue *kv, Datum incoming, bool incoming_null, Oid collation, bool *result_null)
{
	TypeCacheEntry *type = state->key_type;
	FunctionCallInfoData cmp_fcinfo;
	int result;

	if (incoming_null || KV_KEY_IS_NULL(kv))
	{
		*result_null = true;
		return 0;
	}

	InitFunctionCallInfoData(cmp_fcinfo, &type->cmp_proc_finfo, 2, collation, NULL, NULL);
	cmp_fcinfo.arg[0] = kv->key;
	cmp_fcinfo.argnull[0] = KV_KEY_IS_NULL(kv);
	cmp_fcinfo.arg[1] = incoming;
	cmp_fcinfo.argnull[1] = incoming_null;

	result = DatumGetInt32(FunctionCallInvoke(&cmp_fcinfo));
	*result_null = cmp_fcinfo.isnull;

	return result;
}

/*
 * We use our own reallocation function instead of just using repalloc because repalloc frees the old pointer.
 * This is problematic in the context of using KeyValues in aggregates because nodeAgg automatically frees the
 * old transition value when the pointer value changes within the transition function, which would lead to a
 * double free error if we were to free the old pointer ourselves via repalloc.
 */
static KeyValue *
kv_realloc(KeyValue *kv, Size size)
{
	KeyValue *result = palloc0(size);

	Assert(VARSIZE(kv));
	memcpy(result, kv, VARSIZE(kv));

	return result;
}

/*
 * set_kv
 */
static KeyValue *
set_kv(KeyedAggState *state, KeyValue *kv, Datum key, bool key_null, Datum value, bool value_null)
{
	Size klen = 0;
	Size vlen = 0;
	Size new_size;
	char *pos;

	if (!state->key_type->typbyval && !key_null)
		klen = datumGetSize(key, state->key_type->typbyval, state->key_type->typlen);

	if (!state->value_type->typbyval && !value_null)
		vlen = datumGetSize(value, state->value_type->typbyval, state->value_type->typlen);

	new_size = sizeof(KeyValue) + klen + vlen;

	if (kv == NULL)
		kv = palloc0(new_size);
	else if (VARSIZE(kv) != new_size)
		kv = kv_realloc(kv, Max(VARSIZE(kv), new_size));

	kv->klen = klen;
	kv->vlen = vlen;

	pos = (char *) kv;
	pos += sizeof(KeyValue);

	if (!key_null)
	{
		if (!state->key_type->typbyval)
		{
			kv->key = (Datum) pos;
			memcpy(pos, (char *) key, kv->klen);
			pos += kv->klen;
		}
		else
		{
			kv->key = key;
		}
	}
	else
	{
		KV_SET_KEY_NULL(kv);
	}

	if (!value_null)
	{
		if (!state->value_type->typbyval)
		{
			kv->value = (Datum ) pos;
			memcpy(pos, (char *) value, kv->vlen);
			pos += kv->vlen;
		}
		else
		{
			kv->value = value;
		}
	}
	else
	{
		KV_SET_VALUE_NULL(kv);
	}

	SET_VARSIZE(kv, new_size);

	Assert(state->key_type->type_id);
	Assert(state->value_type->type_id);

	kv->key_type = state->key_type->type_id;
	kv->value_type = state->value_type->type_id;

	return kv;
}

/*
 * keyed_trans_startup
 *
 * Get type information for the key and value based on argument types
 */
static KeyValue *
keyed_trans_startup(FunctionCallInfo fcinfo)
{
	List *args = NIL;
	KeyedAggState *state;
	Node *node;
	Oid type;
	MemoryContext old;
	KeyValue *result;

	if (AggGetAggref(fcinfo))
		args = AggGetAggref(fcinfo)->args;
	else
		elog(ERROR, "fcinfo must be an aggregate function call");

	node = linitial(args);
	type = IsA(node, TargetEntry) ? exprType((Node *) ((TargetEntry *) node)->expr) : exprType(node);

	old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	state = palloc0(sizeof(KeyedAggState));
	state->key_type = lookup_type_cache(type, TYPECACHE_CMP_PROC_FINFO);

	if (!OidIsValid(state->key_type->cmp_proc))
		elog(ERROR, "could not determine key type");

	node = lsecond(args);
	type = IsA(node, TargetEntry) ? exprType((Node *) ((TargetEntry *) node)->expr) : exprType(node);

	state->value_type = lookup_type_cache(type, 0);
	fcinfo->flinfo->fn_extra = state;
	MemoryContextSwitchTo(old);

	result = set_kv(state, NULL, PG_GETARG_DATUM(1), PG_ARGISNULL(1), PG_GETARG_DATUM(2), PG_ARGISNULL(2));
	result->key_type = state->key_type->type_id;
	result->value_type = state->value_type->type_id;
	result->key_collation = PG_GET_COLLATION();

	return result;
}

/*
 * keyed_min_trans_internal
 */
static Datum
keyed_min_max_trans_internal(FunctionCallInfo fcinfo, int sign)
{
	KeyValue *kv;
	Datum incoming_key = PG_GETARG_DATUM(1);
	Datum incoming_value = PG_GETARG_DATUM(2);
	KeyedAggState *state;
	MemoryContext old;
	MemoryContext context;
	bool isnull;
	int cmp;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "keyed_min_max_trans_internal called in non-aggregate context");

	old = MemoryContextSwitchTo(context);

	if (PG_ARGISNULL(0))
	{
		kv = keyed_trans_startup(fcinfo);
		MemoryContextSwitchTo(old);
		PG_RETURN_POINTER(kv);
	}

	state = (KeyedAggState *) fcinfo->flinfo->fn_extra;
	kv = point_to_self(state, PG_GETARG_BYTEA_P(0));

	cmp = sign * compare_keys(state, kv, incoming_key,
			PG_ARGISNULL(1), PG_GET_COLLATION(), &isnull);

	if (!isnull && cmp <= 0)
		kv = set_kv(state, kv, incoming_key, PG_ARGISNULL(1), incoming_value, PG_ARGISNULL(2));

	MemoryContextSwitchTo(old);

	Assert(kv->key_type);
	Assert(kv->value_type);
	Assert(VARSIZE(kv));

	PG_RETURN_POINTER(kv);
}

/*
 * keyed_min_max_combine_internal
 */
static Datum
keyed_min_max_combine_internal(FunctionCallInfo fcinfo, int sign)
{
	KeyedAggState *kas;
	KeyValue *state;
	KeyValue *incoming = (KeyValue *) PG_GETARG_VARLENA_P(1);
	MemoryContext old;
	MemoryContext context;
	int cmp;
	bool isnull;

	if (!AggCheckCallContext(fcinfo, &context))
		elog(ERROR, "keyed_min_max_combine_internal called in non-aggregate context");

	if (PG_ARGISNULL(0))
	{
		/*
		 * We can't use the startup function that the aggregate uses because
		 * the combiner Aggref doesn't have all of the original arguments.
		 */
		old = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
		kas = palloc0(sizeof(KeyedAggState));
		kas->key_type = lookup_type_cache(incoming->key_type, TYPECACHE_CMP_PROC_FINFO);
		kas->value_type = lookup_type_cache(incoming->value_type, 0);
		fcinfo->flinfo->fn_extra = kas;
		MemoryContextSwitchTo(old);

		old = MemoryContextSwitchTo(context);
		state = copy_kv(kas, incoming);
		MemoryContextSwitchTo(old);

		PG_RETURN_POINTER(state);
	}

	old = MemoryContextSwitchTo(context);

	state = (KeyValue *) PG_GETARG_VARLENA_P(0);
	kas = (KeyedAggState *) fcinfo->flinfo->fn_extra;
	incoming = point_to_self(kas, (struct varlena *) incoming);

	cmp = sign * compare_keys(kas, state, incoming->key,
			KV_KEY_IS_NULL(incoming), state->key_collation, &isnull);

	if (!isnull && cmp <= 0)
		state = copy_kv(kas, incoming);

	MemoryContextSwitchTo(old);

	PG_RETURN_POINTER(state);
}

/*
 * keyed_min_combine
 */
PG_FUNCTION_INFO_V1(keyed_min_combine);
Datum
keyed_min_combine(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(keyed_min_max_combine_internal(fcinfo, -1));
}

/*
 * keyed_max_combine
 */
PG_FUNCTION_INFO_V1(keyed_max_combine);
Datum
keyed_max_combine(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(keyed_min_max_combine_internal(fcinfo, 1));
}

/*
 * keyed_min_trans
 */
PG_FUNCTION_INFO_V1(keyed_min_trans);
Datum
keyed_min_trans(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(keyed_min_max_trans_internal(fcinfo, -1));
}

/*
 * keyed_max_trans
 */
PG_FUNCTION_INFO_V1(keyed_max_trans);
Datum
keyed_max_trans(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(keyed_min_max_trans_internal(fcinfo, 1));
}

/*
 * keyed_min_max_finalize
 */
PG_FUNCTION_INFO_V1(keyed_min_max_finalize);
Datum
keyed_min_max_finalize(PG_FUNCTION_ARGS)
{
	KeyValue *kv;
	KeyedAggState *kas;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	kv = (KeyValue *) PG_GETARG_VARLENA_P(0);
	if (KV_VALUE_IS_NULL(kv))
		PG_RETURN_NULL();

	kas = palloc0(sizeof(KeyedAggState));
	kas->key_type = lookup_type_cache(kv->key_type, TYPECACHE_CMP_PROC_FINFO);
	kas->value_type = lookup_type_cache(kv->value_type, 0);

	kv = point_to_self(kas, (struct varlena *) kv);

	PG_RETURN_POINTER(kv->value);
}
