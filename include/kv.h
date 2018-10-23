/*-------------------------------------------------------------------------
 *
 * kv.h
 *	  Simple key-value pair type implementation.
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef KV_H
#define KV_H

#include "c.h"

#include "fmgr.h"

typedef struct KeyValue
{
	uint32 vl_len_;
	char flags;
	int klen;
	int vlen;
	Oid key_collation;
	Oid key_type;
	Oid value_type;
	Datum key;
	Datum value;
} KeyValue;

#define KV_KEY_NULL 0x1
#define KV_VALUE_NULL 0x2

#define KV_SET_KEY_NULL(kv) ((kv)->flags |= KV_KEY_NULL)
#define KV_KEY_IS_NULL(kv) ((kv)->flags & KV_KEY_NULL)

#define KV_SET_VALUE_NULL(kv) ((kv)->flags |= KV_VALUE_NULL)
#define KV_VALUE_IS_NULL(kv) ((kv)->flags & KV_VALUE_NULL)

Datum keyed_min_trans(PG_FUNCTION_ARGS);
Datum keyed_max_trans(PG_FUNCTION_ARGS);
Datum keyed_min_max_finalize(PG_FUNCTION_ARGS);
Datum keyed_min_combine(PG_FUNCTION_ARGS);
Datum keyed_max_combine(PG_FUNCTION_ARGS);

#endif
