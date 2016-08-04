/*-------------------------------------------------------------------------
 *
 * hashfuncs.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/utils/hashfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASHFUNCS_H
#define HASHFUNCS_H

#include "postgres.h"
#include "fmgr.h"

#define get_combiner_for_group_hash(hash) ((hash) % continuous_query_num_combiners)
#define is_group_hash_mine(hash) (get_combiner_for_group_hash(hash) == MyContQueryProc->group_id)

extern Datum hash_group(PG_FUNCTION_ARGS);
extern Datum ls_hash_group(PG_FUNCTION_ARGS);
extern uint64 slot_hash_group_skip_attr(TupleTableSlot *slot, AttrNumber sw_attno, FuncExpr *hash, FunctionCallInfo fcinfo);
#define slot_hash_group(slot, hash, fcinfo) slot_hash_group_skip_attr((slot), InvalidAttrNumber, (hash), (fcinfo))

#endif
