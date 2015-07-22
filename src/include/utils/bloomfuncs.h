/*-------------------------------------------------------------------------
 *
 * bloomfuncs.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *	  Interface for Bloom Filter functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFUNCS_H
#define BLOOMFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum bloom_print(PG_FUNCTION_ARGS);
extern Datum bloom_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_agg_transp(PG_FUNCTION_ARGS);
extern Datum bloom_union_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_intersection_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_cardinality(PG_FUNCTION_ARGS);
extern Datum bloom_contains(PG_FUNCTION_ARGS);
extern Datum bloom_empty(PG_FUNCTION_ARGS);
extern Datum bloom_emptyp(PG_FUNCTION_ARGS);
extern Datum bloom_add(PG_FUNCTION_ARGS);

#endif
