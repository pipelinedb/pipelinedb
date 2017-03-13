/*-------------------------------------------------------------------------
 *
 * set.h
 *		Set functions interface
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef SETFUNCS_H
#define SETFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum set_agg_trans(PG_FUNCTION_ARGS);
extern Datum set_agg_combine(PG_FUNCTION_ARGS);
extern Datum set_cardinality(PG_FUNCTION_ARGS);
extern Datum bucket_agg_trans(PG_FUNCTION_ARGS);
extern Datum bucket_agg_combine(PG_FUNCTION_ARGS);
extern Datum bucket_agg_final(PG_FUNCTION_ARGS);
extern Datum bucket_cardinality(PG_FUNCTION_ARGS);
extern Datum bucket_agg_state_send(PG_FUNCTION_ARGS);
extern Datum bucket_agg_state_recv(PG_FUNCTION_ARGS);
extern Datum bucket_cardinalities(PG_FUNCTION_ARGS);
extern Datum bucket_ids(PG_FUNCTION_ARGS);

#endif
