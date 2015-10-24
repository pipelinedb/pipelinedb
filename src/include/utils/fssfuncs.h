/*-------------------------------------------------------------------------
 *
 * fssfuncs.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *	  Interface for Filtered Space Saving functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef FSSFUNCS_H
#define FSSFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum fss_print(PG_FUNCTION_ARGS);
extern Datum fss_agg_trans(PG_FUNCTION_ARGS);
extern Datum fss_agg_transp(PG_FUNCTION_ARGS);
extern Datum fss_agg_weighted(PG_FUNCTION_ARGS);
extern Datum fss_agg_weighted_trans(PG_FUNCTION_ARGS);
extern Datum fss_merge_agg_trans(PG_FUNCTION_ARGS);
extern Datum fss_topk(PG_FUNCTION_ARGS);
extern Datum fss_topk_values(PG_FUNCTION_ARGS);
extern Datum fss_topk_freqs(PG_FUNCTION_ARGS);
extern Datum fss_empty(PG_FUNCTION_ARGS);
extern Datum fss_emptyp(PG_FUNCTION_ARGS);
extern Datum fss_increment(PG_FUNCTION_ARGS);
extern Datum fss_increment_weighted(PG_FUNCTION_ARGS);

#endif
