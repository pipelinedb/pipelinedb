/*-------------------------------------------------------------------------
 *
 * freqfuncs.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FREQHFUNCS_H
#define FREQHFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum cmsketch_print(PG_FUNCTION_ARGS);
extern Datum cmsketch_agg_trans(PG_FUNCTION_ARGS);
extern Datum cmsketch_agg_transp(PG_FUNCTION_ARGS);
extern Datum cmsketch_merge_agg_trans(PG_FUNCTION_ARGS);
extern Datum cmsketch_frequency(PG_FUNCTION_ARGS);
extern Datum cmsketch_total(PG_FUNCTION_ARGS);
extern Datum cmsketch_norm_frequency(PG_FUNCTION_ARGS);
extern Datum cmsketch_empty(PG_FUNCTION_ARGS);
extern Datum cmsketch_emptyp(PG_FUNCTION_ARGS);
extern Datum cmsketch_add(PG_FUNCTION_ARGS);
extern Datum cmsketch_addn(PG_FUNCTION_ARGS);

#endif
