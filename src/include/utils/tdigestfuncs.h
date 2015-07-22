/*-------------------------------------------------------------------------
 *
 * tdigestfuncs.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *	  Interface for t-digest functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef TDIGESTFUNCS_H
#define TDIGESTFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum tdigest_compress(PG_FUNCTION_ARGS);
extern Datum tdigest_print(PG_FUNCTION_ARGS);
extern Datum tdigest_agg_trans(PG_FUNCTION_ARGS);
extern Datum tdigest_agg_transp(PG_FUNCTION_ARGS);
extern Datum tdigest_merge_agg_trans(PG_FUNCTION_ARGS);
extern Datum tdigest_cdf(PG_FUNCTION_ARGS);
extern Datum tdigest_quantile(PG_FUNCTION_ARGS);
extern Datum tdigest_empty(PG_FUNCTION_ARGS);
extern Datum tdigest_emptyp(PG_FUNCTION_ARGS);
extern Datum tdigest_add(PG_FUNCTION_ARGS);
extern Datum tdigest_addn(PG_FUNCTION_ARGS);

#endif
