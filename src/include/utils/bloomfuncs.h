/*-------------------------------------------------------------------------
 *
 * bloomfuncs.h

 *	  Interface for Bloom Filter functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFUNCS_H
#define BLOOMFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum bloom_in(PG_FUNCTION_ARGS);
extern Datum bloom_out(PG_FUNCTION_ARGS);
extern Datum bloom_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_agg_transp(PG_FUNCTION_ARGS);
extern Datum bloom_union_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_intersection_agg_trans(PG_FUNCTION_ARGS);
extern Datum bloom_cardinality(PG_FUNCTION_ARGS);
extern Datum bloom_contains(PG_FUNCTION_ARGS);
extern Datum bloom_combine(PG_FUNCTION_ARGS);

#endif
