/*-------------------------------------------------------------------------
 *
 * hllfuncs.h

 *	  Interface for HyperLogLog-based functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef HLLFUNCS_H
#define HLLFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum hll_print(PG_FUNCTION_ARGS);
extern Datum hll_agg_trans(PG_FUNCTION_ARGS);
extern Datum hll_agg_transp(PG_FUNCTION_ARGS);
extern Datum hll_union_agg_trans(PG_FUNCTION_ARGS);
extern Datum hll_cardinality(PG_FUNCTION_ARGS);
extern Datum hll_empty(PG_FUNCTION_ARGS);
extern Datum hll_emptyp(PG_FUNCTION_ARGS);
extern Datum hll_add(PG_FUNCTION_ARGS);

#endif
