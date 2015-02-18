/*-------------------------------------------------------------------------
 *
 * cmsketchfuncs.h

 *	  Interface for Count-Min Sketch functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef CMSKETCHFUNCS_H
#define CMSKETCHFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum cmsketch_in(PG_FUNCTION_ARGS);
extern Datum cmsketch_out(PG_FUNCTION_ARGS);
extern Datum cmsketch_agg_trans(PG_FUNCTION_ARGS);
extern Datum cmsketch_agg_transp(PG_FUNCTION_ARGS);
extern Datum cmsketch_merge_agg_trans(PG_FUNCTION_ARGS);
extern Datum cmsketch_count(PG_FUNCTION_ARGS);

#endif
