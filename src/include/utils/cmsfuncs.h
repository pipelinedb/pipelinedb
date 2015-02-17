/*-------------------------------------------------------------------------
 *
 * cmsfuncs.h

 *	  Interface for Count-Min Sketch functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef CMSFUNCS_H
#define CMSFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum cms_in(PG_FUNCTION_ARGS);
extern Datum cms_out(PG_FUNCTION_ARGS);
extern Datum cms_agg_trans(PG_FUNCTION_ARGS);
extern Datum cms_agg_transp(PG_FUNCTION_ARGS);
extern Datum cms_merge_agg_trans(PG_FUNCTION_ARGS);
extern Datum cms_count(PG_FUNCTION_ARGS);

#endif
