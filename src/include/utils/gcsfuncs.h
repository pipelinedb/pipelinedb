/*-------------------------------------------------------------------------
 *
 * gcsfuncs.h

 *	  Interface for Golomb-coded Set functions
 *
 *-------------------------------------------------------------------------
 */
#ifndef GCSFUNCS_H
#define GCSFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum gcs_send(PG_FUNCTION_ARGS);
extern Datum gcs_in(PG_FUNCTION_ARGS);
extern Datum gcs_out(PG_FUNCTION_ARGS);
extern Datum gcs_agg_trans(PG_FUNCTION_ARGS);
extern Datum gcs_agg_transp(PG_FUNCTION_ARGS);
extern Datum gcs_union_agg_trans(PG_FUNCTION_ARGS);
extern Datum gcs_intersection_agg_trans(PG_FUNCTION_ARGS);
extern Datum gcs_contains(PG_FUNCTION_ARGS);

#endif
