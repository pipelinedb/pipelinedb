/*-------------------------------------------------------------------------
 *
 * hashfuncs.h
 *
 * IDENTIFICATION
 *	  src/include/utils/hashfuncs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef HASHFUNCS_H
#define HASHFUNCS_H

#include "postgres.h"
#include "fmgr.h"

extern Datum hash_group(PG_FUNCTION_ARGS);

#endif
