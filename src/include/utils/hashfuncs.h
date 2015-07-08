/*-------------------------------------------------------------------------
 *
 * hashfuncs.h
 *
 * Copyright (c) 2013-2015, PipelineDB
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
extern Datum ls_hash_group(PG_FUNCTION_ARGS);

#endif
