/*-------------------------------------------------------------------------
 *
 * pipeline_stream_fn.h
 *	 prototypes for functions in catalog/pipeline_stream.c
 *
 *
 * src/include/catalog/pipeline_stream_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_STREAM_FN_H
#define PIPELINE_STREAM_FN_H

#include "postgres.h"

#include "catalog/pipeline_stream.h"
#include "nodes/bitmapset.h"

extern void UpdateStreamTargets(void);
extern Bitmapset *GetTargetsFor(const char *stream);

#endif
