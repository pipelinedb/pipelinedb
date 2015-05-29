/* Copyright (c) 2013-2015 PipelineDB */
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
#include "nodes/primnodes.h"
#include "utils/relcache.h"

extern void UpdatePipelineStreamCatalog(Relation pipeline_query);
extern Bitmapset *GetAllStreamReaders(RangeVar *stream);
extern Bitmapset *GetLocalStreamReaders(RangeVar *stream);
extern TupleDesc GetStreamTupleDesc(RangeVar *stream, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool PipelineStreamCatalogEntryExists(Oid namespace, char *stream);
extern bool RangeVarIsForStream(RangeVar *stream);

#endif
