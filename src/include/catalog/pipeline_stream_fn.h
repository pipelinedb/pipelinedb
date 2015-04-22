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
#include "utils/relcache.h"

extern void UpdateStreamReaders(Relation pipeline_query);
extern void UpdateStreamQueries(Relation pipeline_query);
extern Bitmapset *GetAllStreamReaders(const char *stream);
extern Bitmapset *GetLocalStreamReaders(const char *stream);
extern TupleDesc GetStreamTupleDesc(const char *stream, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool IsStream(char *stream);
extern bool IsWritableStream(char *stream);

#endif
