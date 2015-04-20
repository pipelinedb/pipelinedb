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
extern Bitmapset *GetStreamReaders(const char *stream);
extern Bitmapset *GetStreamReadersMasked(const char *stream, const char *targets);
extern TupleDesc GetStreamTupleDesc(const char *stream, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool IsStream(char *stream);
extern bool IsWritableStream(char *stream);

#endif
