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

extern Bitmapset *GetAllStreamReaders(Oid namespace, char *stream);
extern Bitmapset *GetLocalStreamReaders(Oid namespace, char *stream);
extern TupleDesc GetStreamTupleDesc(Oid namespace, char *stream, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool IsStream(Oid namespace, char *stream);
extern bool RangeVarIsForStream(RangeVar *stream);
extern bool RangeVarIsForTypedStream(RangeVar *rv);
extern Oid GetDefaultStreamNamespace(char *stream);
extern Oid GetStreamRelId(RangeVar *stream);
extern Oid GetStreamNamespace(Oid stream_relid);

#endif
