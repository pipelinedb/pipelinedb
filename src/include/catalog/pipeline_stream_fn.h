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
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_node.h"
#include "utils/relcache.h"

#define PIPELINE_STREAM_SERVER "pipeline_streams"

extern bool is_stream_relid(Oid relid);

extern void UpdatePipelineStreamCatalog(void);

extern Bitmapset *GetAllStreamReaders(Oid relid);
extern Bitmapset *GetLocalStreamReaders(Oid relid);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool RangeVarIsForStream(RangeVar *stream);
extern bool IsStream(Oid relid);

extern void CreatePipelineStreamEntry(CreateStreamStmt *stmt, Oid relid);
extern void RemovePipelineStreamById(Oid oid);

#endif
