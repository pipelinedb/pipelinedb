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
extern bool is_inferred_stream_relation(Relation rel);
extern bool is_inferred_stream_rte(RangeTblEntry *rte);

extern void UpdatePipelineStreamCatalog(void);

extern Bitmapset *GetAllStreamReaders(Oid relid);
extern Bitmapset *GetLocalStreamReaders(Oid relid);
extern TupleDesc GetInferredStreamTupleDesc(Oid relid, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool RangeVarIsForStream(RangeVar *stream, bool *is_inferred);
extern bool IsInferredStream(Oid relid);
extern bool IsStream(Oid relid);

extern void CreateInferredStream(RangeVar *rv);
extern void CreatePipelineStreamEntry(CreateStreamStmt *stmt, Oid relid);
extern void RemovePipelineStreamById(Oid oid);

extern Relation inferred_stream_open(ParseState *pstate, Relation rel);
extern void prepare_inferred_stream_for_insert(Relation rel, Query *query);
extern void inferred_stream_close(Relation rel);

#endif
