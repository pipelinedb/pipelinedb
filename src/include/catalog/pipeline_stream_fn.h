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

#define needs_mock_relation(rel) ((rel)->rd_rel->relkind == RELKIND_STREAM && IsInferredStream((rel)->rd_id))

extern void UpdatePipelineStreamCatalog(Relation pipeline_query);

extern Bitmapset *GetAllStreamReaders(Oid relid);
extern Bitmapset *GetLocalStreamReaders(Oid relid);
extern TupleDesc GetInferredStreamTupleDesc(Oid relid, List *colnames);

extern bytea *PackTupleDesc(TupleDesc desc);
extern TupleDesc UnpackTupleDesc(bytea *bytes);

extern bool RangeVarIsForStream(RangeVar *stream, bool *is_inferred);
extern bool IsInferredStream(Oid relid);

extern void CreateInferredStream(RangeVar *rv);
extern void CreatePipelineStreamEntry(CreateStreamStmt *stmt, Oid relid);
extern void RemovePipelineStreamById(Oid oid);

extern Relation mock_relation_open(ParseState *pstate, Relation rel);
extern void mock_relation_close(Relation rel);

#endif
