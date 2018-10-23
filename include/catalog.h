/*-------------------------------------------------------------------------
 *
 * catalog.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_CATALOG_H
#define PIPELINE_CATALOG_H

#include "access/attnum.h"
#include "access/htup.h"
#include "utils/relcache.h"

enum PipelineSysCacheIdentifier
{
	PIPELINEQUERYID,
	PIPELINEQUERYRELID,
	PIPELINEQUERYDEFRELID,
	PIPELINEQUERYMATRELID,
	PIPELINEQUERYOSRELID,
	PIPELINEQUERYSEQRELID,
	PIPELINEQUERYPKIDXID,
	PIPELINEQUERYLOOKUPIDXID,
	PIPELINESTREAMRELID,
	PIPELINESTREAMOID,
	PGAGGCOMBINEFN,
	PGAGGPARTIALCOMBINEFN,
	PIPELINECOMBINEAGGFN
};

extern void InitPipelineCatalog(void);
extern void PipelineCatalogInvalidate(int id);
extern void PipelineCatalogInvalidateAll(void);

extern Oid PipelineCatalogTupleInsert(Relation rel, HeapTuple tup);
extern void PipelineCatalogTupleUpdate(Relation rel, ItemPointer otid, HeapTuple tup);
extern void PipelineCatalogTupleDelete(Relation rel, ItemPointer tid);
extern HeapTuple PipelineCatalogLookup(int id, int nkeys, ...);
extern HeapTuple PipelineCatalogLookupForUpdate(Relation rel, int id, Datum key);

extern Datum PipelineCatalogGetAttr(int cacheId, HeapTuple tup, AttrNumber attr, bool *isnull);

extern Oid GetHashGroupOid(void);
extern  Oid GetLSHashGroupOid(void);
extern Oid GetInsertIntoStreamOid(void);

extern Oid GetPipelineQueryOid(void);
extern Oid GetPipelineCombineOid(void);
extern Oid GetPipelineStreamOid(void);
extern Oid GetPipelineExecLockOid(void);
extern Oid GetDeserializeOid(void);
extern bool CreateExtensionIsForPipelineDB(Node *node);

#endif
