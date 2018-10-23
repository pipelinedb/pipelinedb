/*-------------------------------------------------------------------------
 *
 * pipeline_stream.h
 * 		Interface for pipelinedb.stream catalog
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PIPELINE_STREAM_H
#define PIPELINE_STREAM_H

#include "postgres.h"

#include "nodes/parsenodes.h"
#include "storage/lockdefs.h"
#include "utils/relcache.h"

#define PIPELINEDB_SERVER "pipelinedb"
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

extern Oid PipelineStreamRelationOid;

extern char *stream_targets;

typedef struct FormData_pipeline_stream
{
	Oid relid;
#ifdef CATALOG_VARLEN
	bytea queries;
#endif
} FormData_pipeline_stream;

typedef FormData_pipeline_stream *Form_pipeline_stream;

#define Natts_pipeline_stream			2
#define Anum_pipeline_stream_relid		1
#define Anum_pipeline_stream_queries 	2

#define OSREL_OLD_ROW   "old"
#define OSREL_NEW_ROW   "new"
#define OSREL_DELTA_ROW "delta"

extern void SyncPipelineStreamReaders(void);

extern void SyncPipelineStream(void);

extern Bitmapset *GetAllStreamReaders(Oid relid);
extern Bitmapset *GetLocalStreamReaders(Oid relid);

extern bool RangeVarIsStream(RangeVar *rv, bool missing_ok);

extern bool RelidIsStream(Oid relid);
extern bool RelidIsOutputStream(Oid relid);


extern Relation OpenPipelineStream(LOCKMODE mode);
extern void ClosePipelineStream(Relation ps, LOCKMODE mode);
extern void transformCreateStreamStmt(CreateForeignTableStmt *stmt);
extern void CreatePipelineStreamEntry(CreateForeignTableStmt *stmt, Oid relid);

#endif
