/*-------------------------------------------------------------------------
 *
 * pipeline_database.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_database.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_database.h"
#include "catalog/pipeline_database.h"
#include "catalog/pipeline_database_fn.h"
#include "pipeline/cont_scheduler.h"
#include "storage/lock.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

/*
 * CreatePipelineDatabaseCatalogEntry
 */
void
CreatePipelineDatabaseCatalogEntry(Oid dbid)
{
	Relation pipeline_database = heap_open(PipelineDatabaseRelationId, RowExclusiveLock);
	Datum values[Natts_pipeline_database];
	bool nulls[Natts_pipeline_database];
	HeapTuple tup;

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_database_dbid - 1] = ObjectIdGetDatum(dbid);
	values[Anum_pipeline_database_cq_enabled - 1] = BoolGetDatum(continuous_queries_enabled);

	tup = heap_form_tuple(pipeline_database->rd_att, values, nulls);
	simple_heap_insert(pipeline_database, tup);
	CatalogUpdateIndexes(pipeline_database, tup);

	CommandCounterIncrement();

	heap_close(pipeline_database, NoLock);
}


/*
 * RemovePipelineDatabaseByDbId
 */
void
RemovePipelineDatabaseByDbId(Oid dbid)
{
	Relation pipeline_database;
	HeapTuple tuple;

	pipeline_database = heap_open(PipelineDatabaseRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(PIPELINEDATABASEDBID, ObjectIdGetDatum(dbid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for pipeline_database entry with dbid %u", dbid);

	simple_heap_delete(pipeline_database, &tuple->t_self);

	ReleaseSysCache(tuple);

	CommandCounterIncrement();

	heap_close(pipeline_database, RowExclusiveLock);
}
