/*-------------------------------------------------------------------------
 *
 * pipeline_tstate.c
 *	  routines to support manipulation of the pipeline_tstate relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_tstate.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pipeline_tstate.h"
#include "catalog/pipeline_tstate_fn.h"
#include "pipeline/bloom.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"

void
CreateTStateEntry(Oid id)
{
	Relation pipeline_tstate;
	HeapTuple tup;
	bool nulls[Natts_pipeline_tstate];
	Datum values[Natts_pipeline_tstate];

	MemSet(nulls, 0, sizeof(nulls));

	pipeline_tstate = heap_open(PipelineTStateRelationId, RowExclusiveLock);

	values[Anum_pipeline_tstate_id - 1] = Int32GetDatum(id);

	/* distinct Bloom filter should be NULL */
	nulls[Anum_pipeline_tstate_distinct - 1] = true;

	tup = heap_form_tuple(pipeline_tstate->rd_att, values, nulls);
	simple_heap_insert(pipeline_tstate, tup);
	CatalogUpdateIndexes(pipeline_tstate, tup);
	CommandCounterIncrement();

	heap_freetuple(tup);
	heap_close(pipeline_tstate, NoLock);
}

void
RemoveTStateEntry(Oid id)
{
	Relation pipeline_tstate = heap_open(PipelineTStateRelationId, RowExclusiveLock);
	HeapTuple tuple = SearchSysCache1(PIPELINETSTATEID, Int32GetDatum(id));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for continuous view %u transition state", id);

	simple_heap_delete(pipeline_tstate, &tuple->t_self);
	ReleaseSysCache(tuple);
	CommandCounterIncrement();

	heap_close(pipeline_tstate, NoLock);
}

void
ResetTStateEntry(Oid id)
{
	Relation pipeline_tstate = heap_open(PipelineTStateRelationId, RowExclusiveLock);
	HeapTuple tuple;
	bool nulls[Natts_pipeline_tstate];
	bool replaces[Natts_pipeline_tstate];
	Datum values[Natts_pipeline_tstate];
	HeapTuple newtuple;

	tuple = SearchSysCache1(PIPELINETSTATEID, Int32GetDatum(id));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for continuous view %u transition state", id);

	/* Set everything to NULL */
	MemSet(nulls, true, sizeof(nulls));
	MemSet(replaces, true, sizeof(replaces));
	replaces[Anum_pipeline_tstate_id - 1] = false;

	newtuple = heap_modify_tuple(tuple, pipeline_tstate->rd_att,
			values, nulls, replaces);
	simple_heap_update(pipeline_tstate, &newtuple->t_self, newtuple);
	CommandCounterIncrement();
	ReleaseSysCache(tuple);

	heap_close(pipeline_tstate, NoLock);
}

void
UpdateDistinctBloomFilter(Oid id, BloomFilter *distinct)
{
	Relation pipeline_tstate = heap_open(PipelineTStateRelationId, RowExclusiveLock);
	HeapTuple tuple;
	bool nulls[Natts_pipeline_tstate];
	bool replaces[Natts_pipeline_tstate];
	Datum values[Natts_pipeline_tstate];
	HeapTuple newtuple;

	tuple = SearchSysCache1(PIPELINETSTATEID, Int32GetDatum(id));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for continuous view %u transition state", id);

	SET_VARSIZE(distinct, BloomFilterSize(distinct));

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));
	replaces[Anum_pipeline_tstate_distinct - 1] = true;
	values[Anum_pipeline_tstate_distinct - 1] = PointerGetDatum(distinct);

	newtuple = heap_modify_tuple(tuple, pipeline_tstate->rd_att,
				values, nulls, replaces);
	simple_heap_update(pipeline_tstate, &newtuple->t_self, newtuple);
	CatalogUpdateIndexes(pipeline_tstate, newtuple);
	CommandCounterIncrement();
	ReleaseSysCache(tuple);

	heap_close(pipeline_tstate, RowExclusiveLock);
}

BloomFilter *
GetDistinctBloomFilter(Oid id)
{
	BloomFilter *bloom;
	bool isnull = true;
	HeapTuple tuple = SearchSysCache1(PIPELINETSTATEID, Int32GetDatum(id));
	Datum datum = 0;

	if (HeapTupleIsValid(tuple))
		datum = SysCacheGetAttr(PIPELINETSTATEID, tuple, Anum_pipeline_tstate_distinct, &isnull);

	if (isnull)
		bloom = BloomFilterCreate();
	else
		bloom = BloomFilterCopy((BloomFilter *) PG_DETOAST_DATUM(datum));

	if (HeapTupleIsValid(tuple))
		ReleaseSysCache(tuple);

	return bloom;
}
