/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqmatrel.c
 *	  Support for interacting with CV materialization tables and their indexes
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cqmatrel.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "pipeline/cqmatrel.h"
#include "utils/rel.h"

/*
 * CQMatViewOpen
 *
 * Open any indexes associated with the given materialization table
 */
ResultRelInfo *
CQMatViewOpen(Relation matrel)
{
	ResultRelInfo *resultRelInfo;

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1; /* dummy */
	resultRelInfo->ri_RelationDesc = matrel;
	resultRelInfo->ri_TrigDesc = NULL;

	ExecOpenIndices(resultRelInfo);

	return resultRelInfo;
}

/*
 * CQMatViewClose
 *
 * Release any resources associated with the given indexes
 */
void
CQMatViewClose(ResultRelInfo *rinfo)
{
	ExecCloseIndices(rinfo);
	pfree(rinfo);
}

/*
 * ExecInsertCQMatRelIndexTuples
 *
 * This is a trimmed-down version of ExecInsertIndexTuples
 */
void
ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot)
{
	int			i;
	int			numIndexes;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	HeapTuple tup = ExecMaterializeSlot(slot);

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(tup))
		return;

	/* bail if there are no indexes to update */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;
	relationDescs = indstate->ri_IndexRelationDescs;
	indexInfoArray = indstate->ri_IndexRelationInfo;
	heapRelation = indstate->ri_RelationDesc;

	/*
	 * for each index, form and insert the index tuple
	 */
	for (i = 0; i < numIndexes; i++)
	{
		IndexInfo  *indexInfo;

		indexInfo = indexInfoArray[i];

		/* If the index is marked as read-only, ignore it */
		if (!indexInfo->ii_ReadyForInserts)
			continue;

		FormIndexDatum(indexInfo, slot, NULL, values, isnull);

		index_insert(relationDescs[i], values, isnull, &(tup->t_self),
				heapRelation, relationDescs[i]->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);
	}
}

/*
 * ExecCQMatViewUpdate
 *
 * Update an existing row of a CV materialization table.
 */
void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot)
{
	HeapTuple tup = ExecMaterializeSlot(slot);

	simple_heap_update(ri->ri_RelationDesc, &tup->t_self, tup);
	ExecInsertCQMatRelIndexTuples(ri, slot);
}

/*
 * ExecCQMatViewInsert
 *
 * Insert a new row into a CV materialization table
 */
void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot)
{
	HeapTuple tup = ExecMaterializeSlot(slot);

	heap_insert(ri->ri_RelationDesc, tup, GetCurrentCommandId(true), 0, NULL);
	ExecInsertCQMatRelIndexTuples(ri, slot);
}
