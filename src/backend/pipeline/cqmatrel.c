/*-------------------------------------------------------------------------
 *
 * cqmatrel.c
 *	  Support for interacting with CV materialization tables and their indexes
 *
 * Copyright (c) 2013-2015, PipelineDB
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
#include "pipeline/miscutils.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/syscache.h"

bool continuous_query_materialization_table_updatable;

/*
 * CQMatViewOpen
 *
 * Open any indexes associated with the given materialization table
 */
ResultRelInfo *
CQMatRelOpen(Relation matrel)
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
CQMatRelClose(ResultRelInfo *rinfo)
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
ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot, EState *estate)
{
	int			i;
	int			numIndexes;
	RelationPtr relationDescs;
	Relation	heapRelation;
	IndexInfo **indexInfoArray;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	HeapTuple tup;

	/* bail if there are no indexes to update */
	numIndexes = indstate->ri_NumIndices;
	if (numIndexes == 0)
		return;

	tup = ExecMaterializeSlot(slot);

	/* HOT update does not require index inserts */
	if (HeapTupleIsHeapOnly(tup))
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

		/* Index expressions need an EState to be eval'd in */
		if (indexInfo->ii_Expressions)
		{
			ExprContext *econtext = GetPerTupleExprContext(estate);
			econtext->ecxt_scantuple = slot;
		}

		FormIndexDatum(indexInfo, slot, estate, values, isnull);

		index_insert(relationDescs[i], values, isnull, &(tup->t_self),
				heapRelation, relationDescs[i]->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO);
	}
}

static bool
matrel_heap_update(Relation relation, ItemPointer otid, HeapTuple tup)
{
	HTSU_Result result;
	HeapUpdateFailureData hufd;
	LockTupleMode lockmode;

	result = heap_update(relation, otid, tup,
						 GetCurrentCommandId(true), InvalidSnapshot,
						 true /* wait for commit */ ,
						 &hufd, &lockmode);
	switch (result)
	{
		case HeapTupleSelfUpdated:
			/* Tuple was already updated in current command? */
			elog(ERROR, "tuple already updated by self");
			break;

		case HeapTupleMayBeUpdated:
			/* done successfully */
			break;

		case HeapTupleUpdated:
			/*
			 * Tuple updated by a concurrent transaction? The only legal case is if the tuple was deleted
			 * which can happen if the auto-vacuumer deletes the tuple while we were trying to update it.
			 */
			if (memcmp(&hufd.ctid, otid, sizeof(ItemPointerData)) == 0)
				return false;
			elog(ERROR, "tuple concurrently updated");
			break;

		default:
			elog(ERROR, "unrecognized heap_update status: %u", result);
			break;
	}

	return true;
}

/*
 * ExecCQMatViewUpdate
 *
 * Update an existing row of a CV materialization table.
 */
bool
ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate)
{
	HeapTuple tup = ExecMaterializeSlot(slot);
	bool updated = matrel_heap_update(ri->ri_RelationDesc, &tup->t_self, tup);
	ExecInsertCQMatRelIndexTuples(ri, slot, estate);
	return updated;
}

/*
 * ExecCQMatViewInsert
 *
 * Insert a new row into a CV materialization table
 */
void
ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate)
{
	HeapTuple tup = ExecMaterializeSlot(slot);

	heap_insert(ri->ri_RelationDesc, tup, GetCurrentCommandId(true), 0, NULL);
	ExecInsertCQMatRelIndexTuples(ri, slot, estate);
}

char *
CVNameToMatRelName(char *cv_name)
{
	char *relname = palloc0(NAMEDATALEN);

	strcpy(relname, cv_name);
	append_suffix(relname, CQ_MATREL_SUFFIX, NAMEDATALEN);

	return relname;
}
