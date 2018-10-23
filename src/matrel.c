/*-------------------------------------------------------------------------
 *
 * matrel.c
 *	  Support for interacting with CV materialization tables and their indexes
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "executor/executor.h"
#include "matrel.h"
#include "miscutils.h"
#include "nodes/execnodes.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/syscache.h"

bool matrels_writable;

/*
 * CQOSRelOpen
 *
 * Open an output stream
 */
ResultRelInfo *
CQOSRelOpen(Relation osrel)
{
	ResultRelInfo *resultRelInfo;

	resultRelInfo = makeNode(ResultRelInfo);
	resultRelInfo->ri_RangeTableIndex = 1; /* dummy */
	resultRelInfo->ri_RelationDesc = osrel;
	resultRelInfo->ri_TrigDesc = NULL;

	return resultRelInfo;
}

/*
 * CQCSRelClose
 */
void
CQOSRelClose(ResultRelInfo *rinfo)
{
	pfree(rinfo);
}

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

	ExecOpenIndices(resultRelInfo, false);

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
				heapRelation, relationDescs[i]->rd_index->indisunique ? UNIQUE_CHECK_YES : UNIQUE_CHECK_NO, indexInfo);
	}
}

/*
 * ExecCQMatViewUpdate
 *
 * Update an existing row of a CV materialization table.
 */
void
ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate)
{
	HeapTuple tup;
	bool result = true;

	if (ri->ri_RelationDesc->rd_att->constr)
	{
		/*
		 * We don't want the entire sync transaction to fail when a constraint fails
		 */
		PG_TRY();
		{
			ExecConstraints(ri, slot, estate);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			result = false;
		}
		PG_END_TRY();
	}

	if (!result)
		return;

	tup = ExecMaterializeSlot(slot);
	simple_heap_update(ri->ri_RelationDesc, &tup->t_self, tup);

	if (!HeapTupleIsHeapOnly(tup))
		ExecInsertCQMatRelIndexTuples(ri, slot, estate);
}

/*
 * ExecCQMatViewInsert
 *
 * Insert a new row into a CV materialization table
 */
void
ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate)
{
	HeapTuple tup;
	bool result = true;

	if (ri->ri_RelationDesc->rd_att->constr)
	{
		/*
		 * We don't want the entire sync transaction to fail when a constraint fails
		 */
		PG_TRY();
		{
			ExecConstraints(ri, slot, estate);
		}
		PG_CATCH();
		{
			EmitErrorReport();
			FlushErrorState();

			result = false;
		}
		PG_END_TRY();
	}

	if (!result)
		return;

	tup = ExecMaterializeSlot(slot);

	heap_insert(ri->ri_RelationDesc, tup, GetCurrentCommandId(true), 0, NULL);
	ExecInsertCQMatRelIndexTuples(ri, slot, estate);
}

/*
 * CVNameToOSRelName
 */
char *
CVNameToOSRelName(char *cv_name)
{
	char *relname = palloc0(NAMEDATALEN);

	strcpy(relname, cv_name);
	append_suffix(relname, CQ_OSREL_SUFFIX, NAMEDATALEN);

	return relname;
}

/*
 * CVNameToMatRelName
 */
char *
CVNameToMatRelName(char *cv_name)
{
	char *relname = palloc0(NAMEDATALEN);

	strcpy(relname, cv_name);
	append_suffix(relname, CQ_MATREL_SUFFIX, NAMEDATALEN);

	return relname;
}

/*
 * CVNameToDefRelName
 */
char *
CVNameToDefRelName(char *cv_name)
{
	char *relname = palloc0(NAMEDATALEN);

	strcpy(relname, cv_name);
	append_suffix(relname, CQ_DEFREL_SUFFIX, NAMEDATALEN);

	return relname;
}

/*
 * CVNameToSeqRelName
 */
char *
CVNameToSeqRelName(char *cv_name)
{
	char *relname = palloc0(NAMEDATALEN);

	strcpy(relname, cv_name);
	append_suffix(relname, CQ_SEQREL_SUFFIX, NAMEDATALEN);

	return relname;
}
