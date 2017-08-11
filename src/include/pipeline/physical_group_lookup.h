/*-------------------------------------------------------------------------
 *
 * physical_group_lookup.h
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/physical_group_lookup.h
 */
#ifndef PHYSICAL_GROUP_LOOKUP_H
#define PHYSICAL_GROUP_LOOKUP_H

#include "postgres.h"

#include "optimizer/planner.h"

typedef struct HeapTupleEntryData
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	HeapTuple tuple;	/* physical tuple belonging to this entry */
	char flags;
}	HeapTupleEntryData;

typedef struct HeapTupleEntryData *HeapTupleEntry;

extern void SetPhysicalGroupLookupOutput(TupleHashTable output);
extern Node *CreatePhysicalGroupLookupPath(RelOptInfo *joinrel, Path *path);
extern Plan *CreatePhysicalGroupLookupPlan(Plan *outer);

#endif
