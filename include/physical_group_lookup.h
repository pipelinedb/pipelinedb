/*-------------------------------------------------------------------------
 *
 * physical_group_lookup.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PHYSICAL_GROUP_LOOKUP_H
#define PHYSICAL_GROUP_LOOKUP_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "optimizer/planner.h"

typedef struct PhysicalTupleData
{
	HeapTuple tuple;	/* physical tuple belonging to this entry */
	char flags;
}	PhysicalTupleData;

typedef struct PhysicalTupleData *PhysicalTuple;

extern void SetPhysicalGroupLookupOutput(TupleHashTable output);
extern Node *CreatePhysicalGroupLookupPath(RelOptInfo *joinrel, Path *path);
extern Plan *CreatePhysicalGroupLookupPlan(Plan *outer);

#endif
