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

extern Node *CreatePhysicalGroupLookupPath(RelOptInfo *joinrel, NestPath *path);

#endif
