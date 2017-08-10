/*-------------------------------------------------------------------------
 *
 * physical_group_lookup.c
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/physical_group_lookup.c
 */
#include "postgres.h"

#include "access/heapam.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "pipeline/physical_group_lookup.h"
#include "utils/rel.h"

