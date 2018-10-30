/*-------------------------------------------------------------------------
 *
 * compat.h
 *  Interface for functionality that breaks between major PG versions
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/tuptable.h"
#include "nodes/execnodes.h"

#define PIPELINE_COMPAT_INDEX_ATTR_BITMAP_ALL INDEX_ATTR_BITMAP_ALL

extern bool CompatProcOidIsAgg(Oid oid);
extern TupleTableSlot *CompatExecInitExtraTupleSlot(EState *estate);
