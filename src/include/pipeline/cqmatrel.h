/*-------------------------------------------------------------------------
 *
 * cqmatrel.h
 *	  Interface for modifying continuous view materialization tables
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline/cqmatrel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQMATVIEW_H
#define CQMATVIEW_H

#include "nodes/execnodes.h"

extern bool continuous_query_materialization_table_updatable;

#define MatRelUpdatesEnabled() (continuous_query_materialization_table_updatable)

extern char *GetUniqueMatRelName(char *cvname, char* nspname);

extern ResultRelInfo *CQMatRelOpen(Relation matrel);
extern void CQMatRelClose(ResultRelInfo *rinfo);
extern void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot, EState *estate);
extern bool ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);
extern void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);

#endif
