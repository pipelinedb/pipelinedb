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

extern char *GetUniqueMatRelName(char *cvname, char* nspname);

extern ResultRelInfo *CQMatRelOpen(Relation matrel);
extern void CQMatRelClose(ResultRelInfo *rinfo);
extern void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot, EState *estate);
extern void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);
extern void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);

#endif
