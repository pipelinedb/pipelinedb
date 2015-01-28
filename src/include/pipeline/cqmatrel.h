/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqmatrel.h
 *	  Interface for modifying continuous view materialization tables
 *
 * src/include/catalog/pipeline/cqmatrel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQMATVIEW_H
#define CQMATVIEW_H

char *GetUniqueMatRelName(char *cvname, char* nspname);

ResultRelInfo *CQMatViewOpen(Relation matrel);
void CQMatViewClose(ResultRelInfo *rinfo);
void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot);
void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot);
void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot);

#endif
