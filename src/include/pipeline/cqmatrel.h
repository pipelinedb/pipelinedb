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
void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot, EState *estate);
void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);
void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);

#endif
