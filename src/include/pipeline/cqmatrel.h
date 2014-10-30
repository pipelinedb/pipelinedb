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

ResultRelInfo *CQMatViewOpen(Relation matrel);
void CQMatViewClose(ResultRelInfo *rinfo);
void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot);
void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot);
void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot);

#endif
