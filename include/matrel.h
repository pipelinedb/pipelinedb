/*-------------------------------------------------------------------------
 *
 * matrel.h
 *	  Interface for modifying continuous view materialization tables
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQMATVIEW_H
#define CQMATVIEW_H

#include "nodes/execnodes.h"

extern bool matrels_writable;

#define CQ_OSREL_SUFFIX "_osrel"
#define CQ_MATREL_SUFFIX "_mrel"
#define CQ_SEQREL_SUFFIX "_seq"
#define CQ_DEFREL_SUFFIX "_def"
#define CQ_MATREL_PKEY "$pk"
#define MatRelWritable() (matrels_writable)

extern ResultRelInfo *CQMatRelOpen(Relation matrel);
extern void CQOSRelClose(ResultRelInfo *rinfo);
extern ResultRelInfo *CQOSRelOpen(Relation osrel);
extern void CQMatRelClose(ResultRelInfo *rinfo);
extern void ExecInsertCQMatRelIndexTuples(ResultRelInfo *indstate, TupleTableSlot *slot, EState *estate);
extern void ExecCQMatRelUpdate(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);
extern void ExecCQMatRelInsert(ResultRelInfo *ri, TupleTableSlot *slot, EState *estate);

extern char *CVNameToOSRelName(char *cv_name);
extern char *CVNameToMatRelName(char *cv_name);
extern char *CVNameToDefRelName(char *cv_name);
extern char *CVNameToSeqRelName(char *cv_name);

#endif
