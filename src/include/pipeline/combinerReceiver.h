/*-------------------------------------------------------------------------
 *
 * combinerReceiver.h
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/combinerReceiver.h
 *
 */
#ifndef COMBINER_RECEIVER_H
#define COMBINER_RECEIVER_H

#include "tcop/dest.h"

extern DestReceiver *CreateCombinerDestReceiver(void);
extern void SetCombinerDestReceiverParams(DestReceiver *self, TupleBufferBatchReader *reader, ContinuousView *v);
extern void SetCombinerDestReceiverHashFunc(DestReceiver *self, FuncExpr *hash, MemoryContext context);

#endif
