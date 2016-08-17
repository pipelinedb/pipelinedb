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
#include "pipeline/cont_execute.h"

typedef bool (*CombinerReceiveFunc) (ContQuery *query, uint32 shard_hash, uint64 group_hash, HeapTuple tup);
extern CombinerReceiveFunc CombinerReceiveHook;
typedef void (*CombinerFlushFunc) (void);
extern CombinerFlushFunc CombinerFlushHook;

extern DestReceiver *CreateCombinerDestReceiver(void);
extern void SetCombinerDestReceiverParams(DestReceiver *self, ContExecutor *cont_exec, ContQuery *query);
extern void SetCombinerDestReceiverHashFunc(DestReceiver *self, FuncExpr *hash);
extern void CombinerDestReceiverFlush(DestReceiver *self);

#endif
