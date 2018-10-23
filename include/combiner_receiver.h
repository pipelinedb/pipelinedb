/*-------------------------------------------------------------------------
 *
 * combiner_receiver.h
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef COMBINER_RECEIVER_H
#define COMBINER_RECEIVER_H

#include "tcop/dest.h"
#include "executor.h"

typedef struct CombinerReceiver
{
	BatchReceiver base;
	DestReceiver pub;
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	FunctionCallInfo hash_fcinfo;
	FuncExpr *hashfn;

	uint64 name_hash;
	List **tups_per_combiner;
} CombinerReceiver;

typedef bool (*CombinerReceiveFunc) (ContQuery *query, uint32 shard_hash, uint64 group_hash, HeapTuple tup);
extern CombinerReceiveFunc CombinerReceiveHook;
typedef void (*CombinerFlushFunc) (void);
extern CombinerFlushFunc CombinerFlushHook;

extern BatchReceiver *CreateCombinerReceiver(ContExecutor *cont_exec, ContQuery *query, Tuplestorestate *buffer);
extern void SetCombinerDestReceiverHashFunc(BatchReceiver *self, FuncExpr *hash);

#endif
