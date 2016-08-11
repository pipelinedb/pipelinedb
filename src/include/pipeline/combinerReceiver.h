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

typedef struct PartialTupleState
{
	HeapTuple tup;
	uint64 hash;
	Oid query_id;
	NameData matrel_namespace;
	NameData matrel_name;
} PartialTupleState;


typedef void (*CombinerReceiveFunc) (PartialTupleState *hts, int len);
extern CombinerReceiveFunc CombinerReceiveHook;

extern DestReceiver *CreateCombinerDestReceiver(void);
extern void SetCombinerDestReceiverParams(DestReceiver *self, ContExecutor *cont_exec, ContQuery *query);
extern void SetCombinerDestReceiverHashFunc(DestReceiver *self, FuncExpr *hash);
extern void CombinerDestReceiverFlush(DestReceiver *self);

#endif
