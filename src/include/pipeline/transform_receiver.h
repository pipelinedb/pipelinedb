/*-------------------------------------------------------------------------
 *
 * transform_receiver.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/transform_receiver.h
 *
 */
#ifndef TRANSFORM_RECEIVER_H
#define TRANSFORM_RECEIVER_H

#include "tcop/dest.h"
#include "pipeline/executor.h"

typedef struct TransformReceiver
{
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	Relation tg_rel;
	bool os_has_readers;
	FunctionCallInfo trig_fcinfo;

	/* only used by the optimized code path for pipeline_stream_insert */
	HeapTuple *tups;
	int nmaxtups;
	int ntups;
} TransformReceiver;

typedef void (*TransformFlushFunc) (void);
extern TransformFlushFunc TransformFlushHook;

extern TransformReceiver *CreateTransformReceiver(ContExecutor *exec, ContQuery *query);
extern void TransformDestReceiverFlush(TransformReceiver *self, TupleTableSlot *slot, Tuplestorestate *store);

#endif
