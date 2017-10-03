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
	BatchReceiver base;
	ContQuery *cont_query;
	ContExecutor *cont_exec;
	Relation tg_rel;
	bool os_has_readers;
	FunctionCallInfo trig_fcinfo;

	/* only used by the optimized code path for pipeline_stream_insert */
	HeapTuple *tups;
	int nmaxtups;
	int ntups;

	AttrNumber *osrel_attrs;
	bool needs_alignment;
} TransformReceiver;

typedef void (*TransformFlushFunc) (void);
extern TransformFlushFunc TransformFlushHook;

extern BatchReceiver *CreateTransformReceiver(ContExecutor *exec, ContQuery *query, Tuplestorestate *buffer);

#endif
