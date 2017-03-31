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

typedef void (*TransformFlushFunc) (void);
extern TransformFlushFunc TransformFlushHook;

extern DestReceiver *CreateTransformDestReceiver(void);
extern void SetTransformDestReceiverParams(DestReceiver *self, ContExecutor *exec, ContQuery *query);
extern void TransformDestReceiverFlush(DestReceiver *self);

#endif
