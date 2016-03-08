/*-------------------------------------------------------------------------
 *
 * transformReceiver.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/transformReceiver.h
 *
 */
#ifndef TRANSFORM_RECEIVER_H
#define TRANSFORM_RECEIVER_H

#include "tcop/dest.h"
#include "pipeline/cont_execute.h"

extern DestReceiver *CreateTransformDestReceiver(void);
extern void SetTransformDestReceiverParams(DestReceiver *self, ContExecutor *exec, ContQuery *query);
extern void TransformDestReceiverFlush(DestReceiver *self);

#endif
