/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * streamReceiver.h
 *	  An implementation of DestReceiver that that allows sends output
 *	  tuples to a tuple buffer
 *
 * IDENTIFICATION
 *	  src/include/pipeline/streamReceiver.h
 *
 */

#ifndef STREAM_RECEIVER_H
#define STREAM_RECEIVER_H

#include "postgres.h"

#include "tcop/dest.h"

typedef struct StreamReceiver
{
	DestReceiver pub;
	InsertBatchAck *acks;
	int nbatches;
	Bitmapset *targets;
	long count;
	TupleDesc desc;
	MemoryContext context;
	Size bytes;
} StreamReceiver;

extern DestReceiver *CreateStreamDestReceiver(void);
extern void SetStreamDestReceiverParams(DestReceiver *self, Bitmapset *targets,
		TupleDesc desc, int nbatches, InsertBatchAck *acks);

#endif
