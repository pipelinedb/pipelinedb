/*-------------------------------------------------------------------------
 *
 * streamReceiver.h
 *	  An implementation of DestReceiver that that allows sends output
 *	  tuples to a tuple buffer
 *
 * Copyright (c) 2013-2015, PipelineDB
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
	int nacks;

	Bitmapset *targets;
	int num_targets;

	AdhocData *adhoc_data;

	long count;
	TupleDesc desc;
	MemoryContext context;
	Size bytes;
	TimestampTz lastcommit;

} StreamReceiver;

extern DestReceiver *CreateStreamDestReceiver(void);

extern void
SetStreamDestReceiverParams(DestReceiver *self,
							Bitmapset *targets,
							TupleDesc desc,
							int nacks, 
							InsertBatchAck *acks,
							AdhocData *adhoc_data);

#endif
