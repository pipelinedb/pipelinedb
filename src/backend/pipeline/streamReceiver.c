/*-------------------------------------------------------------------------
 *
 * streamReceiver.c
 *	  An implementation of DestReceiver that that allows sends output
 *	  tuples to a tuple buffer
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/streamReceiver.c
 */
#include "pipeline/stream.h"
#include "pipeline/streamReceiver.h"
#include "pipeline/tuplebuf.h"

static void
stream_shutdown(DestReceiver *self)
{

}

static void
stream_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{

}

static void
stream_receive(TupleTableSlot *slot, DestReceiver *self)
{
	StreamReceiver *stream = (StreamReceiver *) self;
	MemoryContext old = MemoryContextSwitchTo(stream->context);
	HeapTuple tup = ExecMaterializeSlot(slot);
	StreamTuple *tuple = MakeStreamTuple(tup, stream->desc, stream->nacks, stream->acks);

	if (TupleBufferInsert(WorkerTupleBuffer, tuple, stream->targets))
	{
		stream->count++;
		stream->bytes += tuple->heaptup->t_len + HEAPTUPLESIZE;
	}

	MemoryContextSwitchTo(old);
}

static void
stream_destroy(DestReceiver *self)
{
	StreamReceiver *stream = (StreamReceiver *) self;
	pfree(stream);
}

DestReceiver *
CreateStreamDestReceiver(void)
{
	StreamReceiver *self = (StreamReceiver *) palloc0(sizeof(StreamReceiver));

	self->pub.receiveSlot = stream_receive; /* might get changed later */
	self->pub.rStartup = stream_startup;
	self->pub.rShutdown = stream_shutdown;
	self->pub.rDestroy = stream_destroy;
	self->pub.mydest = DestCombiner;
	self->count = 0;

	return (DestReceiver *) self;
}

void
SetStreamDestReceiverParams(DestReceiver *self, Bitmapset *targets, TupleDesc desc,
		int nacks, InsertBatchAck *acks)
{
	StreamReceiver *stream = (StreamReceiver *) self;

	stream->desc = desc;
	stream->targets = targets;
	stream->nacks = nacks;
	stream->acks = acks;
	stream->context = CurrentMemoryContext;
}
