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
#include "postgres.h"

#include "access/xact.h"
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
	int num_worker = bms_num_members(stream->targets);
	int num_adhoc = bms_num_members(stream->adhoc_targets);

	int count = 0;
	Size bytes = 0;

	MemoryContext old = MemoryContextSwitchTo(stream->context);
	HeapTuple tup = ExecMaterializeSlot(slot);

	if (num_worker)
	{
		StreamTuple *tuple = MakeStreamTuple(tup, stream->desc, stream->nacks, stream->acks);

		if (TupleBufferInsert(WorkerTupleBuffer, tuple, stream->targets))
		{
			count++;
			bytes += tuple->heaptup->t_len + HEAPTUPLESIZE;
		}
	}

	if (num_adhoc)
	{
		Bitmapset *tmp_targets = bms_copy(stream->adhoc_targets);
		int target = 0;
		int i = 0;

		while ((target = bms_first_member(tmp_targets)) >= 0)
		{
			int tmp_count = 0;
			Size tmp_bytes = 0;
			InsertBatchAck *ack = 0;
			StreamTuple *tuple = 0;
			Bitmapset *targets = 0;
			
			ack = &stream->adhoc_acks[i++];
			tuple = MakeStreamTuple(tup, stream->desc, 1, ack);
			tuple->group_hash = target;

			targets = bms_add_member(0, target);

			if (TupleBufferInsert(AdhocTupleBuffer, tuple, targets))
			{
				tmp_count++;
				tmp_bytes += tuple->heaptup->t_len + HEAPTUPLESIZE;

				count = Max(count, tmp_count);
				bytes = Max(bytes, tmp_bytes);
			}
		}
	}

	stream->count = count;
	stream->bytes = bytes;

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
	self->lastcommit = GetCurrentTimestamp();

	return (DestReceiver *) self;
}

void
SetStreamDestReceiverParams(DestReceiver *self,
							Bitmapset *targets,
							Bitmapset *adhoc_targets,
							TupleDesc desc,
							int nacks, 
							InsertBatchAck *acks,
							int adhoc_nacks,
							InsertBatchAck *adhoc_acks)
{
	StreamReceiver *stream = (StreamReceiver *) self;

	stream->desc = desc;
	stream->targets = targets;
	stream->adhoc_targets = adhoc_targets;
	stream->nacks = nacks;
	stream->acks = acks;

	stream->adhoc_acks = adhoc_acks;
	stream->adhoc_nacks = adhoc_nacks;

	stream->context = CurrentMemoryContext;
}
