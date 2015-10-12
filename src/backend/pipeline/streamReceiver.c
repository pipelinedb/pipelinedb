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

	if (stream->adhoc_data->num_adhoc)
	{
		int i = 0;

		int tmp_count = 0;
		int tmp_bytes = 0;

		for (i = 0; i < stream->adhoc_data->num_adhoc; ++i)
		{
			AdhocQuery *query = &stream->adhoc_data->queries[i];
			StreamTuple *tuple = 
				MakeStreamTuple(tup, stream->desc, 1, &query->ack);
			Bitmapset *single = bms_make_singleton(query->cq_id);

			tuple->group_hash = query->cq_id;

			if (TupleBufferInsert(AdhocTupleBuffer, tuple, single))
			{
				query->count++;
				tmp_count++;
				tmp_bytes += tuple->heaptup->t_len + HEAPTUPLESIZE;
			}

			count = Max(count, tmp_count);
			bytes = Max(bytes, tmp_bytes);
		}
	}

	stream->count += count;
	stream->bytes += bytes;

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
							TupleDesc desc,
							int nacks, 
							InsertBatchAck *acks,
							AdhocData *adhoc_data)
{
	StreamReceiver *stream = (StreamReceiver *) self;

	stream->desc = desc;
	stream->targets = targets;
	stream->nacks = nacks;
	stream->acks = acks;

	stream->adhoc_data = adhoc_data;
	stream->context = CurrentMemoryContext;
}
