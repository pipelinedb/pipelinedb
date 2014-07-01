/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Interface for interacting with the stream buffer
 *
 * src/backend/events/streambuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "events/stream.h"
#include "events/decode.h"
#include "events/streambuf.h"
#include "executor/tuptable.h"
#include "nodes/print.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
long GlobalStreamBufferSize = 100000;

#define StreamBufferSlotSize(slot) (HEAPTUPLESIZE + \
		(slot)->event->t_len + sizeof(StreamBufferSlot) + strlen(slot->stream) + 1 + \
		strlen(slot->encoding) + 1 + (slot)->readby->nwords * sizeof(bitmapword))

#define SlotAfter(slot) ((slot) + StreamBufferSlotSize(slot))

/*
 * wait_for_overwrite
 *
 * Waits until the given slot has been read by all CQs that need to see it
 */
static void wait_for_overwrite(StreamBuffer *buf, StreamBufferSlot *slot)
{

}

/*
 * alloc_slot
 *
 * Finds enough tombstoned slots to zero out and re-use shared memory for a new slot
 */
static StreamBufferSlot *
alloc_slot(const char *stream, const char *encoding, StreamBuffer *buf, HeapTuple event)
{
	char *cur;
	long free = 0;
	HeapTuple shared;
	StreamBufferSlot *result;
	StreamBufferSlot *victim;
	Size size;
	MemoryContext oldcontext;
	Bitmapset *targets;

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	targets = GetTargetsFor(stream, buf->targets);
	MemoryContextSwitchTo(oldcontext);

	if (targets == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}
	size = HEAPTUPLESIZE + event->t_len + sizeof(StreamBufferSlot) +
			strlen(stream) + 1 + sizeof(Bitmapset) + targets->nwords * sizeof(bitmapword);

	if (buf->pos + size > buf->start + buf->capacity)
	{
		StreamBufferSlot *sbs;

		/*
		 * We always start scanning at the beginning of the segment, as we need to look
		 * at complete events. The beginning
		 */
		buf->pos = buf->start;

		cur = buf->pos;
		sbs = (StreamBufferSlot *) cur;
		free = StreamBufferSlotSize(sbs);
		wait_for_overwrite(buf, sbs);

		/*
		 * Scan for queued events that we can clobber to make room for the incoming event.
		 * Note that the incoming event may need to clobber more than one
		 * to fit, so we have to look at entire events before overwriting them
		 */
		while (free < size && cur < buf->start + buf->capacity)
		{
			sbs = (StreamBufferSlot *) cur;
			free += StreamBufferSlotSize(sbs);
			cur += free;
			wait_for_overwrite(buf, sbs);
		}

		/* see declaration of nextvictim in streambuf.h for comments */
		buf->nextvictim = SlotAfter(sbs);
	}

	victim = (StreamBufferSlot *) buf->pos;
	wait_for_overwrite(buf, victim);

	while (buf->nextvictim != NULL &&
			buf->pos < buf->start + buf->capacity &&
			buf->pos + size > (char *) buf->nextvictim)
	{
		/*
		 * this event will consume the gap between the buffer position
		 * and the next complete event's (victim) start address, so we
		 * need to check the event to make sure we can clobber it
		 */
		wait_for_overwrite(buf, buf->nextvictim);
		buf->nextvictim = SlotAfter(buf->nextvictim);
	}

	MemSet(buf->pos, 0, sizeof(StreamBufferSlot));
	result = (StreamBufferSlot *) buf->pos;
	buf->pos += sizeof(StreamBufferSlot);

	MemSet(buf->pos, 0, HEAPTUPLESIZE + event->t_len);
	shared = (HeapTuple) buf->pos;
	buf->pos += HEAPTUPLESIZE + event->t_len;

	shared->t_len = event->t_len;
	shared->t_self = event->t_self;
	shared->t_tableOid = event->t_tableOid;
	shared->t_xc_node_id = event->t_xc_node_id;
	shared->t_data = (HeapTupleHeader) ((char *) shared + HEAPTUPLESIZE);
	memcpy((char *) shared->t_data, (char *) event->t_data, event->t_len);

	result->event = shared;
	result->stream = buf->pos;
	memcpy(result->stream, stream, strlen(stream) + 1);
	buf->pos += strlen(stream) + 1;

	result->encoding = buf->pos;
	memcpy(result->encoding, encoding, strlen(encoding) + 1);
	buf->pos += strlen(encoding) + 1;

	/*
	 * The source bitmap is allocated in local memory (in GetStreamTargets),
	 * but the delete operations that will be performed on it should never
	 * free anything, so it's safe (although a bit sketchy) to copy it into
	 * shared memory.
	 */
	result->readby = (Bitmapset *) buf->pos;
	result->readby->nwords = targets->nwords;
	memcpy(result->readby->words, targets->words, sizeof(bitmapword) * targets->nwords);
	buf->pos += sizeof(bitmapword) * targets->nwords;

	return result;
}

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
extern StreamBufferSlot *
AppendStreamEvent(const char *stream, const char *encoding, StreamBuffer *buf, HeapTuple event)
{
	StreamBufferSlot *sbs = alloc_slot(stream, encoding, buf, event);

	SHMQueueElemInit(&(sbs->link));

	LWLockAcquire(StreamBufferLock, LW_EXCLUSIVE);
	SHMQueueInsertBefore(&(GlobalStreamBuffer->buf), &(sbs->link));
	LWLockRelease(StreamBufferLock);

	return sbs;
}

/*
 * InitGlobalStreamBuffer
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
extern void InitGlobalStreamBuffer(void)
{
	bool found;
	MemoryContext oldcontext;

	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer",
			GlobalStreamBufferSize + sizeof(StreamBuffer), &found);

	GlobalStreamBuffer->capacity = GlobalStreamBufferSize;
	GlobalStreamBuffer->start = (char *) (GlobalStreamBuffer + sizeof(StreamBuffer));
	GlobalStreamBuffer->pos = GlobalStreamBuffer->start;

	/*
	 * This is kind of ugly to not just put this thing in shmem,
	 * but the Bitmapset is a local-memory implementation so we just leave it in cache memory
	 */
	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	GlobalStreamBuffer->targets = CreateStreamTargets();
	MemoryContextSwitchTo(oldcontext);

	if (!found)
		SHMQueueInit(&(GlobalStreamBuffer->buf));
}

/*
 * OpenStreamBufferReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
extern StreamBufferReader *
OpenStreamBufferReader(StreamBuffer *buf, int queryid)
{
	StreamBufferReader *reader = (StreamBufferReader *) palloc(sizeof(StreamBufferReader));
	reader->queryid = queryid;
	reader->buf = buf;
	reader->pos = buf->start;

	return reader;
}

/*
 * NextStreamEvent
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 * If this is the last reader that needs to see a given event, the event is deleted
 * from the stream buffer.
 */
extern StreamBufferSlot *
NextStreamEvent(StreamBufferReader *reader)
{
	StreamBufferSlot *result = NULL;
	StreamBufferSlot *current = reader->next;

	if (current == NULL)
		current = (StreamBufferSlot *)
			SHMQueueNext(&(reader->buf->buf), &(reader->buf->buf), offsetof(StreamBufferSlot, link));

	if (current == NULL)
		return NULL;

	if (bms_is_member(reader->queryid, current->readby))
	{
		result = current;
		bms_del_member(current->readby, reader->queryid);
	}

	reader->next = (StreamBufferSlot *)
			SHMQueueNext(&(reader->buf->buf), &(current->link), offsetof(StreamBufferSlot, link));

	return result;
}

extern void
PrintStreamBuffer(StreamBuffer *buf, bool verbose)
{
	TupleTableSlot *slot;
	StreamEventDecoder *decoder;
	MemoryContext oldcontext;
	StreamBufferSlot *sbs = (StreamBufferSlot *)
		SHMQueueNext(&(buf->buf), &(buf->buf), offsetof(StreamBufferSlot, link));
	int count = 0;

	printf("====\n");
	while (sbs != NULL)
	{
		oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
		decoder = GetStreamEventDecoder(sbs->encoding);
		MemoryContextSwitchTo(oldcontext);

		count++;
		printf("size = %d, stream = \"%s\", encoding = \"%s\"\n",
				(int) StreamBufferSlotSize(sbs), sbs->stream, sbs->encoding);

		slot = MakeSingleTupleTableSlot(decoder->schema);
		ExecStoreTuple(sbs->event, slot, InvalidBuffer, false);

		if (verbose)
			print_slot(slot);

		sbs = (StreamBufferSlot *)
				SHMQueueNext(&(buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
	}
	printf("\n%d events.\n", count);
	printf("====\n");
}
