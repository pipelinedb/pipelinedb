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
#include "events/streambuf.h"
#include "executor/tuptable.h"
#include "storage/lwlock.h"

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
long GlobalStreamBufferSize = 100000;

#define StreamBufferSlotSize(slot) (HEAPTUPLESIZE + \
		(slot)->event->t_len + sizeof(StreamBufferSlot) + strlen(slot->stream) + 1)

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
alloc_slot(const char *stream, StreamBuffer *buf, HeapTuple event)
{
	HeapTuple shared;
	StreamBufferSlot *result;
	StreamBufferSlot *victim;
	Size size = HEAPTUPLESIZE + event->t_len + sizeof(StreamBufferSlot) + strlen(stream) + 1;
	char *cur;
	long free = 0;

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

	return result;
}

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
extern StreamBufferSlot *
AppendStreamEvent(const char *stream, StreamBuffer *buf, HeapTuple event)
{
	Bitmapset *targets = GetTargetsFor(stream, buf->targets);
	StreamBufferSlot *sbs;
	if (targets == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}

	elog(LOG, "GOT");
	sbs = alloc_slot(stream, buf, event);

	/*
	 * The source bitmap is allocated in local memory (in GetStreamTargets),
	 * but the delete operations that will be performed on it should never
	 * free anything, so it's safe (although a bit sketchy) to copy it into
	 * shared memory.
	 */


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
	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer",
			GlobalStreamBufferSize + sizeof(StreamBuffer), &found);

	GlobalStreamBuffer->capacity = GlobalStreamBufferSize;
	GlobalStreamBuffer->start = (char *) (GlobalStreamBuffer + sizeof(StreamBuffer));
	GlobalStreamBuffer->pos = GlobalStreamBuffer->start;
	GlobalStreamBuffer->targets = CreateStreamTargets();
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
	return NULL;
}

/*
 * NextStreamEvent
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 * If this is the last reader that needs to see a given event, the event is deleted
 * from the stream buffer.
 */
extern HeapTuple
NextStreamEvent(StreamBufferReader *reader)
{
	return NULL;
}
