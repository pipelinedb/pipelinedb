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
#include "miscadmin.h"

/* Whether or not to print the state of the stream buffer as it changes */
bool DebugPrintStreamBuffer;

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
int StreamBufferBlocks;

StreamTargets *targets;

/*
 * wait_for_overwrite
 *
 * Waits until the given slot has been read by all CQs that need to see it
 */
static void wait_for_overwrite(StreamBuffer *buf, StreamBufferSlot *slot)
{
	if (DebugPrintStreamBuffer)
	{
		elog(LOG, "evicted %dB at [%d, %d)", StreamBufferSlotSize(slot),
				BufferOffset(buf, slot), BufferOffset(buf, slot) + StreamBufferSlotSize(slot));
	}
}

/*
 * alloc_slot
 *
 * Finds enough tombstoned slots to zero out and re-use shared memory for a new slot
 */
static StreamBufferSlot *
alloc_slot(const char *stream, const char *encoding, StreamBuffer *buf, StreamEvent event)
{
	char *pos = *buf->pos;
	long free = 0;
	StreamEvent shared;
	StreamBufferSlot *result;
	Size size;
	MemoryContext oldcontext;
	Bitmapset *bms;
	StreamBufferSlot *victim = *GlobalStreamBuffer->nextvictim;

	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	bms = GetTargetsFor(stream, targets);
	MemoryContextSwitchTo(oldcontext);

	if (bms == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}
	size = sizeof(StreamEventData) + event->len + sizeof(StreamBufferSlot) +
			strlen(stream) + 1 + strlen(encoding) + 1 + sizeof(Bitmapset) + bms->nwords * sizeof(bitmapword);

	if (size > buf->capacity)
		elog(ERROR, "event of size %d too big for stream buffer of size %d", (int) size, (int) buf->capacity);

	if (pos + size > BufferEnd(buf))
	{
		StreamBufferSlot *sbs;

		if (DebugPrintStreamBuffer)
		{
			elog(LOG, "wrapping around to 0 from offset %d", (int) (pos - buf->start));
			if (pos < buf->start + buf->capacity)
				elog(LOG, "garbage detected at [%d, %d)", BufferOffset(buf, pos), (int) buf->capacity);
		}

		/*
		 * We always start scanning at the beginning of the segment, as we need to look
		 * at complete events. The beginning
		 */
		pos = buf->start;

		sbs = (StreamBufferSlot *) pos;
		free = StreamBufferSlotSize(sbs);
		wait_for_overwrite(buf, sbs);

		/*
		 * Scan for queued events that we can clobber to make room for the incoming event.
		 * Note that the incoming event may need to clobber more than one
		 * to fit, so we have to look at entire events before overwriting them
		 */
		while (free < size)
		{
			sbs = (StreamBufferSlot *) SHMQueueNext(&(buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
			wait_for_overwrite(buf, sbs);
			free += StreamBufferSlotSize(sbs);
		}

		/* see declaration of nextvictim in streambuf.h for comments */
		victim = (StreamBufferSlot *)
				SHMQueueNext(&(buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
	}

	while (victim != NULL && (char *) victim != buf->start &&
			pos + size > (char *) victim)
	{
		/*
		 * this event will consume the gap between the buffer position
		 * and the next complete event's (victim) start address, so we
		 * need to check the event to make sure we can clobber it
		 */
		wait_for_overwrite(buf, victim);
		victim = (StreamBufferSlot *)
				SHMQueueNext(&(buf->buf), &(victim->link), offsetof(StreamBufferSlot, link));
	}

	*GlobalStreamBuffer->nextvictim = victim;

	MemSet(pos, 0, sizeof(StreamBufferSlot));
	result = (StreamBufferSlot *) pos;
	pos += sizeof(StreamBufferSlot);

	MemSet(pos, 0, sizeof(StreamEvent) + event->len);
	shared = (StreamEvent) pos;
	shared->len = event->len;
	pos += sizeof(StreamEventData);

	shared->raw = pos;
	memcpy(shared->raw, event->raw, event->len);
	pos += event->len;

	result->event = shared;
	result->stream = pos;
	memcpy(result->stream, stream, strlen(stream) + 1);
	pos += strlen(stream) + 1;

	result->encoding = pos;
	memcpy(result->encoding, encoding, strlen(encoding) + 1);
	pos += strlen(encoding) + 1;

	/*
	 * The source bitmap is allocated in local memory (in GetStreamTargets),
	 * but the delete operations that will be performed on it should never
	 * free anything, so it's safe (although a bit sketchy) to copy it into
	 * shared memory.
	 */
	result->readby = (Bitmapset *) pos;
	result->readby->nwords = bms->nwords;
	memcpy(result->readby->words, bms->words, sizeof(bitmapword) * bms->nwords);
	pos += sizeof(Bitmapset) + sizeof(bitmapword) * bms->nwords;

	result->len = size;

	*buf->pos = pos;

	return result;
}

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
extern StreamBufferSlot *
AppendStreamEvent(const char *stream, const char *encoding, StreamBuffer *buf, StreamEvent ev)
{
	StreamBufferSlot *sbs;
	char *prev = *buf->pos;

	LWLockAcquire(StreamBufferLock, LW_EXCLUSIVE);
	sbs = alloc_slot(stream, encoding, buf, ev);

	if (sbs != NULL)
	{
		SHMQueueElemInit(&(sbs->link));
		SHMQueueInsertBefore(&(buf->buf), &(sbs->link));
	}

	LWLockRelease(StreamBufferLock);

	if (DebugPrintStreamBuffer)
	{
		/* did we wrap around? */
		if (*buf->pos < prev)
			prev = buf->start;

		elog(LOG, "appended %dB at [%d, %d)",
				StreamBufferSlotSize(sbs), BufferOffset(buf, prev), BufferOffset(buf, *buf->pos));
	}

	return sbs;
}

/*
 * StreamBufferSize
 *
 * Retrieves the size in bytes of the stream buffer
 */
extern Size
StreamBufferShmemSize(void)
{
	return (StreamBufferBlocks * BLCKSZ) + sizeof(StreamBuffer);
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
	Size size = StreamBufferShmemSize();

	Size headersize = MAXALIGN(sizeof(StreamBuffer));

	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer", headersize , &found);
	GlobalStreamBuffer->capacity = size;

	/*
	 * This is kind of ugly to not just put this thing in shmem,
	 * but the Bitmapset is a constant and a local-memory implementation
	 * so we just leave it in cache memory.
	 */
	oldcontext = MemoryContextSwitchTo(CacheMemoryContext);
	targets = CreateStreamTargets();
	MemoryContextSwitchTo(oldcontext);

	if (!found)
	{
		SHMQueueInit(&(GlobalStreamBuffer->buf));

		GlobalStreamBuffer->start = (char *) ShmemAlloc(size);

		GlobalStreamBuffer->pos = ShmemAlloc(sizeof(char *));
		*GlobalStreamBuffer->pos = GlobalStreamBuffer->start;

		GlobalStreamBuffer->nextvictim = ShmemAlloc(sizeof(StreamBufferSlot *));
		*GlobalStreamBuffer->nextvictim = NULL;
	}
}

/*
 * OpenStreamBufferReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
extern StreamBufferReader *
OpenStreamBufferReader(StreamBuffer *buf, int queryid)
{
	StreamBufferSlot *sbs;
	StreamBufferReader *reader = (StreamBufferReader *) palloc(sizeof(StreamBufferReader));
	int count = 0;
	reader->queryid = queryid;
	reader->buf = buf;
	reader->pos = buf->start;

	/* advance the reader to the first relevant slot in the stream buffer */
	sbs = (StreamBufferSlot *)
		SHMQueueNext(&(reader->buf->buf), &(reader->buf->buf), offsetof(StreamBufferSlot, link));
	while (sbs != NULL)
	{
		if (bms_is_member(queryid, sbs->readby))
			break;
		sbs = (StreamBufferSlot *)
				SHMQueueNext(&(reader->buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
		count++;
	}

	if (DebugPrintStreamBuffer)
		elog(LOG, "advanced reader %d past %d events", queryid, count);

	reader->next = sbs;

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

	LWLockAcquire(StreamBufferLock, LW_EXCLUSIVE);

	if (current == NULL)
	{
		current = (StreamBufferSlot *)
			SHMQueueNext(&(reader->buf->buf), &(reader->buf->buf), offsetof(StreamBufferSlot, link));
	}

	if (current == NULL)
	{
		LWLockRelease(StreamBufferLock);
		return NULL;
	}

	if (bms_is_member(reader->queryid, current->readby))
	{
		result = current;
		bms_del_member(current->readby, reader->queryid);
	}

	reader->next = (StreamBufferSlot *)
			SHMQueueNext(&(reader->buf->buf), &(current->link), offsetof(StreamBufferSlot, link));

	LWLockRelease(StreamBufferLock);

	if (result && DebugPrintStreamBuffer)
		elog(LOG, "read event at [%d, %d)", BufferOffset(reader->buf, result), \
				BufferOffset(reader->buf, result) + StreamBufferSlotSize(result));

	return result;
}

extern void
ReadAndPrintStreamBuffer(StreamBuffer *buf, int32 queryid, int intervalms)
{
	StreamBufferReader *reader = OpenStreamBufferReader(buf, queryid);
	StreamBufferSlot *sbs;
	int count = 0;
	int size = 0;

	printf("====\n");
	while ((sbs = NextStreamEvent(reader)) != NULL)
	{
		count++;
		printf("size = %dB, stream = \"%s\", encoding = \"%s\" addr = %p\n",
				(int) StreamBufferSlotSize(sbs), sbs->stream, sbs->encoding, &(sbs->link));

		size += StreamBufferSlotSize(sbs);

		if (intervalms > 0)
			pg_usleep(intervalms * 1000);
	}
	printf("\n%d events (%dB).\n", count, size);
	printf("^^^^\n");
}

extern void
PrintStreamBuffer(StreamBuffer *buf)
{
	int count = 0;
	StreamBufferSlot *sbs = (StreamBufferSlot *)
		SHMQueueNext(&(buf->buf), &(buf->buf), offsetof(StreamBufferSlot, link));

	printf("====\n");
	LWLockAcquire(StreamBufferLock, LW_EXCLUSIVE);
	while (sbs != NULL)
	{
		count++;
		printf("[%p] size = %d, stream = \"%s\", encoding = \"%s\"\n", sbs,
				(int) StreamBufferSlotSize(sbs), sbs->stream, sbs->encoding);

		sbs = (StreamBufferSlot *)
				SHMQueueNext(&(buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
	}
	LWLockRelease(StreamBufferLock);

	printf("\n%d events.\n", count);
	printf("^^^^\n");
}
