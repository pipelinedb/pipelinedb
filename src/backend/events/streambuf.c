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
#include "storage/spin.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define NO_SLOTS_FOLLOW -1

#define BufferWrapped(buf) (*(buf)->pos < *(buf)->last)
#define AtBufferEnd(reader) (reader->pos == *((reader)->buf)->last)
#define HasUnreadData(reader) ((reader)->pos < *((reader)->buf)->last)
#define BufferUnchanged(reader) ((reader)->pos == *((reader)->buf)->pos)
#define IsNewReadCycle(reader) (!(reader)->reading)
#define HasPendingReads(slot) (bms_num_members((slot)->readby) > 0)
#define IsNewAppendCycle(buf) ((*buf->prev) == NULL)
#define MustEvict(buf) (!IsNewAppendCycle(buf) && (*buf->prev)->nextoffset != NO_SLOTS_FOLLOW)

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
	/* block until all CQs have marked this event as read */
	while (HasPendingReads(slot));

	if (DebugPrintStreamBuffer)
	{
		elog(LOG, "evicted %dB at [%d, %d)", SlotSize(slot),
				BufferOffset(buf, slot), BufferOffset(buf, slot) + SlotSize(slot));
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
	char *sbspos;
	long free = 0;
	long offset = NO_SLOTS_FOLLOW;
	StreamEvent shared;
	StreamBufferSlot *result;
	Size size;
	MemoryContext oldcontext;
	Bitmapset *bms;
	StreamBufferSlot *sbs;

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
		/* wait for the last event to be read by all readers */
		while (HasPendingReads(*buf->prev));

		*buf->pos = buf->start;

		/* we need to make sure all readers are done reading before we begin clobbering entries */
		LWLockAcquire(StreamBufferLock, LW_EXCLUSIVE);

		*buf->last = NULL;
		*buf->tail = *buf->prev;

		 /* the buffer got full, so start a new append cycle */
		(*buf->prev)->nextoffset = BufferEnd(buf) - SlotEnd(*buf->prev);
		*buf->prev = NULL;
		free = 0;
		pos = *buf->pos;

		LWLockRelease(StreamBufferLock);
	}
	else
	{
		/*
		 * The current append cycle has enough room for this event, so append
		 * it directly after the previous event.
		 */
		if (IsNewAppendCycle(buf) || !MustEvict(buf))
			free = size;
		else if (MustEvict(buf))
			free = (*buf->prev)->nextoffset;
	}

	sbspos = pos + free;

/*
 * Scan for queued events that we can clobber to make room for the incoming event.
 * Note that the incoming event may need to clobber more than one
 * to fit, so we have to look at entire events before overwriting them
 */
	while (free < size)
	{
		int chunk;

		sbs = (StreamBufferSlot *) sbspos;
		wait_for_overwrite(buf, sbs);

		chunk = SlotSize(sbs) + sbs->nextoffset;
		free += chunk;
		sbspos += chunk;
	}

	/*
	 * If there is an event beyond the one we're about to append,
	 * we need to know how far it will be from the incoming event's
	 * slot, because this gap is garbage that can be consumed by the
	 * next append. This ensures that every append is contiguous.
	 */
	if (*buf->tail)
	{
		if (size < free)
			offset = free - size;
		else
			offset = 0;
	}

	MemSet(pos, 0, sizeof(StreamBufferSlot));
	result = (StreamBufferSlot *) pos;
	pos += sizeof(StreamBufferSlot);

	MemSet(pos, 0, sizeof(StreamEventData) + event->len);
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
	result->nextoffset = offset;

	if (*buf->prev)
		(*buf->prev)->nextoffset = 0;

	*buf->prev = result;
	*buf->pos = pos;
	*buf->last = pos;

	SpinLockInit(&result->mutex);

	return result;
}

/*
 * OpenStreamBuffer
 *
 * Indicate that the current process is currently writing to the given StreamBuffer
 */
extern void
OpenStreamBuffer(StreamBuffer *buf)
{
	SpinLockAcquire(&buf->mutex);
	buf->writers++;
	SpinLockRelease(&buf->mutex);
}

/*
 * CloseStreamBuffer
 *
 * Indicate that the current process is finished writing to the given StreamBuffer
 */
extern void
CloseStreamBuffer(StreamBuffer *buf)
{
	SpinLockAcquire(&buf->mutex);
	buf->writers--;
	SpinLockRelease(&buf->mutex);
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

	sbs = alloc_slot(stream, encoding, buf, ev);

	if (DebugPrintStreamBuffer)
	{
		/* did we wrap around? */
		if (prev + SlotSize(sbs) > *buf->pos)
			prev = buf->start;

		elog(LOG, "appended %dB at [%d, %d) +%d", SlotSize(sbs),
				BufferOffset(buf, prev), BufferOffset(buf, *buf->pos), sbs->nextoffset);
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
		MemSet(GlobalStreamBuffer->start, 0, size);

		GlobalStreamBuffer->pos = ShmemAlloc(sizeof(char *));
		*GlobalStreamBuffer->pos = GlobalStreamBuffer->start;

		GlobalStreamBuffer->last = ShmemAlloc(sizeof(char *));
		*GlobalStreamBuffer->last = NULL;

		GlobalStreamBuffer->prev = ShmemAlloc(sizeof(StreamBufferSlot *));
		*GlobalStreamBuffer->prev = NULL;

		GlobalStreamBuffer->tail = ShmemAlloc(sizeof(StreamBufferSlot *));
		*GlobalStreamBuffer->tail = NULL;
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

	LWLockAcquire(StreamBufferLock, LW_SHARED);
	reader->reading = true;

	/* TODO: advance the reader to the first relevant slot in the stream buffer */

	if (DebugPrintStreamBuffer)
		elog(LOG, "advanced reader %d past %d events", queryid, count);

	return reader;
}

/*
 * PinNextStreamEvent
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 * If this is the last reader that needs to see a given event, the event is deleted
 * from the stream buffer.
 */
extern StreamBufferSlot *
PinNextStreamEvent(StreamBufferReader *reader)
{
	StreamBuffer *buf = reader->buf;
	StreamBufferSlot *result = NULL;
	StreamBufferSlot *current = NULL;

	if (BufferUnchanged(reader))
	{
		if (reader->reading && buf->writers == 0)
		{
			/* the buffer hasn't changed and no writers are writing, so release the read lock */
			LWLockRelease(StreamBufferLock);
			reader->reading = false;
		}
		return NULL;
	}

	if (IsNewReadCycle(reader))
	{
		/* new data has been added to the buffer but we don't have a read lock yet, so get one */
		LWLockAcquire(StreamBufferLock, LW_SHARED);
		reader->reading = true;
	}

	if (HasUnreadData(reader))
	{
		/* new data since last read */
		current = (StreamBufferSlot *) reader->pos;
	}
	else if (AtBufferEnd(reader))
	{
		/* we've consumed all the data in the buffer, wrap around to start reading from the beginning */
		reader->reading = false;
		LWLockRelease(StreamBufferLock);
		reader->pos = buf->start;

		return NULL;
	}

	/*
	 * Advance the reader to the end of the current slot. Events are appended contiguously,
	 * so this is right where the next event will be.
	 */
	reader->pos += SlotSize(current);

	if (bms_is_member(reader->queryid, current->readby))
		result = current;

	if (result && DebugPrintStreamBuffer)
	{
		elog(LOG, "read event at [%d, %d)", BufferOffset(reader->buf, result),
				BufferOffset(reader->buf, result) + SlotSize(result));
	}

	return result;
}

/*
 * UnpinStreamEvent
 *
 * Marks the given slot as read by the given reader. Once all open readers
 * have unpinned a slot, it can be freed.
 */
extern void
UnpinStreamEvent(StreamBufferReader *reader, StreamBufferSlot *slot)
{
	volatile Bitmapset *bms = slot->readby;
	SpinLockAcquire(&slot->mutex);
	bms_del_member((Bitmapset *) bms, reader->queryid);
	SpinLockRelease(&slot->mutex);
}

extern void
ReadAndPrintStreamBuffer(StreamBuffer *buf, int32 queryid, int intervalms)
{
	StreamBufferReader *reader = OpenStreamBufferReader(buf, queryid);
	StreamBufferSlot *sbs;
	int count = 0;
	int size = 0;

	printf("====\n");
	while ((sbs = PinNextStreamEvent(reader)) != NULL)
	{
		count++;
		printf("size = %dB, stream = \"%s\", encoding = \"%s\" addr = %p\n",
				(int) SlotSize(sbs), sbs->stream, sbs->encoding, &(sbs->link));

		size += SlotSize(sbs);

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
				(int) SlotSize(sbs), sbs->stream, sbs->encoding);

		sbs = (StreamBufferSlot *)
				SHMQueueNext(&(buf->buf), &(sbs->link), offsetof(StreamBufferSlot, link));
	}
	LWLockRelease(StreamBufferLock);

	printf("\n%d events.\n", count);
	printf("^^^^\n");
}
