/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Interface for interacting with the stream buffer
 *
 * src/backend/pipeline/streambuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "catalog/pipeline_stream_fn.h"
#include "pipeline/stream.h"
#include "pipeline/streambuf.h"
#include "postmaster/bgworker.h"
#include "executor/tuptable.h"
#include "nodes/print.h"
#include "storage/lwlock.h"
#include "storage/spalloc.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define NO_SLOTS_FOLLOW -1

/* Whether or not to print the state of the stream buffer as it changes */
bool DebugPrintStreamBuffer;

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
int StreamBufferBlocks;

static void
spfree_tupledesc(TupleDesc desc)
{
// FIXME(derekjn) these spfree calls seem to be
// causing spalloc calls from other procs to fail when
// each is being called frequently
//
//	int i;
//	for (i=0; i<desc->natts; i++)
//		spfree(desc->attrs[i]);
//
//	spfree(desc->attrs);
//	spfree(desc);
}

/*
 * WaitForOverwrite
 *
 * Waits until the given slot has been read by all CQs that need to see it
 */
void
WaitForOverwrite(StreamBuffer *buf, StreamBufferSlot *slot, int sleepms)
{
	/* block until all CQs have marked this event as read */
	while (HasPendingReads(slot))
		pg_usleep(sleepms * 1000);

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
alloc_slot(const char *stream, StreamBuffer *buf, StreamEvent event)
{
	char *pos;
	char *sbspos;
	long free = 0;
	long offset = NO_SLOTS_FOLLOW;
	StreamEvent shared;
	StreamBufferSlot *result;
	Size size;
	Size rawsize = event->raw->t_len + HEAPTUPLESIZE;
	Bitmapset *bms = GetTargetsFor(stream);
	StreamBufferSlot *sbs;

	if (bms == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}

	size = sizeof(StreamEventData) + rawsize + sizeof(StreamBufferSlot) +
			strlen(stream) + 1 + BITMAPSET_SIZE(bms->nwords);

	if (size > buf->capacity)
		elog(ERROR, "event of size %d too big for stream buffer of size %d", (int) size, (int) buf->capacity);

	LWLockAcquire(StreamBufferAppendLock, LW_EXCLUSIVE);
	pos = buf->pos;

	if (pos + size > BufferEnd(buf))
	{
		/* wait for the last event to be read by all readers */
		WaitForOverwrite(buf, buf->prev, 0);

		buf->pos = buf->start;

		/* we need to make sure all readers are done reading before we begin clobbering entries */
		LWLockAcquire(StreamBufferWrapLock, LW_EXCLUSIVE);

		buf->tail = buf->prev;

		 /* the buffer got full, so start a new append cycle */
		buf->prev->nextoffset = BufferEnd(buf) - SlotEnd(buf->prev);
		buf->prev = NULL;
		free = 0;
		pos = buf->pos;

		LWLockRelease(StreamBufferWrapLock);
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
			free = buf->prev->nextoffset;
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
		WaitForOverwrite(buf, sbs, 0);

		chunk = sbs->len + sbs->nextoffset;
		free += chunk;
		sbspos += chunk;
	}

	/*
	 * If there is an event beyond the one we're about to append,
	 * we need to know how far it will be from the incoming event's
	 * slot, because this gap is garbage that can be consumed by the
	 * next append. This ensures that every append is contiguous.
	 */
	if (buf->tail)
	{
		if (size < free)
			offset = free - size;
		else
			offset = 0;
	}

	MemSet(pos, 0, free);
	result = (StreamBufferSlot *) pos;
	pos += sizeof(StreamBufferSlot);

	MemSet(pos, 0, sizeof(StreamEventData) + rawsize);
	shared = (StreamEvent) pos;
	shared->desc = event->desc;
	shared->arrivaltime = event->arrivaltime;
	pos += sizeof(StreamEventData);

	shared->raw = (HeapTuple) pos;
	shared->raw->t_len = event->raw->t_len;
	shared->raw->t_self = event->raw->t_self;
	shared->raw->t_tableOid = event->raw->t_tableOid;
	shared->raw->t_data = (HeapTupleHeader) ((char *) shared->raw + HEAPTUPLESIZE);

	memcpy(shared->raw->t_data, event->raw->t_data, event->raw->t_len);
	pos += rawsize;

	result->event = shared;
	result->stream = pos;
	memcpy(result->stream, stream, strlen(stream) + 1);
	pos += strlen(stream) + 1;

	/*
	 * The source bitmap is allocated in local memory (in GetStreamTargets),
	 * but the delete operations that will be performed on it should never
	 * free anything, so it's safe (although a bit sketchy) to copy it into
	 * shared memory.
	 */
	result->readby = (Bitmapset *) pos;
	memcpy(result->readby, bms, BITMAPSET_SIZE(bms->nwords));
	pos += BITMAPSET_SIZE(bms->nwords);

	result->len = size;
	result->nextoffset = offset;

	if (buf->prev)
		buf->prev->nextoffset = 0;

	buf->prev = result;
	buf->pos = pos;
	buf->last = pos;

	SpinLockAcquire(&buf->mutex);

	/*
	 * Stream event appended, now signal workers waiting on this stream
	 * only if the buffer went from empty to not empty
	 */
	if (buf->empty)
	{
		int32 latchCtr;
		Bitmapset* toSignal = bms_copy(bms);
 		while ((latchCtr = bms_first_member(toSignal)) >= 0)
		{
			SetStreamBufferLatch(latchCtr);
		}
		bms_free(toSignal);
		buf->empty = false;
	}

	/* increment the unread counter */
	buf->unread++;
	SpinLockRelease(&buf->mutex);

	SpinLockInit(&result->mutex);

	LWLockRelease(StreamBufferAppendLock);

	return result;
}

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
StreamBufferSlot *
AppendStreamEvent(const char *stream, StreamBuffer *buf, StreamEvent ev)
{
	StreamBufferSlot *sbs;
	char *prev = buf->pos;

	sbs = alloc_slot(stream, buf, ev);

	if (DebugPrintStreamBuffer)
	{
		/* did we wrap around? */
		if (prev + SlotSize(sbs) > buf->pos)
			prev = buf->start;

		elog(LOG, "appended %dB at [%d, %d) +%d", SlotSize(sbs),
				BufferOffset(buf, prev), BufferOffset(buf, buf->pos), sbs->nextoffset);
	}

	return sbs;
}

/*
 * StreamBufferSize
 *
 * Retrieves the size in bytes of the stream buffer
 */
Size
StreamBufferShmemSize(void)
{
	return (StreamBufferBlocks * BLCKSZ) + sizeof(StreamBuffer);
}

/*
 * StreamIsBeingRead
 *
 * Returns true if at least one continuous query is reading from the given stream
 */
bool
IsInputStream(const char *stream)
{
	return GetTargetsFor(stream) != NULL;
}

/*
 * InitGlobalStreamBuffer
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
void
InitGlobalStreamBuffer(void)
{
	bool found;
	Size size = StreamBufferShmemSize();
	Size headersize = MAXALIGN(sizeof(StreamBuffer));

	LWLockAcquire(StreamBufferAppendLock, LW_EXCLUSIVE);

	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer", headersize , &found);
	GlobalStreamBuffer->capacity = size;

	if (!found)
	{
		SHMQueueInit(&(GlobalStreamBuffer->buf));

		GlobalStreamBuffer->start = (char *) ShmemAlloc(size);
		MemSet(GlobalStreamBuffer->start, 0, size);

		GlobalStreamBuffer->pos = GlobalStreamBuffer->start;
		GlobalStreamBuffer->last = NULL;
		GlobalStreamBuffer->prev = NULL;
		GlobalStreamBuffer->tail = NULL;

		GlobalStreamBuffer->unread = 0;
		GlobalStreamBuffer->empty = true;

		SpinLockInit(&GlobalStreamBuffer->mutex);
	}

	LWLockRelease(StreamBufferAppendLock);
}

/*
 * OpenStreamBufferReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
StreamBufferReader *
OpenStreamBufferReader(StreamBuffer *buf, int queryid)
{
	StreamBufferReader *reader = (StreamBufferReader *) palloc(sizeof(StreamBufferReader));
	reader->queryid = queryid;
	reader->buf = buf;
	reader->pos = buf->start;

	LWLockAcquire(StreamBufferWrapLock, LW_SHARED);
	reader->reading = true;

	return reader;
}

void
CloseStreamBufferReader(StreamBufferReader *reader)
{
	/* currently every reader gets its own process */
	LWLockRelease(StreamBufferWrapLock);
	pfree(reader);
}

/*
 * PinNextStreamEvent
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 * If this is the last reader that needs to see a given event, the event is deleted
 * from the stream buffer.
 */
StreamBufferSlot *
PinNextStreamEvent(StreamBufferReader *reader)
{
	StreamBuffer *buf = reader->buf;
	StreamBufferSlot *result = NULL;
	StreamBufferSlot *current = NULL;

	if (BufferUnchanged(reader))
		return NULL;

	if (ReaderNeedsWrap(buf, reader))
	{
		reader->pos = buf->start;
		reader->reading = false;
	}

	if (IsNewReadCycle(reader))
	{
		/* new data has been added to the buffer but we don't have a read lock yet, so get one */
		LWLockAcquire(StreamBufferWrapLock, LW_SHARED);
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
		reader->pos = buf->start;
		LWLockRelease(StreamBufferWrapLock);

		return NULL;
	}

	if (current == NULL)
		return NULL;

	/*
	 * Advance the reader to the end of the current slot. Events are appended contiguously,
	 * so this is right where the next event will be.
	 */
	reader->pos += current->len;

	if (bms_is_member(reader->queryid, current->readby))
		result = current;

	if (result && DebugPrintStreamBuffer)
	{
		elog(LOG, "read event at [%d, %d)", BufferOffset(reader->buf, result),
				BufferOffset(reader->buf, reader->pos));
	}

	return result;
}

/*
 * UnpinStreamEvent
 *
 * Marks the given slot as read by the given reader. Once all open readers
 * have unpinned a slot, it can be freed.
 */
void
UnpinStreamEvent(StreamBufferReader *reader, StreamBufferSlot *slot)
{
	Bitmapset *bms = slot->readby;

	SpinLockAcquire(&slot->mutex);

	bms_del_member(bms, reader->queryid);

	/*
	 * If this is the last reader that needs to read this event,
	 * decrement the unread count.
	 */
	if (bms_is_empty(bms))
	{
		SpinLockAcquire(&(reader->buf->mutex));

		reader->buf->unread--;

		if (reader->buf->unread == 0)
			reader->buf->empty = true;

		SpinLockRelease(&(reader->buf->mutex));

		/*
		 * This increment operation is safe because it will only ever happen
		 * from a single process. Namely, the process that is the last reader
		 * for this event.
		 */
		slot->event->desc->tdrefcount++;
		if (slot->event->desc->tdrefcount == 0)
			spfree_tupledesc(slot->event->desc);
	}

	SpinLockRelease(&slot->mutex);
}

void
ResetStreamBufferLatch(int32 id)
{
	ResetLatch((&GlobalStreamBuffer->procLatch[id]));
}

void
WaitOnStreamBufferLatch(int32 id)
{
	WaitLatch((&GlobalStreamBuffer->procLatch[id]), WL_LATCH_SET | WL_TIMEOUT, 500);
}

void
SetStreamBufferLatch(int32 id)
{
	SetLatch((&GlobalStreamBuffer->procLatch[id]));
}
