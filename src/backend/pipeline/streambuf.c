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
#include "nodes/bitmapset.h"
#include "nodes/print.h"
#include "storage/lwlock.h"
#include "storage/spalloc.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define MAGIC 0xDEADBABE /* x_x */
#define MAX_CQS 128 /* TODO(usmanm): Make this dynamic */

#define BufferOffset(ptr) ((int32) ((char *) (ptr) - GlobalStreamBuffer->start))
#define SlotEnd(slot) ((char *) (slot) + (slot)->size)
#define SlotNext(slot) ((StreamBufferSlot *) SlotEnd(slot))
#define NoUnreadSlots(reader) ((reader)->slot == GlobalStreamBuffer->head && (reader)->nonce == GlobalStreamBuffer->nonce)

/* Whether or not to print the state of the stream buffer as it changes */
bool DebugPrintStreamBuffer;

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
int StreamBufferBlocks;

static void
spfree_tupledesc(TupleDesc desc)
{
	spfree(desc);
}

/*
 * WaitForOverwrite
 *
 * Waits until the given slot has been read by all CQs that need to see it
 */
void
StreamBufferWaitOnSlot(StreamBufferSlot *slot, int sleepms)
{
	while (true)
	{
		/* Was slot overwritten? */
		if (slot->magic != MAGIC)
			break;
		/* Has the slot been read by all events? */
		if (slot < GlobalStreamBuffer->tail || bms_is_empty(slot->readers))
			break;
		pg_usleep(sleepms * 1000);
	}

	if (DebugPrintStreamBuffer)
	{
		elog(LOG, "evicted %zu bytes at [%d, %d)", slot->size,
				BufferOffset(slot), BufferOffset(SlotEnd(slot)));
	}
}

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
StreamBufferSlot *
StreamBufferInsert(const char *stream, StreamEvent *event)
{
	char *start;
	char *pos;
	char *end = GlobalStreamBuffer->start + GlobalStreamBuffer->size;
	Bitmapset *bms = GetTargetsFor(stream);
	StreamBufferSlot *slot;
	Size size;
	Size tupsize;

	if (bms == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}

	tupsize = event->tuple->t_len + HEAPTUPLESIZE;
	size = sizeof(StreamBufferSlot) + sizeof(StreamEvent) + tupsize +
			strlen(stream) + 1 + BITMAPSET_SIZE(bms->nwords);

	if (size > GlobalStreamBuffer->size)
		elog(ERROR, "event of size %zu too big for stream buffer of size %zu", size, GlobalStreamBuffer->size);

	LWLockAcquire(StreamBufferHeadLock, LW_EXCLUSIVE);
	LWLockAcquire(StreamBufferTailLock, LW_SHARED);

	/* If buffer is empty, start consuming it from the start again. */
	if (StreamBufferIsEmpty())
		start = GlobalStreamBuffer->start;
	else
		start = (char *) GlobalStreamBuffer->head;

	/*
	 * Not enough size at the end? Wait till all events in the buffer
	 * are read and then wrap around.
	 */
	if (size > end - start)
	{
		while (!StreamBufferIsEmpty())
		{
			LWLockRelease(StreamBufferTailLock);
			pg_usleep(500); /* 0.5ms */
			LWLockAcquire(StreamBufferTailLock, LW_SHARED);
		}

		start = GlobalStreamBuffer->start;
	}

	pos = start;

	/* Initialize the newly allocated slot and copy data into it. */
	MemSet(pos, 0, size);
	slot = (StreamBufferSlot *) pos;
	slot->magic = MAGIC;
	slot->size = size;
	SpinLockInit(&slot->mutex);

	pos += sizeof(StreamBufferSlot);
	slot->event = (StreamEvent *) pos;
	slot->event->desc = event->desc;
	slot->event->arrivaltime = event->arrivaltime;

	pos += sizeof(StreamEvent);
	slot->event->tuple = (HeapTuple) pos;
	slot->event->tuple->t_len = event->tuple->t_len;
	slot->event->tuple->t_self = event->tuple->t_self;
	slot->event->tuple->t_tableOid = event->tuple->t_tableOid;
	slot->event->tuple->t_data = (HeapTupleHeader) ((char *) slot->event->tuple + HEAPTUPLESIZE);
	memcpy(slot->event->tuple->t_data, event->tuple->t_data, event->tuple->t_len);

	pos += tupsize;
	slot->stream = pos;
	strcpy(slot->stream, stream);

	pos += strlen(stream) + 1;
	slot->readers = (Bitmapset *) pos;
	memcpy(slot->readers, bms, BITMAPSET_SIZE(bms->nwords));

	/* Move head forward */
	GlobalStreamBuffer->head = SlotNext(slot);

	/* Did we wrap around? */
	if (start == GlobalStreamBuffer->start)
	{
		LWLockRelease(StreamBufferTailLock);
		LWLockAcquire(StreamBufferTailLock, LW_EXCLUSIVE);

		GlobalStreamBuffer->tail = slot;
		GlobalStreamBuffer->nonce++;

		/* Wake up all readers */
		StreamBufferNotifyAllAndClearWaiters();
	}

	LWLockRelease(StreamBufferTailLock);
	LWLockRelease(StreamBufferHeadLock);

	if (DebugPrintStreamBuffer)
		elog(LOG, "appended %zu bytes at [%d, %d); readers: %d", slot->size,
				BufferOffset(slot), BufferOffset(SlotEnd(slot)), bms_num_members(slot->readers));

	return slot;
}

/*
 * StreamBufferShmemSize
 *
 * Retrieves the size in bytes of the stream buffer
 */
Size
StreamBufferShmemSize(void)
{
	return (StreamBufferBlocks * BLCKSZ) + sizeof(StreamBuffer);
}

/*
 * StreamBufferInit
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
void
StreamBufferInit(void)
{
	bool found;
	Size size = StreamBufferShmemSize();
	Size headersize = MAXALIGN(sizeof(StreamBuffer));

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer", headersize , &found);

	if (!found)
	{
		GlobalStreamBuffer->start = (char *) ShmemAlloc(size);
		GlobalStreamBuffer->size = size;
		MemSet(GlobalStreamBuffer->start, 0, size);

		GlobalStreamBuffer->head = (StreamBufferSlot *) GlobalStreamBuffer->start;
		GlobalStreamBuffer->tail = GlobalStreamBuffer->head;

		GlobalStreamBuffer->latches = (Latch *) spalloc0(sizeof(Latch) * MAX_CQS);
		GlobalStreamBuffer->waiters = (Bitmapset *) spalloc0(BITMAPSET_SIZE(MAX_CQS / BITS_PER_BITMAPWORD));
		GlobalStreamBuffer->waiters->nwords = MAX_CQS / BITS_PER_BITMAPWORD;

		SpinLockInit(&GlobalStreamBuffer->mutex);
	}

	LWLockRelease(PipelineMetadataLock);
}

/*
 * StreamBufferOpenReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
StreamBufferReader *
StreamBufferOpenReader(int id)
{
	StreamBufferReader *reader = (StreamBufferReader *) palloc(sizeof(StreamBufferReader));
	reader->id = id;
	reader->slot = GlobalStreamBuffer->tail;
	reader->nonce = GlobalStreamBuffer->nonce;
	reader->retry_slot = false;
	return reader;
}

/*
 * StreamBufferCloseReader
 */
void
StreamBufferCloseReader(StreamBufferReader *reader)
{
	pfree(reader);
}

/*
 * StreamBufferPinNextSlot
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 */
StreamBufferSlot *
StreamBufferPinNextSlot(StreamBufferReader *reader)
{
	if (StreamBufferIsEmpty())
		return NULL;

	if (NoUnreadSlots(reader))
	{
		reader->retry_slot = true;
		return NULL;
	}

	LWLockAcquire(StreamBufferTailLock, LW_SHARED);

	/* Did the stream buffer wrapped around? */
	if (reader->nonce < GlobalStreamBuffer->nonce)
	{
		reader->nonce = GlobalStreamBuffer->nonce;
		reader->slot = NULL;
	}

	/*
	 * If we're behind the tail, then pick tail as the next
	 * event, otherwise follow pointer to the next element.
	 * If the current_slot is NULL, then it automatically goes
	 * into the first check.
	 */
	if (reader->slot < GlobalStreamBuffer->tail)
		reader->slot = GlobalStreamBuffer->tail;
	else if (!reader->retry_slot)
		reader->slot = SlotNext(reader->slot);

	/* Return the first event in the buffer that we need to read from */
	while (true)
	{
		if (NoUnreadSlots(reader))
		{
			reader->retry_slot = true;
			LWLockRelease(StreamBufferTailLock);
			return NULL;
		}

		if (bms_is_member(reader->id, reader->slot->readers))
			break;

		reader->slot = SlotNext(reader->slot);
	}

	LWLockRelease(StreamBufferTailLock);

	if (DebugPrintStreamBuffer)
		elog(LOG, "[%d] pinned event at [%d, %d)",
				reader->id, BufferOffset(reader->slot), BufferOffset(SlotEnd(reader->slot)));

	reader->retry_slot = false;
	return reader->slot;
}

/*
 * StreamBufferUnpinSlot
 *
 * Marks the given slot as read by the given reader. Once all open readers
 * have unpinned a slot, it is freed.
 */
void
StreamBufferUnpinSlot(StreamBufferReader *reader, StreamBufferSlot *slot)
{
	SpinLockAcquire(&slot->mutex);
	bms_del_member(slot->readers, reader->id);
	SpinLockRelease(&slot->mutex);

	if (DebugPrintStreamBuffer)
		elog(LOG, "[%d] unpinned event at [%d, %d); readers %d",
				reader->id, BufferOffset(slot), BufferOffset(SlotEnd(slot)), bms_num_members(slot->readers));

	if (!bms_is_empty(slot->readers))
		return;

	LWLockAcquire(StreamBufferTailLock, LW_EXCLUSIVE);

	/*
	 * We're incrementing it because our refcount begins as negative
	 * for stream inserts--see comments in stream.c:InsertIntoStream.
	 */
	if (++slot->event->desc->tdrefcount == 0)
		spfree_tupledesc(slot->event->desc);

	/*
	 * If this slot was the tail, move tail ahead to the next slot that is not fully
	 * unpinned.
	 */
	if (GlobalStreamBuffer->tail == slot)
	{
		do
		{
			GlobalStreamBuffer->tail = SlotNext(GlobalStreamBuffer->tail);
		} while (!StreamBufferIsEmpty() && bms_is_empty(GlobalStreamBuffer->tail->readers));
	}

	LWLockRelease(StreamBufferTailLock);
}

/*
 * StreamBufferIsEmpty
 */
bool
StreamBufferIsEmpty(void)
{
	return (GlobalStreamBuffer->tail == GlobalStreamBuffer->head);
}

static void
clear_readers(Bitmapset *readers)
{
	int i;
	for (i = 0; i < readers->nwords; i++)
		readers->words[i] = 0;
}

static void
notify_readers(Bitmapset *readers)
{
	int32 id;
	while ((id = bms_first_member(readers)) >= 0)
		SetLatch((&GlobalStreamBuffer->latches[id]));
}

/*
 * StreamBufferWait
 */
void
StreamBufferWait(int32_t id)
{
	if (!StreamBufferIsEmpty())
		return;

	SpinLockAcquire(&GlobalStreamBuffer->mutex);
	bms_add_member(GlobalStreamBuffer->waiters, id);
	ResetLatch((&GlobalStreamBuffer->latches[id]));
	SpinLockRelease(&GlobalStreamBuffer->mutex);

	WaitLatch((&GlobalStreamBuffer->latches[id]), WL_LATCH_SET, 0);
}

/*
 * StreamBufferNotifyAllAndClearWaiters
 */
void
StreamBufferNotifyAllAndClearWaiters(void)
{
	Bitmapset *waiters;

	SpinLockAcquire(&GlobalStreamBuffer->mutex);
	waiters = bms_copy(GlobalStreamBuffer->waiters);
	clear_readers(GlobalStreamBuffer->waiters);
	SpinLockRelease(&GlobalStreamBuffer->mutex);

	notify_readers(waiters);
	bms_free(waiters);
}

/*
 * StreamBufferResetNotify
 */
void
StreamBufferResetNotify(int32_t id)
{
	ResetLatch((&GlobalStreamBuffer->latches[id]));
}

/*
 * StreamBufferNotify
 */
void
StreamBufferNotify(int32_t id)
{
	SetLatch((&GlobalStreamBuffer->latches[id]));
}
