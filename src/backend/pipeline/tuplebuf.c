/*-------------------------------------------------------------------------
 *
 * tuplebuf.c
 *
 *	  Interface for interacting with the tuple buffers
 *
 * src/backend/pipeline/tuplebuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "pipeline/cqproc.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/tuplebuf.h"
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
#define MURMUR_SEED 0x9eaca8149c92387e

#define BufferOffset(buf, ptr) ((int32) ((char *) (ptr) - (buf)->start))
#define BufferEnd(buf) ((buf)->start + (buf)->size)
#define SlotEnd(slot) ((char *) (slot) + (slot)->size)
#define SlotNext(slot) ((TupleBufferSlot *) SlotEnd(slot))
#define NoUnreadSlots(reader) ((reader)->slot == (reader)->buf->head && (reader)->slot_id == (reader)->buf->nonce)
#define SlotIsValid(slot) ((slot) && (slot)->magic == MAGIC)
#define SlotBehindTail(slot) (!(slot) || (slot) < (slot)->buf->tail || (slot)->id < (slot)->buf->nonce)
#define SlotEqualsTail(slot) ((slot) == (slot)->buf->tail && (slot)->id == (slot)->buf->nonce)

/* Whether or not to print the state of the stream buffer as it changes */
bool DebugPrintTupleBuffer;

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
TupleBuffer *WorkerTupleBuffer = NULL;
TupleBuffer *CombinerTupleBuffer = NULL;

/* Maximum size in blocks of the global stream buffer */
int TupleBufferBlocks;

int EmptyTupleBufferWaitTime;

static List *MyPinnedSlots = NIL;

typedef struct
{
	int32_t cq_id;
	TupleBufferSlot *slot;
} MyPinnedSlotEntry;

/*
 * MakeTuple
 */
Tuple *
MakeTuple(HeapTuple heaptup, TupleDesc desc)
{
	Tuple *t = palloc0(sizeof(Tuple));
	t->heaptup = heaptup;
	t->desc = desc;
	t->arrivaltime = GetCurrentTimestamp();
	return t;
}

/*
 * TupleBufferWaitOnSlot
 *
 * Waits until the given slot has been read by all CQs that need to see it
 */
void
TupleBufferWaitOnSlot(TupleBufferSlot *slot, int sleepms)
{
	while (true)
	{
		/* Was slot overwritten? */
		if (!SlotIsValid(slot))
			break;
		/* Has the slot been read by all events? */
		if (SlotBehindTail(slot) || bms_is_empty(slot->readby))
			break;
		pg_usleep(sleepms * 1000);
	}

	if (DebugPrintTupleBuffer)
	{
		elog(LOG, "evicted %zu bytes at [%d, %d)", slot->size,
				BufferOffset(slot->buf, slot), BufferOffset(slot->buf, SlotEnd(slot)));
	}
}

/*
 * TupleBufferInsert
 *
 * Appends a decoded event to the given stream buffer
 */
TupleBufferSlot *
TupleBufferInsert(TupleBuffer *buf, Tuple *tuple, Bitmapset *bms)
{
	char *start;
	char *pos;
	char *end;
	bool was_empty;
	TupleBufferSlot *slot;
	Size size;
	Size tupsize;

	if (bms == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}

	tupsize = tuple->heaptup->t_len + HEAPTUPLESIZE;
	size = sizeof(TupleBufferSlot) + sizeof(Tuple) + tupsize + BITMAPSET_SIZE(bms->nwords);

	if (size > buf->size)
		elog(ERROR, "event of size %zu too big for stream buffer of size %zu", size, WorkerTupleBuffer->size);

	LWLockAcquire(buf->head_lock, LW_EXCLUSIVE);
	LWLockAcquire(buf->tail_lock, LW_SHARED);

	/* If the buffer is empty, we'll have to reset the tail, so upgrade to an EXCLUSIVE lock. */
	if (TupleBufferIsEmpty(buf))
	{
		LWLockRelease(buf->tail_lock);
		LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

		buf->head = NULL;

		start = buf->start;
		end = BufferEnd(buf);
	}
	else
	{
		start = SlotEnd(buf->head);

		/* If the tail is infront of us, then don't go beyond it. */
		if (buf->tail > buf->head)
			end = buf->tail;
		else
		{
			end = BufferEnd(buf);

			/* If there is not enough space left in the buffer, wrap around. */
			if (size > end - start)
			{
				start = buf->start;
				end = buf->tail;
			}
		}

		/* If there isn't enough space, then wait for the tail to move on till there is enough. */
		while (size > end - start)
		{
			LWLockRelease(buf->tail_lock);
			pg_usleep(500); /* 0.5ms */
			LWLockAcquire(buf->tail_lock, LW_SHARED);

			if (TupleBufferIsEmpty(buf))
			{
				LWLockRelease(buf->tail_lock);
				LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

				buf->head = NULL;

				start = buf->start;
				end = BufferEnd(buf);
			}
			else
				end = buf->tail;
		}
	}

	was_empty = TupleBufferIsEmpty(buf);

	pos = start;

	/* Initialize the newly allocated slot and copy data into it. */
	MemSet(pos, 0, size);
	slot = (TupleBufferSlot *) pos;
	slot->magic = MAGIC;
	slot->id = ++buf->nonce;
	slot->next = NULL;
	slot->buf = buf;
	slot->size = size;
	SpinLockInit(&slot->mutex);

	pos += sizeof(TupleBufferSlot);
	slot->tuple = (Tuple *) pos;
	slot->tuple->desc = tuple->desc;
	slot->tuple->arrivaltime = tuple->arrivaltime;

	pos += sizeof(Tuple);
	slot->tuple->heaptup = (HeapTuple) pos;
	slot->tuple->heaptup->t_len = tuple->heaptup->t_len;
	slot->tuple->heaptup->t_self = tuple->heaptup->t_self;
	slot->tuple->heaptup->t_tableOid = tuple->heaptup->t_tableOid;
	slot->tuple->heaptup->t_data = (HeapTupleHeader) ((char *) slot->tuple->heaptup + HEAPTUPLESIZE);
	memcpy(slot->tuple->heaptup->t_data, tuple->heaptup->t_data, tuple->heaptup->t_len);

	pos += tupsize;
	slot->readby = (Bitmapset *) pos;
	memcpy(slot->readby, bms, BITMAPSET_SIZE(bms->nwords));

	/* Move head forward */
	if (buf->head)
	{
		buf->head->next = slot;
		buf->head = slot;
	}
	else
	{
		Assert(TupleBufferIsEmpty(buf));

		buf->head = slot;
		buf->tail = slot;
	}

	/* Notify all readers if we were empty. */
	if (was_empty)
		TupleBufferNotifyAndClearWaiters(buf);

	LWLockRelease(buf->tail_lock);
	LWLockRelease(buf->head_lock);

	if (DebugPrintTupleBuffer)
		elog(LOG, "appended %zu bytes at [%d, %d); readers: %d", slot->size,
				BufferOffset(buf, slot), BufferOffset(buf, SlotEnd(slot)), bms_num_members(slot->readby));

	return slot;
}

static Size
buffer_size()
{
	return (TupleBufferBlocks * BLCKSZ) + sizeof(TupleBuffer);
}

/*
 * TupleBuffersShmemSize
 *
 * Retrieves the size in bytes of the stream buffer
 */
Size
TupleBuffersShmemSize(void)
{
	return buffer_size() * 2;
}

void
TupleBuffersInit(void)
{
	WorkerTupleBuffer = TupleBufferInit("WorkerTupleBuffer", buffer_size(), WorkerBufferHeadLock, WorkerBufferTailLock, MAX_PARALLELISM);
	CombinerTupleBuffer = TupleBufferInit("CombinerTupleBuffer", buffer_size(), CombinerBufferHeadLock, CombinerBufferTailLock, 1);
}

/*
 * TupleBufferInit
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
TupleBuffer *
TupleBufferInit(char *name, Size size, LWLock *head_lock, LWLock *tail_lock, uint8_t max_readers)
{
	bool found;
	Size headersize = MAXALIGN(sizeof(TupleBuffer));
	TupleBuffer *buf;

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	buf = ShmemInitStruct(name, headersize , &found);

	if (!found)
	{
		buf->name = ShmemAlloc(strlen(name) + 1);
		strcpy(buf->name, name);
		buf->head_lock = head_lock;
		buf->tail_lock = tail_lock;
		buf->max_readers = max_readers;

		buf->start = (char *) ShmemAlloc(size);
		buf->size = size;
		MemSet(buf->start, 0, size);

		buf->head = NULL;
		buf->tail = NULL;

		buf->latches = (Latch **) spalloc0(sizeof(Latch *) * MAX_CQS);
		buf->waiters = (Bitmapset *) spalloc0(BITMAPSET_SIZE(MAX_CQS / BITS_PER_BITMAPWORD));
		buf->waiters->nwords = MAX_CQS / BITS_PER_BITMAPWORD;

		SpinLockInit(&buf->mutex);
	}

	LWLockRelease(PipelineMetadataLock);

	return buf;
}

/*
 * TupleBufferOpenReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
TupleBufferReader *
TupleBufferOpenReader(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, uint8_t num_readers)
{
	TupleBufferReader *reader = (TupleBufferReader *) palloc(sizeof(TupleBufferReader));
	reader->buf = buf;
	reader->cq_id = cq_id;
	reader->reader_id = reader_id;
	reader->num_readers = num_readers;
	reader->slot = NULL;
	reader->slot_id = 0;
	reader->retry_slot = false;
	return reader;
}

/*
 * TupleBufferCloseReader
 */
void
TupleBufferCloseReader(TupleBufferReader *reader)
{
	pfree(reader);
}

/*
 * TupleBufferPinNextSlot
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 */
TupleBufferSlot *
TupleBufferPinNextSlot(TupleBufferReader *reader)
{
	MyPinnedSlotEntry *entry;
	MemoryContext oldcontext;

	if (TupleBufferIsEmpty(reader->buf))
		return NULL;

	if (NoUnreadSlots(reader))
	{
		reader->retry_slot = true;
		return NULL;
	}

	LWLockAcquire(reader->buf->tail_lock, LW_SHARED);

	/* Did the stream buffer wrapped around? */
	if (reader->nonce < reader->buf->nonce)
	{
		reader->nonce = reader->buf->nonce;
		reader->slot = NULL;
	}

	/*
	 * If we're behind the tail, then pick tail as the next
	 * event, otherwise follow pointer to the next element.
	 * If the current_slot is NULL, then it automatically goes
	 * into the first check.
	 */
	if (SlotBehindTail(reader->slot))
		reader->slot = reader->buf->tail;
	else if (!reader->retry_slot)
		reader->slot = SlotNext(reader->slot);

	/* Return the first event in the buffer that we need to read from */
	while (true)
	{
		if (NoUnreadSlots(reader))
		{
			reader->retry_slot = true;
			LWLockRelease(reader->buf->tail_lock);
			return NULL;
		}

		if (bms_is_member(reader->cq_id, reader->slot->readby) &&
				(JumpConsistentHash((uint64_t) reader->slot, reader->num_readers) == reader->reader_id))
			break;

		reader->slot = SlotNext(reader->slot);
	}

	LWLockRelease(reader->buf->tail_lock);

	Assert(SlotIsValid(slot));

	if (DebugPrintTupleBuffer)
		elog(LOG, "[%d] pinned event at [%d, %d)",
				reader->cq_id, BufferOffset(reader->buf, reader->slot), BufferOffset(reader->buf, SlotEnd(reader->slot)));

	oldcontext = MemoryContextSwitchTo(CQExecutionContext);

	entry = palloc0(sizeof(MyPinnedSlotEntry));
	entry->slot = reader->slot;
	entry->cq_id = reader->cq_id;
	MyPinnedSlots = lappend(MyPinnedSlots, entry);

	MemoryContextSwitchTo(oldcontext);

	reader->retry_slot = false;
	return reader->slot;
}

static void
unpin_slot(int32_t cq_id, TupleBufferSlot *slot)
{
	if (!SlotIsValid(slot) || SlotBehindTail(slot))
		return;

	SpinLockAcquire(&slot->mutex);
	bms_del_member(slot->readby, cq_id);
	SpinLockRelease(&slot->mutex);

	if (DebugPrintTupleBuffer)
		elog(LOG, "[%d] unpinned event at [%d, %d); readers %d",
				cq_id, BufferOffset(slot->buf, slot), BufferOffset(slot->buf, SlotEnd(slot)), bms_num_members(slot->readby));

	if (!bms_is_empty(slot->readby))
		return;

	LWLockAcquire(slot->buf->tail_lock, LW_EXCLUSIVE);

	/*
	 * We're incrementing it because our refcount begins as negative
	 * for stream inserts--see comments in stream.c:InsertIntoStream.
	 */
	if (slot->tuple->desc)
		if (++slot->tuple->desc->tdrefcount == 0)
			spfree(slot->tuple->desc);

	/*
	 * If this slot was the tail, move tail ahead to the next slot that is not fully
	 * unpinned.
	 */
	if (SlotEqualsTail(slot))
	{
		do
			slot->buf->tail = slot->buf->tail->next;
		while (!TupleBufferIsEmpty(slot->buf) && bms_is_empty(slot->buf->tail->readby));
	}

	LWLockRelease(slot->buf->tail_lock);
}

/*
 * TupleBufferUnpinSlot
 *
 * Marks the given slot as read by the given reader. Once all open readers
 * have unpinned a slot, it is freed.
 */
void
TupleBufferUnpinSlot(TupleBufferReader *reader, TupleBufferSlot *slot)
{
	unpin_slot(reader->cq_id, slot);
}

/*
 * TupleBufferIsEmpty
 */
bool
TupleBufferIsEmpty(TupleBuffer *buf)
{
	return (buf->tail == NULL);
}

void
TupleBufferInitLatch(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, Latch *proclatch)
{
	memcpy(&buf->latches[cq_id][reader_id], proclatch, sizeof(Latch));
}

static void
clear_readers(Bitmapset *readers)
{
	int i;
	for (i = 0; i < readers->nwords; i++)
		readers->words[i] = 0;
}

static void
notify_readers(TupleBuffer *buf, Bitmapset *readers)
{
	int32 id;
	while ((id = bms_first_member(readers)) >= 0)
		TupleBufferNotify(buf, id);
}

/*
 * TupleBufferWait
 */
void
TupleBufferWait(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id)
{
	SpinLockAcquire(&buf->mutex);

	if (!TupleBufferIsEmpty(buf))
	{
		SpinLockRelease(&buf->mutex);
		return;
	}

	bms_add_member(buf->waiters, cq_id);
	ResetLatch((&buf->latches[cq_id][reader_id]));
	SpinLockRelease(&buf->mutex);

	WaitLatch((&buf->latches[cq_id][reader_id]), WL_LATCH_SET, 0);
}

/*
 * TupleBufferNotifyAndClearWaiters
 */
void
TupleBufferNotifyAndClearWaiters(TupleBuffer *buf)
{
	Bitmapset *waiters;

	SpinLockAcquire(&buf->mutex);
	waiters = bms_copy(buf->waiters);
	clear_readers(buf->waiters);
	SpinLockRelease(&buf->mutex);

	notify_readers(buf, waiters);
	bms_free(waiters);
}

/*
 * TupleBufferResetNotify
 */
void
TupleBufferResetNotify(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id)
{
	ResetLatch((&buf->latches[cq_id][reader_id]));
}

/*
 * TupleBufferNotify
 */
void
TupleBufferNotify(TupleBuffer *buf, uint32_t cq_id)
{
	int i;

	for (i = 0; i < buf->max_readers; i++)
	{
		Latch *l = &buf->latches[cq_id][i];
		if (!l->owner_pid)
			break;
		SetLatch(l);
	}
}

/*
 * TupleBufferUnpinAllPinnedSlots
 */
void
TupleBufferUnpinAllPinnedSlots(void)
{
	ListCell *lc;

	if (!MyPinnedSlots)
		return;

	foreach(lc, MyPinnedSlots)
	{
		MyPinnedSlotEntry *entry = (MyPinnedSlotEntry *) lfirst(lc);
		unpin_slot(entry->cq_id, entry->slot);
	}

	MyPinnedSlots = NIL;
}

/*
 * TupleBufferClearPinnedSlots
 */
void
TupleBufferClearPinnedSlots(void)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(CQExecutionContext);

	list_free_deep(MyPinnedSlots);
	MyPinnedSlots = NIL;

	MemoryContextSwitchTo(oldcontext);
}
