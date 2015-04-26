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
#include "libpq/pqformat.h"
#include "pipeline/cont_xact.h"
#include "pipeline/cqproc.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/tuplebuf.h"
#include "postmaster/bgworker.h"
#include "executor/tuptable.h"
#include "nodes/bitmapset.h"
#include "nodes/print.h"
#include "storage/dsm_alloc.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "miscadmin.h"

#define MAGIC 0xDEADBABE /* x_x */
#define MURMUR_SEED 0x9eaca8149c92387e
#define WAIT_SLEEP_MS 5

#define BufferOffset(buf, ptr) ((int32) ((char *) (ptr) - (buf)->start))
#define BufferEnd(buf) ((buf)->start + (buf)->size)
#define SlotEnd(slot) ((char *) (slot) + (slot)->size)
#define SlotNext(slot) ((TupleBufferSlot *) SlotEnd(slot))
#define NoUnreadSlots(reader) ((reader)->slot_id == (reader)->buf->head_id)
#define SlotIsValid(slot) ((slot) && (slot)->magic == MAGIC)
#define SlotEqualsTail(slot) ((slot) == (slot)->buf->tail && (slot)->id == (slot)->buf->tail_id)
#define HasEnoughSize(start, end, size) ((intptr_t) size <= ((intptr_t) end - (intptr_t) start))

TupleBuffer *WorkerTupleBuffer = NULL;
TupleBuffer *CombinerTupleBuffer = NULL;

/* GUC parameters */
bool debug_tuple_stream_buffer;
int tuple_buffer_blocks;
int empty_tuple_buffer_wait_time;

static List *MyPinnedSlots = NIL;
static List *MyReaders = NIL;

typedef struct
{
	int32_t cq_id;
	TupleBufferSlot *slot;
} MyPinnedSlotEntry;

/*
 * MakeTuple
 */
Tuple *
MakeTuple(HeapTuple heaptup, TupleDesc desc, int num_acks, StreamBatchAck *acks)
{
	Tuple *t = palloc0(sizeof(Tuple));
	t->heaptup = heaptup;
	t->desc = PackTupleDesc(desc);
	t->arrivaltime = GetCurrentTimestamp();
	t->num_acks = num_acks;
	t->acks = acks;
	return t;
}

static void try_move_tail(TupleBuffer *buf, TupleBufferSlot *tail)
{
	LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

	if (!SlotIsValid(tail))
	{
		LWLockRelease(buf->tail_lock);
		return;
	}

	/*
	 * If this slot was the tail, move tail ahead to the next slot that is not fully
	 * unpinned.
	 */
	if (SlotEqualsTail(tail))
	{
		do
		{
			buf->tail->magic = 0;
			buf->tail = buf->tail->next;

			if (TupleBufferIsEmpty(buf))
				break;

			if (buf->writer_latch.owner_pid)
				SetLatch(&buf->writer_latch);

			buf->tail_id = buf->tail->id;
		}
		while (bms_is_empty(buf->tail->readers));
	}

	LWLockRelease(buf->tail_lock);
}

/*
 * TupleBufferWaitOnSlot
 *
 * Waits until the tail has passed this slot. This guarantees that any tuples inserted uptill
 * (and including) this slot have been unpinned.
 */
void
TupleBufferWaitOnSlot(TupleBuffer *buf, TupleBufferSlot *slot)
{
	while (true)
	{
		TupleBufferSlot *tail;

		/* TODO(usmanm): Could this result in a live lock if slot is repeatedly equal to tail? */
		if (!SlotIsValid(slot))
			break;

		tail = buf->tail;
		if (SlotIsValid(tail) && bms_is_empty(tail->readers))
			try_move_tail(buf, tail);

		pg_usleep(WAIT_SLEEP_MS * 1000);
	}

	if (debug_tuple_stream_buffer)
		elog(LOG, "evicted %zu bytes at [%d, %d)", slot->size,
				BufferOffset(slot->buf, slot), BufferOffset(slot->buf, SlotEnd(slot)));
}

/*
 * TupleBufferInsert
 *
 * Appends a decoded event to the given stream buffer
 */
TupleBufferSlot *
TupleBufferInsert(TupleBuffer *buf, Tuple *tuple, Bitmapset *readers)
{
	char *start;
	char *pos;
	char *end;
	TupleBufferSlot *slot;
	Size size;
	Size tupsize;
	Size acks_size;
	int desclen = tuple->desc ? VARSIZE(tuple->desc) : 0;
	TimestampTz start_wait;

	if (readers == NULL)
	{
		/* nothing is reading from this stream, so it's a noop */
		return NULL;
	}

	tupsize = tuple->heaptup->t_len + HEAPTUPLESIZE;
	acks_size = sizeof(StreamBatchAck) * tuple->num_acks;
	size = sizeof(TupleBufferSlot) + sizeof(Tuple) + tupsize + BITMAPSET_SIZE(readers->nwords) + desclen + acks_size;

	if (size > buf->size)
		elog(ERROR, "event of size %zu too big for stream buffer of size %zu", size, WorkerTupleBuffer->size);

	LWLockAcquire(buf->head_lock, LW_EXCLUSIVE);
	LWLockAcquire(buf->tail_lock, LW_SHARED);

	memcpy(&buf->writer_latch, &MyProc->procLatch, sizeof(Latch));

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
			end = (char *) buf->tail;
		else
		{
			end = BufferEnd(buf);

			/* If there is not enough space left in the buffer, wrap around. */
			if (!HasEnoughSize(start, end, size))
			{
				start = buf->start;
				end = (char *) buf->tail;
			}
		}

		start_wait = GetCurrentTimestamp();

		/* If there isn't enough space, then wait for the tail to move on till there is enough. */
		while (!HasEnoughSize(start, end, size))
		{
			LWLockRelease(buf->tail_lock);

			if (TimestampDifferenceExceeds(start_wait, GetCurrentTimestamp(), empty_tuple_buffer_wait_time))
				pg_usleep(WAIT_SLEEP_MS * 1000);
			else
				WaitLatch((&buf->writer_latch), WL_LATCH_SET, 0);

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
				end = (char *) buf->tail;
		}
	}

	pos = start;

	/* Initialize the newly allocated slot and copy data into it. */
	MemSet(pos, 0, size);
	slot = (TupleBufferSlot *) pos;
	slot->id = buf->head_id + 1;
	slot->next = NULL;
	slot->buf = buf;
	slot->size = size;
	SpinLockInit(&slot->mutex);

	pos += sizeof(TupleBufferSlot);
	slot->tuple = (Tuple *) pos;
	memcpy(slot->tuple, tuple, sizeof(Tuple));
	pos += sizeof(Tuple);

	if (tuple->desc)
	{
		slot->tuple->desc = (bytea *) pos;
		memcpy(slot->tuple->desc, tuple->desc, desclen);
		pos += desclen;
	}

	slot->tuple->heaptup = (HeapTuple) pos;
	slot->tuple->heaptup->t_len = tuple->heaptup->t_len;
	slot->tuple->heaptup->t_self = tuple->heaptup->t_self;
	slot->tuple->heaptup->t_tableOid = tuple->heaptup->t_tableOid;
	slot->tuple->heaptup->t_data = (HeapTupleHeader) ((char *) slot->tuple->heaptup + HEAPTUPLESIZE);
	memcpy(slot->tuple->heaptup->t_data, tuple->heaptup->t_data, tuple->heaptup->t_len);
	pos += tupsize;

	slot->tuple->acks = (StreamBatchAck *) pos;
	memcpy(slot->tuple->acks, tuple->acks, acks_size);
	pos += acks_size;

	slot->readers = (Bitmapset *) pos;
	memcpy(slot->readers, readers, BITMAPSET_SIZE(readers->nwords));

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
		buf->tail_id = slot->id;
	}

	slot->magic = MAGIC;
	buf->head_id = buf->head->id;

	/* Notify any readers waiting for a CQ that is set for this slot. */
	SpinLockAcquire(&buf->mutex);
	if (!bms_is_empty(buf->waiters))
	{
		Bitmapset *notify = bms_intersect(slot->readers, buf->waiters);

		if (!bms_is_empty(notify))
		{
			int i;

			buf->waiters = bms_del_members(buf->waiters, notify);

			while ((i = bms_first_member(notify)) >= 0)
				TupleBufferNotify(buf, i);
		}

		bms_free(notify);
	}
	SpinLockRelease(&buf->mutex);

	/* Mark this latch as invalid. */
	buf->writer_latch.owner_pid = 0;

	LWLockRelease(buf->tail_lock);
	LWLockRelease(buf->head_lock);

	if (debug_tuple_stream_buffer)
		elog(LOG, "appended %zu bytes at [%d, %d); readers: %d", slot->size,
				BufferOffset(buf, slot), BufferOffset(buf, SlotEnd(slot)), bms_num_members(slot->readers));

	return slot;
}

static Size
buffer_size()
{
	return (tuple_buffer_blocks * BLCKSZ) + sizeof(TupleBuffer);
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

		buf->head_id = buf->tail_id = 0;
		buf->head = buf->tail = NULL;

		buf->waiters = NULL;
		buf->latches = dsm_array_new(sizeof(Latch) * max_readers);

		SpinLockInit(&buf->mutex);

		MemSet(&buf->writer_latch, 0, sizeof(Latch));
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

	MyReaders = lappend(MyReaders, reader);

	return reader;
}

/*
 * TupleBufferCloseReader
 */
void
TupleBufferCloseReader(TupleBufferReader *reader)
{
	MyReaders = list_delete(MyReaders, reader);
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
	int i;
	StreamBatchAck *acks;
	MemoryContext oldcontext;

	if (NoUnreadSlots(reader))
		return NULL;

	if (TupleBufferIsEmpty(reader->buf))
		return NULL;

	LWLockAcquire(reader->buf->tail_lock, LW_SHARED);

	/* Maybe the buffer became empty while we were trying to acquire the tail_lock? */
	if (TupleBufferIsEmpty(reader->buf))
	{
		LWLockRelease(reader->buf->tail_lock);
		return NULL;
	}

	/*
	 * If we're behind the tail, then pick tail as the next
	 * slot, otherwise follow pointer to the next slot.
	 */
	if (reader->slot_id < reader->buf->tail_id)
		reader->slot = reader->buf->tail;
	else
		reader->slot = reader->slot->next;

	/* Update slot_id to the most recent slot we've considered */
	reader->slot_id = reader->slot->id;

	/* Return the first event in the buffer that we need to read from */
	while (true)
	{
		/* Is this a slot for me? */
		if (bms_is_member(reader->cq_id, reader->slot->readers) &&
				(JumpConsistentHash((uint64_t) reader->slot, reader->num_readers) == reader->reader_id))
			break;

		if (NoUnreadSlots(reader))
		{
			LWLockRelease(reader->buf->tail_lock);
			return NULL;
		}

		/* Move over to the next slot. */
		reader->slot = reader->slot->next;
		reader->slot_id = reader->slot->id;
	}

	LWLockRelease(reader->buf->tail_lock);

	Assert(SlotIsValid(slot));

	if (debug_tuple_stream_buffer)
		elog(LOG, "[%d] pinned event at [%d, %d)",
				reader->cq_id, BufferOffset(reader->buf, reader->slot), BufferOffset(reader->buf, SlotEnd(reader->slot)));

	oldcontext = MemoryContextSwitchTo(CQExecutionContext);

	entry = palloc0(sizeof(MyPinnedSlotEntry));
	entry->slot = reader->slot;
	entry->cq_id = reader->cq_id;
	MyPinnedSlots = lappend(MyPinnedSlots, entry);

	acks = entry->slot->tuple->acks;

	for (i = 0; i < entry->slot->tuple->num_acks; i++) {
		StreamBatchAck ack = acks[i];
		ListCell *lc;
		bool found = false;

		if (!dsm_is_valid_ptr(ack.batch) || ack.batch_id != ack.batch->id)
			continue;

		foreach(lc, MyAcks) {
			StreamBatchAck *ack2 = lfirst(lc);
			if (ack.batch_id == ack2->batch_id) {
				ack2->count += ack.count;
				found = true;
			}
		}

		if (!found) {
			StreamBatchAck *ack2 = (StreamBatchAck *) palloc(sizeof(StreamBatchAck));
			memcpy(ack2, &ack, sizeof(StreamBatchAck));
			MyAcks = lappend(MyAcks, ack2);
		}
	}

	MemoryContextSwitchTo(oldcontext);

	return reader->slot;
}

static void
unpin_slot(int32_t cq_id, TupleBufferSlot *slot)
{
	TupleBuffer *buf;
	int i;

	if (!SlotIsValid(slot))
		return;

	buf = slot->buf;

	SpinLockAcquire(&slot->mutex);
	bms_del_member(slot->readers, cq_id);
	SpinLockRelease(&slot->mutex);

	for (i = 0; i < slot->tuple->num_acks; i++)
		StreamBatchIncrementNumReads(slot->tuple->acks[i].batch);

	if (debug_tuple_stream_buffer)
		elog(LOG, "[%d] unpinned event at [%d, %d); readers %d",
				cq_id, BufferOffset(buf, slot), BufferOffset(buf, SlotEnd(slot)), bms_num_members(slot->readers));

	if (!bms_is_empty(slot->readers))
		return;

	try_move_tail(buf, slot);
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
	Latch *latches = dsm_array_get(buf->latches, cq_id);
	memcpy(&latches[reader_id], proclatch, sizeof(Latch));
}

/*
 * TupleBufferWait
 */
void
TupleBufferWait(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id)
{
	bool should_wait = false;

	SpinLockAcquire(&buf->mutex);
	if (!TupleBufferHasUnreadSlots())
	{
		buf->waiters = dsm_bms_add_member(buf->waiters, cq_id);
		should_wait = true;
	}
	SpinLockRelease(&buf->mutex);

	if (should_wait)
	{
		Latch *latches = dsm_array_get(buf->latches, cq_id);
		WaitLatch((&latches[reader_id]), WL_LATCH_SET, 0);
	}
}

/*
 * TupleBufferResetNotify
 */
void
TupleBufferResetNotify(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id)
{
	Latch *latches = dsm_array_get(buf->latches, cq_id);
	ResetLatch((&latches[reader_id]));
}

/*
 * TupleBufferNotify
 */
void
TupleBufferNotify(TupleBuffer *buf, uint32_t cq_id)
{
	int i;
	Latch *latches = dsm_array_get(buf->latches, cq_id);

	for (i = 0; i < buf->max_readers; i++)
	{
		Latch *l = &latches[i];
		if (!l || !l->owner_pid)
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

	TupleBufferClearPinnedSlots();
}

/*
 * TupleBufferClearPinnedSlots
 */
void
TupleBufferClearPinnedSlots(void)
{
	MemoryContext oldcontext = MemoryContextSwitchTo(CQExecutionContext);
	ListCell *lc;

	list_free_deep(MyPinnedSlots);
	MyPinnedSlots = NIL;

	foreach(lc, MyAcks)
		StreamBatchMarkAcked(lfirst(lc));

	list_free_deep(MyAcks);
	MyAcks = NIL;

	MemoryContextSwitchTo(oldcontext);
}

/*
 * TupleBufferHasUnreadSlots
 */
bool
TupleBufferHasUnreadSlots(void)
{
	ListCell *lc;

	foreach(lc, MyReaders)
	{
		TupleBufferReader *reader = (TupleBufferReader *) lfirst(lc);

		if (!NoUnreadSlots(reader) && !TupleBufferIsEmpty(reader->buf))
			return true;
	}

	return false;
}

/*
 * TupleBufferClearReaders
 */
void
TupleBufferClearReaders(void)
{
	list_free_deep(MyReaders);
	MyReaders = NIL;
}

/*
 * TupleBufferDrain
 */
void
TupleBufferDrain(TupleBuffer *buf, uint32_t cq_id)
{
	TupleBufferReader *reader = TupleBufferOpenReader(buf, cq_id, 0, 1);
	TupleBufferSlot *tbs;
	while ((tbs = TupleBufferPinNextSlot(reader)))
		TupleBufferUnpinSlot(reader, tbs);
	TupleBufferCloseReader(reader);

	TupleBufferClearPinnedSlots();
}
