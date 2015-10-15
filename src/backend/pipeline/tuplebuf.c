/*-------------------------------------------------------------------------
 *
 * tuplebuf.c
 *
 *	  Interface for interacting with the tuple buffers
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/tuplebuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream_fn.h"
#include "libpq/pqformat.h"
#include "pgstat.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "pipeline/tuplebuf.h"
#include "postmaster/bgworker.h"
#include "executor/tuptable.h"
#include "nodes/bitmapset.h"
#include "nodes/print.h"
#include "storage/shm_alloc.h"
#include "storage/lwlock.h"
#include "storage/spin.h"
#include "storage/proc.h"
#include "utils/memutils.h"
#include "utils/typcache.h"
#include "miscadmin.h"

#define MAGIC 0xDEADBABE /* x_x */
#define MURMUR_SEED 0x9eaca8149c92387e
#define MAX_WAIT_SLEEP_MS 10
#define WAIT_SLEEP_MS 1

#define BufferOffset(buf, ptr) ((int32) ((char *) (ptr) - (buf)->start))
#define BufferEnd(buf) ((buf)->start + (buf)->size)
#define SlotEnd(slot) ((char *) (slot) + (slot)->size)
#define SlotNext(slot) ((TupleBufferSlot *) SlotEnd(slot))
#define NoUnreadSlots(reader) ((reader)->slot_id == (reader)->buf->head_id)
#define SlotIsValid(slot) ((slot) && (slot)->magic == MAGIC)
#define SlotEqualsTail(buf, slot) ((slot) == (buf)->tail && (slot)->id == (buf)->tail_id)
#define HasEnoughSize(start, end, size) ((intptr_t) size <= ((intptr_t) end - (intptr_t) start))
#define IsTailAhead(buf) ((uintptr_t) (buf)->tail > (uintptr_t) (buf)->head)

TupleBuffer *WorkerTupleBuffer = NULL;
TupleBuffer *CombinerTupleBuffer = NULL;
TupleBuffer *AdhocTupleBuffer = NULL;

/* GUC parameters */
int tuple_buffer_blocks;

/*
 * MakeStreamTuple
 */
StreamTuple *
MakeStreamTuple(HeapTuple heaptup, TupleDesc desc, int num_acks, InsertBatchAck *acks)
{
	StreamTuple *t = palloc0(sizeof(StreamTuple));
	int i;

	t->db_oid = MyDatabaseId;
	t->heaptup = heaptup;
	t->desc = PackTupleDesc(desc);
	t->arrival_time = GetCurrentTimestamp();
	t->num_acks = num_acks;
	t->acks = acks;

	if (desc)
	{
		Assert(!IsContQueryProcess());

		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute attr = desc->attrs[i];
			RecordTupleDesc *rdesc;
			Datum v;
			bool isnull;
			HeapTupleHeader rec;
			int32 tupTypmod;
			TupleDesc attdesc;

			if (attr->atttypid != RECORDOID)
				continue;

			v = heap_getattr(heaptup, i + 1, desc, &isnull);

			if (isnull)
				continue;

			if (t->record_descs == NULL)
				t->record_descs = palloc0(sizeof(RecordTupleDesc) * desc->natts);

			rec = DatumGetHeapTupleHeader(v);
			Assert(HeapTupleHeaderGetTypeId(rec) == RECORDOID);
			tupTypmod = HeapTupleHeaderGetTypMod(rec);

			rdesc = &t->record_descs[t->num_record_descs++];
			rdesc->typmod = tupTypmod;
			attdesc = lookup_rowtype_tupdesc(RECORDOID, tupTypmod);
			rdesc->desc = PackTupleDesc(attdesc);
			ReleaseTupleDesc(attdesc);
		}
	}

	return t;
}

static void try_moving_tail(TupleBuffer *buf, TupleBufferSlot *tail)
{
	if (!SlotIsValid(tail) || tail->unread || !SlotEqualsTail(buf, tail))
		return;

	LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

	/*
	 * If this slot was the tail, move tail ahead to the next slot that is not read.
	 */
	if (SlotIsValid(tail) && SlotEqualsTail(buf, tail))
	{
		while (!buf->tail->unread)
		{
			buf->tail->magic = 0;
			buf->tail->unread = false;
			buf->tail = buf->tail->next;

			if (TupleBufferIsEmpty(buf))
				break;

			buf->tail_id = buf->tail->id;
		}

		if (buf->writer_latch)
			SetLatch(buf->writer_latch);
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
	TupleBufferSlot *tail;
	uint64_t id = slot->id;

	while (true)
	{
		if (!SlotIsValid(slot) || slot->id != id)
			break;

		tail = buf->tail;

		pg_usleep(WAIT_SLEEP_MS * 1000);

		/* If tail was unchanged, try moving it. */
		if (tail == buf->tail)
			try_moving_tail(buf, buf->tail);
	}
}

/*
 * TupleBufferInsert
 *
 * Appends a decoded event to the given stream buffer
 */
TupleBufferSlot *
TupleBufferInsert(TupleBuffer *buf, StreamTuple *tuple, Bitmapset *queries)
{
	char *start;
	char *pos;
	char *end;
	TupleBufferSlot *slot;
	Size size;
	Size tupsize;
	Size acks_size;
	int desclen = tuple->desc ? VARSIZE(tuple->desc) : 0;
	int i;

	/* no one is going to read this tuple, so it's a noop */
	if (bms_is_empty(queries))
		return NULL;

	tupsize = tuple->heaptup->t_len + HEAPTUPLESIZE;
	acks_size = sizeof(InsertBatchAck) * tuple->num_acks;
	size = sizeof(TupleBufferSlot) + sizeof(StreamTuple) + tupsize + BITMAPSET_SIZE(queries->nwords) + desclen + acks_size;

	for (i = 0; i < tuple->num_record_descs; i++)
		size += sizeof(RecordTupleDesc) + VARSIZE(tuple->record_descs[i].desc);

	if (size > buf->size)
		elog(ERROR, "event of size %zu too big for stream buffer of size %zu", size, WorkerTupleBuffer->size);

	LWLockAcquire(buf->head_lock, LW_EXCLUSIVE);
	LWLockAcquire(buf->tail_lock, LW_SHARED);

	buf->writer_latch = &MyProc->procLatch;

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
		TimestampTz start_wait;

		start = SlotEnd(buf->head);

		/* If the tail is infront of us, then don't go beyond it. */
		if (IsTailAhead(buf))
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

			try_moving_tail(buf, buf->tail);

			if (TimestampDifferenceExceeds(start_wait, GetCurrentTimestamp(), MAX_WAIT_SLEEP_MS))
				pg_usleep(WAIT_SLEEP_MS * 1000);
			else
				WaitLatch(buf->writer_latch, WL_LATCH_SET, 0);

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
	slot = (TupleBufferSlot *) pos;

	Assert(slot->magic != MAGIC);

	/* Initialize the newly allocated slot and copy data into it. */
	MemSet(pos, 0, size);
	slot->id = buf->head_id + 1;
	slot->db_oid = MyDatabaseId;
	slot->unread = true;
	slot->next = NULL;
	slot->buf = buf;
	slot->size = size;

	pos += sizeof(TupleBufferSlot);
	slot->tuple = (StreamTuple *) pos;
	memcpy(slot->tuple, tuple, sizeof(StreamTuple));
	pos += sizeof(StreamTuple);

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

	if (synchronous_stream_insert)
	{
		slot->tuple->acks = (InsertBatchAck *) pos;
		memcpy(slot->tuple->acks, tuple->acks, acks_size);
		pos += acks_size;
	}

	if (tuple->num_record_descs)
	{
		Size len;

		Assert(!IsContQueryProcess());

		len = sizeof(RecordTupleDesc) * tuple->num_record_descs;
		slot->tuple->record_descs = (RecordTupleDesc *) pos;
		memcpy(slot->tuple->record_descs, tuple->record_descs, len);
		pos += len;

		for (i = 0; i < tuple->num_record_descs; i++)
		{
			RecordTupleDesc *rdesc = &slot->tuple->record_descs[i];
			len = VARSIZE(tuple->record_descs[i].desc);
			rdesc->desc = (bytea *) pos;
			memcpy(pos, tuple->record_descs[i].desc, len);
			pos += len;
		}
	}

	slot->tuple->group_hash = tuple->group_hash;

	slot->queries = (Bitmapset *) pos;
	memcpy(slot->queries, queries, BITMAPSET_SIZE(queries->nwords));

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
		Bitmapset *notify;
		int i;

		notify = bms_copy(buf->waiters);

		while ((i = bms_first_member(notify)) >= 0)
		{
			TupleBufferReader *reader = buf->readers[i];

			/* can never wake up waiters that are not connected to the same database */
			if (reader->proc->group->db_oid != slot->db_oid)
				continue;

			/* wake up reader only if it will read this slot */
			if (reader->should_read_fn(reader, slot))
			{
				buf->waiters = bms_del_member(buf->waiters, i);
				buf->readers[i] = NULL;
				SetLatch(reader->proc->latch);
			}
		}

		bms_free(notify);
	}
	SpinLockRelease(&buf->mutex);

	/* Mark this latch as invalid. */
	buf->writer_latch = NULL;

	LWLockRelease(buf->tail_lock);
	LWLockRelease(buf->head_lock);

	return slot;
}

static Size tuple_buffer_header_size()
{
	return MAXALIGN(add_size(sizeof(TupleBuffer), mul_size(sizeof(TupleBufferReader *), max_worker_processes)));
}


static Size tuple_buffer_size()
{
	return MAXALIGN(mul_size(tuple_buffer_blocks, BLCKSZ));
}

/*
 * TupleBuffersShmemSize
 *
 * Retrieves the size in bytes of the stream buffer
 */
Size
TupleBuffersShmemSize(void)
{
	return MAXALIGN(mul_size(add_size(tuple_buffer_size(), tuple_buffer_header_size()), 3));
}

/*
 * TupleBuffersShmemInit
 */
void
TupleBuffersShmemInit(void)
{
	WorkerTupleBuffer = TupleBufferInit("WorkerTupleBuffer", tuple_buffer_size(), WorkerBufferHeadLock, WorkerBufferTailLock);
	CombinerTupleBuffer = TupleBufferInit("CombinerTupleBuffer", tuple_buffer_size(), CombinerBufferHeadLock, CombinerBufferTailLock);
	AdhocTupleBuffer = TupleBufferInit("AdhocTupleBuffer", tuple_buffer_size(), AdhocBufferHeadLock, AdhocBufferTailLock);
}

/*
 * TupleBufferInit
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
TupleBuffer *
TupleBufferInit(char *name, Size size, LWLock *head_lock, LWLock *tail_lock)
{
	bool found;
	Size header_size = tuple_buffer_header_size();
	TupleBuffer *buf;

	buf = ShmemInitStruct(name, header_size , &found);

	if (!found)
	{
		int bms_words = max_worker_processes / BITS_PER_BITMAPWORD + 1; /* extra word, in case of rounding issues */

		MemSet(buf, 0, header_size);

		buf->start = (char *) ShmemAlloc(size);
		buf->size = size;
		MemSet(buf->start, 0, size);

		buf->head_lock = head_lock;
		buf->tail_lock = tail_lock;

		SpinLockInit(&buf->mutex);

		buf->waiters = ShmemAlloc(BITMAPSET_SIZE(bms_words));
		buf->waiters->nwords = bms_words;
	}

	return buf;
}

/*
 * TupleBufferOpenReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
TupleBufferReader *
TupleBufferOpenReader(TupleBuffer *buf, TupleBufferShouldReadFunc should_read_fn)
{
	TupleBufferReader *reader = (TupleBufferReader *) ShmemDynAlloc(sizeof(TupleBufferReader));

	if (MyContQueryProc == NULL)
		ereport(ERROR, (errmsg("tuple buffer readers can only be opened by continuous query processes")));

	reader->buf = buf;
	reader->proc = MyContQueryProc;
	reader->slot = NULL;
	reader->slot_id = 0;
	reader->should_read_fn = should_read_fn;
	reader->pinned = NIL;

	return reader;
}

/*
 * TupleBufferCloseReader
 */
void
TupleBufferCloseReader(TupleBufferReader *reader)
{
	ShmemDynFree(reader);
}

/*
 * TupleBufferPinNextSlot
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 */
TupleBufferSlot *
TupleBufferPinNextSlot(TupleBufferReader *reader)
{
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
		if (reader->proc->group->db_oid == reader->slot->db_oid && reader->should_read_fn(reader, reader->slot))
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

	Assert(SlotIsValid(reader->slot));

	oldcontext = MemoryContextSwitchTo(ContQueryBatchContext);

	reader->pinned = lappend(reader->pinned, reader->slot);

	MemoryContextSwitchTo(oldcontext);

	return reader->slot;
}

/*
 * TupleBufferUnpinSlot
 *
 * Marks the given slot as read by the given reader. Once all open readers
 * have unpinned a slot, it is freed.
 */
void
TupleBufferUnpinSlot(TupleBufferSlot *slot)
{
	TupleBuffer *buf = slot->buf;

	if (!SlotIsValid(slot))
		return;

	if (!slot->unread)
	{
		try_moving_tail(buf, slot);
		return;
	}

	slot->unread = false;
	try_moving_tail(buf, slot);
}

/*
 * TupleBufferIsEmpty
 */
bool
TupleBufferIsEmpty(TupleBuffer *buf)
{
	return (buf->tail == NULL);
}

/*
 * TupleBufferTryWait
 */
void
TupleBufferTryWait(TupleBufferReader *reader)
{
	TupleBuffer *buf = reader->buf;
	bool should_wait = false;

	SpinLockAcquire(&buf->mutex);
	if (!TupleBufferHasUnreadSlots(reader))
	{
		buf->waiters = bms_add_member(buf->waiters, reader->proc->id);
		buf->readers[reader->proc->id] = reader;
		should_wait = true;
	}
	SpinLockRelease(&buf->mutex);

	if (should_wait)
		WaitLatch(reader->proc->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
}

/*
 * TupleBufferTryWaitTimeout
 */
void
TupleBufferTryWaitTimeout(TupleBufferReader *reader, int timeout)
{
	TupleBuffer *buf = reader->buf;
	bool should_wait = false;

	SpinLockAcquire(&buf->mutex);
	if (!TupleBufferHasUnreadSlots(reader))
	{
		buf->waiters = bms_add_member(buf->waiters, reader->proc->id);
		buf->readers[reader->proc->id] = reader;
		should_wait = true;
	}
	SpinLockRelease(&buf->mutex);

	if (should_wait)
		WaitLatch(reader->proc->latch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT, timeout);
}

/*
 * TupleBufferUnpinAllPinnedSlots
 */
void
TupleBufferUnpinAllPinnedSlots(TupleBufferReader *reader)
{
	ListCell *lc;

	if (!reader->pinned)
		return;

	foreach(lc, reader->pinned)
		TupleBufferUnpinSlot(lfirst(lc));

	list_free(reader->pinned);
	reader->pinned = NIL;
}

/*
 * TupleBufferHasUnreadSlots
 */
bool
TupleBufferHasUnreadSlots(TupleBufferReader *reader)
{
	if (!NoUnreadSlots(reader) && !TupleBufferIsEmpty(reader->buf))
		return true;

	return false;
}

/*
 * TupleBufferDrain
 */
void
TupleBufferDrain(TupleBuffer *buf, Oid db_oid)
{
	TupleBufferSlot *slot;

	LWLockAcquire(buf->head_lock, LW_EXCLUSIVE);
	LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

	slot = buf->tail;

	while (slot)
	{
		if (slot->db_oid == db_oid)
			slot->unread = false;
		slot = slot->next;
	}

	slot = buf->tail;

	LWLockRelease(buf->tail_lock);

	try_moving_tail(buf, slot);

	LWLockRelease(buf->head_lock);
}

void
TupleBufferDrainGeneric(TupleBuffer *buf, TupleBufferShouldDrainFunc fn, void *ctx)
{
	TupleBufferSlot *slot;
	LWLockAcquire(buf->tail_lock, LW_EXCLUSIVE);

	slot = buf->tail;

	while (slot)
	{
		if (fn(slot, ctx))
			slot->unread = false;

		slot = slot->next;
	}

	slot = buf->tail;

	LWLockRelease(buf->tail_lock);

	try_moving_tail(buf, slot);
}

/*
 * TupleBufferOpenBatchReader
 */
TupleBufferBatchReader *
TupleBufferOpenBatchReader(TupleBuffer *buf, TupleBufferShouldReadFunc read_func)
{
	TupleBufferBatchReader *reader = palloc0(sizeof(TupleBufferBatchReader));

	reader->rdr = TupleBufferOpenReader(buf, read_func);
	reader->cq_id = InvalidOid;
	reader->started = false;
	reader->depleted = false;
	reader->batch_done = false;
	reader->current = NULL;
	reader->params = GetContQueryRunParams();
	reader->queries_seen = NULL;
	reader->yielded = NIL;

	return reader;
}

/*
 * TupleBufferBatchReaderSetCQId
 */
void
TupleBufferBatchReaderSetCQId(TupleBufferBatchReader *reader, Oid cq_id)
{
	if (reader->cq_id != InvalidOid)
		ereport(ERROR, (errmsg("rewind the reader before changing the continuous query id")));

	reader->cq_id = cq_id;
}

/*
 * TupleBufferBatchReaderHasTuplesForCQId
 */
bool
TupleBufferBatchReaderHasTuplesForCQId(TupleBufferBatchReader *reader, Oid cq_id)
{
	/*
	 * If the batch is not done, we can't be sure if we'll see tuples for this cq id so just fake
	 * a yes.
	 */
	if (!reader->batch_done)
		return true;

	return bms_is_member(cq_id, reader->queries_seen);
}

/*
 * TupleBufferBatchReaderNext
 */
TupleBufferSlot *
TupleBufferBatchReaderNext(TupleBufferBatchReader *reader)
{
	TupleBufferSlot *slot;
	MemoryContext old_cxt;

	if (reader->cq_id == InvalidOid)
		ereport(ERROR, (errmsg("continuous query id must be set before trying to read tuples")));

	/*
	 * If we've read a full batch, mark the reader as timed out so that we don't read more tuples from the
	 * underlying reader.
	 */
	if (!reader->batch_done && list_length(reader->rdr->pinned) >= reader->params->batch_size)
	{
		reader->batch_done = true;
		reader->depleted = true;
		return NULL;
	}

	/*
	 * We've yielded all tuple belonging to the request CQ in this batch?
	 */
	if (reader->depleted)
		return NULL;

	/*
	 * If we've read a full batch from the underlying reader, just scan the *pinned* tuples
	 * in the underlying reader for matching ones.
	 */
	if (reader->batch_done)
	{
		while (true)
		{
			if (reader->current == NULL)
				reader->current = list_head(reader->rdr->pinned);
			else
				reader->current = lnext(reader->current);

			if (reader->current == NULL)
			{
				reader->depleted = true;
				return NULL;
			}

			slot = lfirst(reader->current);
			if (bms_is_member(reader->cq_id, slot->queries))
				goto yield;
		}

	}

	/*
	 * If we haven't started reading a new batch, mark ourselves as started.
	 */
	if (!reader->started)
	{
		reader->started = true;
		reader->start_time = GetCurrentTimestamp();
	}

	while (true)
	{
		/*
		 * We've waited long enough to read a full batch? Don't try to read more tuples from the underlying reader. We also
		 * break early here in case continuous queries were deactivated.
		 */
		if (TimestampDifferenceExceeds(reader->start_time, GetCurrentTimestamp(), reader->params->max_wait) ||
				!reader->rdr->proc->group->active || reader->rdr->proc->group->terminate)
		{
			reader->batch_done = true;
			reader->depleted = true;
			return NULL;
		}

		slot = TupleBufferPinNextSlot(reader->rdr);

		if (slot == NULL)
		{
			pg_usleep(WAIT_SLEEP_MS * 1000);
			continue;
		}

		/* keep track of all the queries seen, so we can quickly skip execution for ones which we have no tuples for */
		old_cxt = MemoryContextSwitchTo(ContQueryBatchContext);
		reader->queries_seen = bms_add_members(reader->queries_seen, slot->queries);
		MemoryContextSwitchTo(old_cxt);

		if (bms_is_member(reader->cq_id, slot->queries))
			goto yield;
	}

yield:
	Assert(slot);
	Assert(slot->magic == MAGIC);

	old_cxt = MemoryContextSwitchTo(ContQueryBatchContext);
	reader->yielded = lappend(reader->yielded, slot);
	MemoryContextSwitchTo(old_cxt);

	return slot;
}

/*
 * TupleBufferBatchReaderRewind
 */
void
TupleBufferBatchReaderRewind(TupleBufferBatchReader *reader)
{
	if (reader->cq_id == InvalidOid)
		return;

	reader->cq_id = InvalidOid;

	if (!reader->started)
		return;

	reader->batch_done = true;
	reader->depleted = false;
	reader->current = NULL;

	list_free(reader->yielded);
	reader->yielded = NIL;
}

/*
 * TupleBufferBatchReaderReset
 */
void
TupleBufferBatchReaderReset(TupleBufferBatchReader *reader)
{
	/* free everything that keeps track of a single batch */
	MemoryContext old_ctx = MemoryContextSwitchTo(ContQueryBatchContext);

	if (synchronous_stream_insert)
	{
		ListCell *lc;

		foreach(lc, reader->rdr->pinned)
		{
			TupleBufferSlot *slot = lfirst(lc);
			int i;

			for (i = 0; i < slot->tuple->num_acks; i++)
				InsertBatchMarkAcked(&slot->tuple->acks[i]);
		}
	}

	TupleBufferUnpinAllPinnedSlots(reader->rdr);
	TupleBufferBatchReaderRewind(reader);

	bms_free(reader->queries_seen);
	reader->queries_seen = NULL;

	MemoryContextSwitchTo(old_ctx);

	/* we need to restart scanning from the underlying reader */
	reader->started = false;
	reader->batch_done = false;
}

/*
 * TupleBufferCloseBatchReader
 */
void
TupleBufferCloseBatchReader(TupleBufferBatchReader *reader)
{
	TupleBuffer *buf = reader->rdr->buf;

	SpinLockAcquire(&buf->mutex);
	buf->waiters = bms_del_member(buf->waiters, reader->rdr->proc->id);

	SpinLockRelease(&buf->mutex);

	TupleBufferCloseReader(reader->rdr);
	pfree(reader);
}

/*
 * TupleBufferBatchReaderTrySleep
 */
void
TupleBufferBatchReaderTrySleep(TupleBufferBatchReader *reader, TimestampTz last_processed)
{
	if (!TupleBufferHasUnreadSlots(reader->rdr) &&
			TimestampDifferenceExceeds(last_processed, GetCurrentTimestamp(), reader->params->max_wait) &&
			!(!reader->rdr->proc->group->active || reader->rdr->proc->group->terminate))
	{
		cq_stat_report(true);

		pgstat_report_activity(STATE_IDLE, GetContQueryProcName(reader->rdr->proc));
		TupleBufferTryWait(reader->rdr);
		pgstat_report_activity(STATE_RUNNING, GetContQueryProcName(reader->rdr->proc));

		ResetLatch(reader->rdr->proc->latch);
	}
}

void
TupleBufferBatchReaderTrySleepTimeout(TupleBufferBatchReader *reader,
									  TimestampTz last_processed,
									  int timeout)
{
	if (!TupleBufferHasUnreadSlots(reader->rdr) &&
			TimestampDifferenceExceeds(last_processed, GetCurrentTimestamp(), reader->params->max_wait) &&
			!(!reader->rdr->proc->group->active || reader->rdr->proc->group->terminate))
	{
		cq_stat_report(true);

		pgstat_report_activity(STATE_IDLE, GetContQueryProcName(reader->rdr->proc));
		TupleBufferTryWaitTimeout(reader->rdr, timeout);
		pgstat_report_activity(STATE_RUNNING, GetContQueryProcName(reader->rdr->proc));

		ResetLatch(reader->rdr->proc->latch);
	}
}
