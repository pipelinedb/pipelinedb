/*-------------------------------------------------------------------------
 *
 * queue.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pipeline/ipc/broker.h"
#include "pipeline/ipc/queue.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"

#define MAGIC 0xDEADBABE /* x_x */
#define WAIT_SLEEP_NS 250
#define MAX_WAIT_SLEEP_NS 5000 /* 5ms */

struct ipc_queue_slot
{
	uint64_t next;
	bool     wraps;
	bool     peeked;
	int      len;
	char     bytes[1]; /* dynamically allocated */
};

static inline uint64_t
ipc_queue_offset(ipc_queue *ipcq, uint64_t ptr)
{
	Assert(ipcq->size > 0);
	return ptr % ipcq->size;
}

static inline bool
ipc_queue_needs_wrap(ipc_queue *ipcq, uint64_t start, int len)
{
	/* Is there enough space left or do we cross the physical boundary? */
	return (start % ipcq->size) + len > ipcq->size;
}

static inline ipc_queue_slot *
ipc_queue_slot_get(ipc_queue *ipcq, uint64_t ptr)
{
	return (ipc_queue_slot *) ((uintptr_t) ipcq->bytes + ipc_queue_offset(ipcq, ptr));
}

void
ipc_queue_init(void *ptr, Size size, LWLock *lock, bool used_by_router)
{
	ipc_queue *ipcq;

	ipcq = (ipc_queue *) ptr;
	if (ipcq->magic == MAGIC)
		elog(ERROR, "ipc_queue already initialized");

	MemSet((char *) ipcq, 0, size);

	ipcq->used_by_router = used_by_router;

	/* We leave enough space for a ipc_queue_slot to go at the end, so we never overflow the buffer. */
	ipcq->size = size - sizeof(ipc_queue) - sizeof(ipc_queue_slot);

	/* Initialize atomic types. */
	pg_atomic_init_u64(&ipcq->head, 0);
	pg_atomic_init_u64(&ipcq->tail, 0);
	pg_atomic_init_u64(&ipcq->cursor, 0);
	pg_atomic_init_u64(&ipcq->producer_latch, 0);
	pg_atomic_init_u64(&ipcq->consumer_latch, 0);

	ipcq->lock = lock;

	/* This marks the ipc_queue as initialized. */
	ipcq->magic = MAGIC;
}

bool
ipc_queue_is_empty(ipc_queue *ipcq)
{
	return pg_atomic_read_u64(&ipcq->head) == pg_atomic_read_u64(&ipcq->tail);
}

bool
ipc_queue_has_unread(ipc_queue *ipcq)
{
	return pg_atomic_read_u64(&ipcq->head) > pg_atomic_read_u64(&ipcq->cursor);
}

bool
ipc_queue_push_nolock(ipc_queue *ipcq, void *ptr, int len, bool wait)
{
	uint64_t head;
	uint64_t tail;
	Latch *producer_latch;
	Latch *consumer_latch;
	ipc_queue_slot *slot;
	int len_needed;
	char *pos;
	bool needs_wrap = false;

	Assert(ipcq->magic == MAGIC);
	if (ipcq->lock)
		Assert(LWLockHeldByMe(ipcq->lock));

	len_needed = sizeof(ipc_queue_slot) + len;
	if (len_needed > ipcq->size)
		elog(ERROR, "item size %d exceeds ipc_queue size %ld", len, ipcq->size);

	head = pg_atomic_read_u64(&ipcq->head);

	/*
	 *If we need to wrap around, we waste the space at the end of the buffer. This is simpler
	 * than splitting up the data to be written into two separate continuous memory areas.
	 */
	if (ipc_queue_needs_wrap(ipcq, head, len_needed))
	{
		needs_wrap = true;

		/* Account for garbage space at the end, and no longer require space for ipc_queue_slot. */
		len_needed = len + ipcq->size - ipc_queue_offset(ipcq, head);
	}

	/*
	 * Set ourselves as the producer before trying to read the tail. This ensures consistency with the reader,
	 * because the reader will always update the tail before reading the value for producer latch. So in case the
	 * reader reads the value for producer_latch before the writer sets it, the writer is guaranteed to see the
	 * updated value for tail and therefore the effect is the same as being woken up after that change was made.
	 */
	if (wait)
	{
		producer_latch = MyLatch;
		pg_atomic_write_u64(&ipcq->producer_latch, (uint64) producer_latch);
	}

	/* FIXME(usmanm): On postmaster shutdown, this stays looping, we must break out. */
	for (;;)
	{
		int space_used;

		tail = pg_atomic_read_u64(&ipcq->tail);
		space_used = head - tail;

		/* Is there enough space in the buffer? */
		if (ipcq->size - space_used >= len_needed)
			break;
		else if (!wait)
			return false;

		WaitLatch(producer_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
		ResetLatch(producer_latch);

		CHECK_FOR_INTERRUPTS();
	}

	pg_atomic_write_u64(&ipcq->producer_latch, (uint64) NULL);

	slot = ipc_queue_slot_get(ipcq, head);
	slot->len = len;
	slot->wraps = needs_wrap;
	slot->peeked = false;

	/*
	 * If we're wrapping around, copy into the start of buffer, otherwise copy
	 * ahead of this slot.
	 */
	if (needs_wrap)
		pos = ipcq->bytes;
	else
		pos = slot->bytes;

	MemSet(pos, 0, len);

	/* Copy over data. */
	if (ipcq->copy_fn)
		ipcq->copy_fn(pos, ptr, len);
	else
		memcpy(pos, ptr, len);

	head += len_needed;
	slot->next = head;
	pg_atomic_write_u64(&ipcq->head, head);

	if (ipcq->used_by_router)
		signal_ipc_broker_process();
	else
	{
		consumer_latch = (Latch *) pg_atomic_read_u64(&ipcq->consumer_latch);
		if (consumer_latch)
			SetLatch(consumer_latch);
	}

	return true;
}

bool
ipc_queue_push(ipc_queue *ipcq, void *ptr, int len, bool wait)
{
	bool success;

	ipc_queue_lock(ipcq, true);
	success = ipc_queue_push_nolock(ipcq, ptr, len, wait);
	ipc_queue_unlock(ipcq);

	return success;
}

void *
ipc_queue_peek_next(ipc_queue *ipcq, int *len)
{
	ipc_queue_slot *slot;
	char *pos;

	Assert(ipcq->magic == MAGIC);

	/* Are there any unread slots? */
	if (!ipc_queue_has_unread(ipcq))
	{
		*len = 0;
		return NULL;
	}

	slot = ipc_queue_slot_get(ipcq, pg_atomic_read_u64(&ipcq->cursor));

	if (slot->wraps)
		pos = ipcq->bytes;
	else
		pos = slot->bytes;

	pg_atomic_write_u64(&ipcq->cursor, slot->next);

	*len = slot->len;

	if (ipcq->peek_fn && !slot->peeked)
	{
		ipcq->peek_fn(pos, slot->len);
		slot->peeked = true;
	}

	return pos;
}

void
ipc_queue_unpeek(ipc_queue *ipcq)
{
	Assert(ipcq->magic == MAGIC);
	pg_atomic_write_u64(&ipcq->cursor, pg_atomic_read_u64(&ipcq->tail));
}

bool
ipc_queue_has_unpopped(ipc_queue *ipcq)
{
	Assert(ipcq->magic == MAGIC);
	return pg_atomic_read_u64(&ipcq->tail) == pg_atomic_read_u64(&ipcq->cursor);
}

void
ipc_queue_pop_peeked(ipc_queue *ipcq)
{
	Latch *producer_latch;
	uint64_t tail;
	uint64_t cur;

	Assert(ipcq->magic == MAGIC);

	tail = pg_atomic_read_u64(&ipcq->tail);
	cur = pg_atomic_read_u64(&ipcq->cursor);

	Assert(tail <= cur);

	if (ipcq->pop_fn)
	{
		uint64_t start = tail;

		while (start < cur)
		{
			ipc_queue_slot *slot = ipc_queue_slot_get(ipcq, start);
			char *pos;

			if (slot->wraps)
				pos = ipcq->bytes;
			else
				pos = slot->bytes;

			ipcq->pop_fn(pos, slot->len);
			start = slot->next;
		}
	}

	/*
	 * We must update tail before setting the latch, because the producer will set its latch
	 * before getting the new value for tail. This guarantees that we don't
	 * miss a wake up.
	 */
	pg_atomic_write_u64(&ipcq->tail, cur);
	producer_latch = (Latch *) pg_atomic_read_u64(&ipcq->producer_latch);
	if (producer_latch != NULL)
		SetLatch(producer_latch);
}

void
ipc_queue_wait_non_empty(ipc_queue *ipcq, int timeoutms)
{
	Latch *consumer_latch;
	uint64_t head;
	uint64_t tail;
	int flags;

	Assert(ipcq->magic == MAGIC);

	head = pg_atomic_read_u64(&ipcq->head);
	tail = pg_atomic_read_u64(&ipcq->tail);

	Assert(tail <= head);

	if (tail < head)
		return;

	consumer_latch = MyLatch;
	pg_atomic_write_u64(&ipcq->consumer_latch, (uint64) consumer_latch);

	flags = WL_LATCH_SET | WL_POSTMASTER_DEATH;
	if (timeoutms > 0)
		flags |= WL_TIMEOUT;

	head = pg_atomic_read_u64(&ipcq->head);

	if (head > tail)
		return;

	WaitLatch(consumer_latch, flags, timeoutms);
	CHECK_FOR_INTERRUPTS();
	ResetLatch(consumer_latch);

	pg_atomic_write_u64(&ipcq->consumer_latch, (uint64) NULL);
}

void
ipc_queue_set_handlers(ipc_queue *ipcq, ipc_queue_peek_fn peek_fn,
		ipc_queue_pop_fn pop_fn, ipc_queue_copy_fn cpy_fn)
{
	ipcq->peek_fn = peek_fn;
	ipcq->pop_fn = pop_fn;
	ipcq->copy_fn = cpy_fn;
}

bool
ipc_queue_lock(ipc_queue *mpq, bool wait)
{
	Assert(mpq->magic == MAGIC);
	Assert(mpq->lock);

	if (wait)
	{
		LWLockAcquire(mpq->lock, LW_EXCLUSIVE);
		return true;
	}

	return LWLockConditionalAcquire(mpq->lock, LW_EXCLUSIVE);
}

void
ipc_queue_unlock(ipc_queue *mpq)
{
	Assert(mpq->magic == MAGIC);
	Assert(mpq->lock);
	Assert(LWLockHeldByMe(mpq->lock));
	LWLockRelease(mpq->lock);
}
