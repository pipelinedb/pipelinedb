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
#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/broker.h"
#include "pipeline/ipc/queue.h"
#include "port/atomics.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "utils/memutils.h"

#define MAGIC 0xDEADBABE /* x_x */
#define WAIT_SLEEP_NS 250
#define MAX_WAIT_SLEEP_NS 5000 /* 5ms */

void
ipc_queue_init(void *ptr, Size size, LWLock *lock)
{
	ipc_queue *ipcq;

	ipcq = (ipc_queue *) ptr;
	if (ipcq->magic == MAGIC)
		elog(ERROR, "ipc_queue already initialized");

	MemSet((char *) ipcq, 0, size);

	/* We leave enough space for a ipc_queue_slot to go at the end, so we never overflow the buffer. */
	ipcq->size = size - sizeof(ipc_queue) - sizeof(ipc_queue_slot);

	/* Initialize atomic types. */
	pg_atomic_init_u64(&ipcq->head, 0);
	pg_atomic_init_u64(&ipcq->tail, 0);
	ipcq->cursor = 0;

	pg_atomic_init_u64(&ipcq->producer_latch, 0);
	pg_atomic_init_u64(&ipcq->consumer_latch, 0);

	ipcq->lock = lock;

	/* This marks the ipc_queue as initialized. */
	ipcq->magic = MAGIC;
}

bool
ipc_queue_push_nolock(ipc_queue *ipcq, void *ptr, int len, bool wait)
{
	uint64 head;
	uint64 tail;
	Latch *producer_latch = NULL;
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

	for (;;)
	{
		int r;

		tail = pg_atomic_read_u64(&ipcq->tail);

		Assert(tail <= head);

		/* Is there enough space in the buffer? */
		if (ipc_queue_free_size(ipcq, head, tail) >= len_needed)
			break;
		else if (!wait)
			return false;

		Assert(producer_latch);

		r = WaitLatch(producer_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
		if (r & WL_POSTMASTER_DEATH)
			return false;

		ResetLatch(producer_latch);
		CHECK_FOR_INTERRUPTS();
	}

	pg_atomic_write_u64(&ipcq->producer_latch, (uint64) NULL);

	slot = ipc_queue_slot_get(ipcq, head);
	slot->time = GetCurrentTimestamp();
	slot->len = len;
	slot->wraps = needs_wrap;
	slot->peeked = false;

	/*
	 * If we're wrapping around, copy into the start of buffer, otherwise copy
	 * ahead of this slot.
	 */
	pos = needs_wrap ? ipcq->bytes : slot->bytes;
	ipc_queue_check_overflow(ipcq, pos, len);

	/* Copy over data. */
	if (ipcq->copy_fn)
		ipcq->copy_fn(pos, ptr, len);
	else
		memcpy(pos, ptr, len);

	head += len_needed;
	slot->next = head;

	ipc_queue_update_head(ipcq, head);

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

	slot = ipc_queue_slot_get(ipcq, ipcq->cursor);

	if (slot->wraps)
		pos = ipcq->bytes;
	else
		pos = slot->bytes;

	ipcq->cursor = slot->next;
	*len = slot->len;

	if (ipcq->peek_fn && !slot->peeked)
	{
		ipcq->peek_fn(pos, slot->len);
		slot->peeked = true;
	}

	return pos;
}

void
ipc_queue_unpeek_all(ipc_queue *ipcq)
{
	Assert(ipcq->magic == MAGIC);
	ipcq->cursor = pg_atomic_read_u64(&ipcq->tail);
}

void
ipc_queue_update_tail(ipc_queue *ipcq, uint64 tail)
{
	/*
	 * We must update tail before setting the latch, because the producer will set its latch
	 * before getting the new value for tail. This guarantees that we don't
	 * miss a wake up.
	 */
	pg_atomic_write_u64(&ipcq->tail, tail);

	if (ipcq->produced_by_broker)
		signal_ipc_broker_process();
	else
	{
		Latch *latch = (Latch *) pg_atomic_read_u64(&ipcq->producer_latch);
		if (latch != NULL)
			SetLatch(latch);
	}
}

static inline void
ipc_queue_slot_pop(ipc_queue *ipcq, ipc_queue_slot *slot)
{
	char *pos;

	Assert(ipcq->pop_fn);

	if (slot->wraps)
		pos = ipcq->bytes;
	else
		pos = slot->bytes;

	ipcq->pop_fn(pos, slot->len);
}

void
ipc_queue_pop_peeked(ipc_queue *ipcq)
{
	uint64 cur = ipcq->cursor;

	Assert(ipcq->magic == MAGIC);

	if (ipcq->pop_fn)
	{
		uint64 start = pg_atomic_read_u64(&ipcq->tail);

		Assert(start <= cur);

		while (start < cur)
		{
			ipc_queue_slot *slot = ipc_queue_slot_get(ipcq, start);

			ipc_queue_slot_pop(ipcq, slot);
			start = slot->next;
		}
	}

	ipc_queue_update_tail(ipcq, cur);
}

void
ipc_queue_pop_inserted_before(ipc_queue *ipcq, TimestampTz time)
{
	uint64 head;
	uint64 start;

	Assert(ipcq->magic == MAGIC);

	head = pg_atomic_read_u64(&ipcq->head);
	start = pg_atomic_read_u64(&ipcq->tail);

	while (start < head)
	{
		ipc_queue_slot *slot = ipc_queue_slot_get(ipcq, start);

		if (slot->time >= time)
			break;

		if (ipcq->pop_fn)
			ipc_queue_slot_pop(ipcq, slot);

		start = slot->next;
	}

	ipcq->cursor = start;
	ipc_queue_update_tail(ipcq, start);
}

void
ipc_queue_wait_non_empty(ipc_queue *ipcq, int timeoutms)
{
	Latch *consumer_latch;
	uint64 head;
	uint64 tail;
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

	for (;;)
	{
		int r;

		head = pg_atomic_read_u64(&ipcq->head);

		if (head > tail)
			break;

		r = WaitLatch(consumer_latch, flags, timeoutms);
		if (r & WL_POSTMASTER_DEATH)
			break;

		if (ShouldTerminateContQueryProcess())
			break;

		ResetLatch(consumer_latch);
		CHECK_FOR_INTERRUPTS();
	}

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

void
ipc_queue_update_head(ipc_queue *ipcq, uint64 head)
{
	pg_atomic_write_u64(&ipcq->head, head);

	if (ipcq->consumed_by_broker)
		signal_ipc_broker_process();
	else
	{
		Latch *latch = (Latch *) pg_atomic_read_u64(&ipcq->consumer_latch);
		if (latch)
			SetLatch(latch);
	}
}

void
ipc_multi_queue_wait_non_empty(ipc_multi_queue *ipcmq, int timeoutms)
{
	Latch *consumer_latch;
	uint64 tails[ipcmq->nqueues];
	int flags;
	int i;

	for (i = 0; i < ipcmq->nqueues; i++)
	{
		ipc_queue *ipcq = ipcmq->queues[i];
		uint64 head;

		Assert(ipcq->magic == MAGIC);

		head = pg_atomic_read_u64(&ipcq->head);
		tails[i] = pg_atomic_read_u64(&ipcq->tail);

		if (tails[i] < head)
			return;
	}

	consumer_latch = MyLatch;

	for (i = 0; i < ipcmq->nqueues; i++)
	{
		ipc_queue *ipcq = ipcmq->queues[i];
		pg_atomic_write_u64(&ipcq->consumer_latch, (uint64) consumer_latch);
	}

	flags = WL_LATCH_SET | WL_POSTMASTER_DEATH;
	if (timeoutms > 0)
		flags |= WL_TIMEOUT;

	for (;;)
	{
		int r;
		bool non_empty = false;

		for (i = 0; i < ipcmq->nqueues; i++)
		{
			ipc_queue *ipcq = ipcmq->queues[i];
			uint64 head = pg_atomic_read_u64(&ipcq->head);

			if (head > tails[i])
			{
				non_empty = true;
				break;
			}
		}

		if (non_empty)
			break;

		r = WaitLatch(consumer_latch, flags, timeoutms);
		if (r & WL_POSTMASTER_DEATH)
			break;

		if (ShouldTerminateContQueryProcess())
			break;

		ResetLatch(consumer_latch);
		CHECK_FOR_INTERRUPTS();
	}

	for (i = 0; i < ipcmq->nqueues; i++)
	{
		ipc_queue *ipcq = ipcmq->queues[i];
		pg_atomic_write_u64(&ipcq->consumer_latch, (uint64) NULL);
	}
}

void *
ipc_multi_queue_peek_next(ipc_multi_queue *ipcmq, int *len)
{
	static int idx = 0;
	int i;
	void *ptr;

	ptr = ipc_queue_peek_next(ipcmq->queues[idx], len);

	if (!ptr)
	{
		for (i = 0; i < ipcmq->nqueues; i++)
		{
			idx = (idx + 1) % ipcmq->nqueues;
			ptr = ipc_queue_peek_next(ipcmq->queues[idx], len);

			if (ptr)
				break;
		}
	}
	else if (idx != ipcmq->pqueue)
		idx = (idx + 1) % ipcmq->nqueues;

	return ptr;
}

void
ipc_multi_queue_set_priority_queue(ipc_multi_queue *ipcmq, int pq)
{
	if (pq >= ipcmq->nqueues || pq < -1)
		elog(ERROR, "queue number must be in [0, nqueues) or -1 for no priority");

	ipcmq->pqueue = pq;
}

void
ipc_multi_queue_unpeek_all(ipc_multi_queue *ipcmq)
{
	int i;

	for (i = 0; i < ipcmq->nqueues; i++)
		ipc_queue_unpeek_all(ipcmq->queues[i]);
}

void
ipc_multi_queue_pop_peeked(ipc_multi_queue *ipcmq)
{
	int i;

	for (i = 0; i < ipcmq->nqueues; i++)
		ipc_queue_pop_peeked(ipcmq->queues[i]);
}

bool
ipc_multi_queue_is_empty(ipc_multi_queue *ipcmq)
{
	bool is_empty = true;
	int i;

	for (i = 0; i < ipcmq->nqueues && is_empty; i++)
		is_empty &= ipc_queue_is_empty(ipcmq->queues[i]);

	return is_empty;
}

bool
ipc_multi_queue_has_unread(ipc_multi_queue *ipcmq)
{
	bool has_unread = false;
	int i;

	for (i = 0; i < ipcmq->nqueues && !has_unread; i++)
		has_unread |= ipc_queue_has_unread(ipcmq->queues[i]);

	return has_unread;
}

ipc_multi_queue *
ipc_multi_queue_init(ipc_queue *q1, ipc_queue *q2)
{
	MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);
	ipc_multi_queue *ipcmq = palloc0(sizeof(ipc_multi_queue));

	ipcmq->queues = palloc0(sizeof(ipc_queue *) * 2);
	ipcmq->queues[0] = q1;
	ipcmq->queues[1] = q2;
	ipcmq->nqueues = 2;
	ipcmq->pqueue = -1;

	MemoryContextSwitchTo(old);

	return ipcmq;
}

void
ipc_multi_queue_pop_inserted_before(ipc_multi_queue *ipcmq, TimestampTz time)
{
	int i;

	for (i = 0; i < ipcmq->nqueues; i++)
		ipc_queue_pop_inserted_before(ipcmq->queues[i], time);
}
