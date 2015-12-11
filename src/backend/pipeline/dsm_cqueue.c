/*-------------------------------------------------------------------------
 *
 * dsm_cqueue.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/backend/pipeline/dsm_cqueue.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "pipeline/dsm_cqueue.h"
#include "pipeline/miscutils.h"
#include "postmaster/bgworker.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "storage/proc.h"

#define MAGIC 0xDEADBABE /* x_x */
#define WAIT_SLEEP_NS 250
#define MAX_WAIT_SLEEP_NS 5000 /* 5ms */

static inline uint64_t
dsm_cqueue_offset(dsm_cqueue *cq, uint64_t ptr)
{
	Assert(cq->size > 0);
	return ptr % cq->size;
}


static inline bool
dsm_cqueue_needs_wrap(dsm_cqueue *cq, uint64_t start, int len)
{
	/* Is there enough space left or do we cross the physical boundary? */
	return (start % cq->size) + len > cq->size;
}

static inline dsm_cqueue_slot *
dsm_cqueue_slot_get(dsm_cqueue *cq, uint64_t ptr)
{
	return (dsm_cqueue_slot *) ((uintptr_t) cq->bytes + dsm_cqueue_offset(cq, ptr));
}

void
dsm_cqueue_init(void *ptr, Size size, int tranche_id)
{
	dsm_cqueue *cq;

	if (size <= sizeof(dsm_cqueue))
		elog(ERROR, "dsm_segment too small");

	cq = (dsm_cqueue *) ptr;
	if (cq->magic == MAGIC)
		elog(ERROR, "dsm_cqueue already initialized");

	MemSet((char *) cq, 0, size);

	/* We leave enough space for a dsm_cqueue_slot to go at the end, so we never overflow the buffer. */
	cq->size = size - sizeof(dsm_cqueue) - sizeof(dsm_cqueue_slot);

	/* Initialize atomic types. */
	atomic_init(&cq->head, 0);
	atomic_init(&cq->tail, 0);
	atomic_init(&cq->cursor, 0);
	atomic_init(&cq->producer_latch, 0);
	atomic_init(&cq->consumer_latch, 0);

	/* Initialize producer lock. */
	LWLockInitialize(&cq->lock, tranche_id);

	/* This marks the dsm_cqueue as initialized. */
	cq->magic = MAGIC;
}

bool
dsm_cqueue_is_empty(dsm_cqueue *cq)
{
	return atomic_load(&cq->head) == atomic_load(&cq->tail);
}

bool
dsm_cqueue_has_unread(dsm_cqueue *cq)
{
	return atomic_load(&cq->head) > atomic_load(&cq->cursor);
}

void
dsm_cqueue_push(dsm_cqueue *cq, void *ptr, int len)
{
	dsm_cqueue_lock(cq);
	dsm_cqueue_push_nolock(cq, ptr, len);
	dsm_cqueue_unlock(cq);
}

void
dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len)
{
	uint64_t head;
	uint64_t tail;
	Latch *producer_latch;
	Latch *consumer_latch;
	dsm_cqueue_slot *slot;
	int len_needed;
	char *pos;
	bool needs_wrap = false;

	Assert(cq->magic == MAGIC);
	Assert(LWLockHeldByMe(&cq->lock));

	len_needed = sizeof(dsm_cqueue_slot) + len;
	if (len_needed > cq->size)
		elog(ERROR, "item size %d exceeds dsm_cqueue size %d", len, cq->size);

	head = atomic_load(&cq->head);

	/*
	 *If we need to wrap around, we waste the space at the end of the buffer. This is simpler
	 * than splitting up the data to be written into two separate continuous memory areas.
	 */
	if (dsm_cqueue_needs_wrap(cq, head, len_needed))
	{
		needs_wrap = true;

		/* Account for garbage space at the end, and no longer require space for dsm_cqueue_slot. */
		len_needed = len + cq->size - dsm_cqueue_offset(cq, head);
	}

	/*
	 * Set ourselves as the producer before trying to read the tail. This ensures consistency with the reader,
	 * because the reader will always update the tail before reading the value for producer latch. So in case the
	 * reader reads the value for producer_latch before the writer sets it, the writer is guaranteed to see the
	 * updated value for tail and therefore the effect is the same as being woken up after that change was made.
	 */
	producer_latch = &MyProc->procLatch;
	atomic_store(&cq->producer_latch, producer_latch);

	/* FIXME(usmanm): On postmaster shutdown, this stays looping, we must break out. */
	for (;;)
	{
		int space_used;

		tail = atomic_load(&cq->tail);
		space_used = head - tail;

		/* Is there enough space in the buffer? */
		if (cq->size - space_used >= len_needed)
			break;

		WaitLatch(producer_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
		CHECK_FOR_INTERRUPTS();
		ResetLatch(producer_latch);
	}

	atomic_store(&cq->producer_latch, NULL);

	slot = dsm_cqueue_slot_get(cq, head);
	slot->len = len;
	slot->wraps = needs_wrap;
	slot->peeked = false;

	/*
	 * If we're wrapping around, copy into the start of buffer, otherwise copy
	 * ahead of this slot.
	 */
	if (needs_wrap)
		pos = cq->bytes;
	else
		pos = slot->bytes;

	memset(pos, 0, len);

	/* Copy over data. */
	if (cq->copy_fn)
		cq->copy_fn(pos, ptr, len);
	else
		memcpy(pos, ptr, len);

	head += len_needed;
	slot->next = head;
	atomic_store(&cq->head, head);

	consumer_latch = (Latch *) atomic_load(&cq->consumer_latch);
	if (consumer_latch)
		SetLatch(consumer_latch);
}

void *
dsm_cqueue_peek_next(dsm_cqueue *cq, int *len)
{
	dsm_cqueue_slot *slot;
	char *pos;

	Assert(cq->magic == MAGIC);

	/* Are there any unread slots? */
	if (!dsm_cqueue_has_unread(cq))
	{
		*len = 0;
		return NULL;
	}

	slot = dsm_cqueue_slot_get(cq, atomic_load(&cq->cursor));

	if (slot->wraps)
		pos = cq->bytes;
	else
		pos = slot->bytes;

	atomic_store(&cq->cursor, slot->next);

	*len = slot->len;

	if (cq->peek_fn && !slot->peeked)
	{
		cq->peek_fn(pos, slot->len);
		slot->peeked = true;
	}

	return pos;
}

void
dsm_cqueue_unpeek(dsm_cqueue *cq)
{
	Assert(cq->magic == MAGIC);
	atomic_store(&cq->cursor, atomic_load(&cq->tail));
}

bool
dsm_cqueue_has_unpopped(dsm_cqueue *cq)
{
	Assert(cq->magic == MAGIC);
	return atomic_load(&cq->tail) == atomic_load(&cq->cursor);
}

void
dsm_cqueue_pop_peeked(dsm_cqueue *cq)
{
	Latch *producer_latch;
	uint64_t tail;
	uint64_t cur;

	Assert(cq->magic == MAGIC);

	tail = atomic_load(&cq->tail);
	cur = atomic_load(&cq->cursor);

	Assert(tail <= cur);

	if (cq->pop_fn)
	{
		uint64_t start = tail;

		while (start < cur)
		{
			dsm_cqueue_slot *slot = dsm_cqueue_slot_get(cq, start);
			char *pos;

			if (slot->wraps)
				pos = cq->bytes;
			else
				pos = slot->bytes;

			cq->pop_fn(pos, slot->len);
			start = slot->next;
		}
	}

	/*
	 * We must update tail before setting the latch, because the producer will set its latch
	 * before getting the new value for tail. This guarantees that we don't
	 * miss a wake up.
	 */
	atomic_store(&cq->tail, cur);
	producer_latch = (Latch *) atomic_load(&cq->producer_latch);
	if (producer_latch != NULL)
		SetLatch(producer_latch);
}

void
dsm_cqueue_wait_non_empty(dsm_cqueue *cq, int timeoutms)
{
	Latch *consumer_latch;
	uint64_t head;
	uint64_t tail;
	int flags;

	Assert(cq->magic == MAGIC);

	head = atomic_load(&cq->head);
	tail = atomic_load(&cq->tail);

	Assert(tail <= head);

	if (tail < head)
		return;

	consumer_latch = &MyProc->procLatch;
	atomic_store(&cq->consumer_latch, consumer_latch);

	flags = WL_LATCH_SET | WL_POSTMASTER_DEATH;
	if (timeoutms > 0)
		flags |= WL_TIMEOUT;

	for (;;)
	{
		head = atomic_load(&cq->head);

		if (head > tail)
			break;

		WaitLatch(consumer_latch, flags, timeoutms);
		CHECK_FOR_INTERRUPTS();
		ResetLatch(consumer_latch);
	}

	atomic_store(&cq->consumer_latch, NULL);
}

void
dsm_cqueue_set_handlers(dsm_cqueue *cq, dsm_cqueue_peek_fn peek_fn,
		dsm_cqueue_pop_fn pop_fn, dsm_cqueue_copy_fn cpy_fn)
{
	cq->peek_fn = peek_fn;
	cq->pop_fn = pop_fn;
	cq->copy_fn = cpy_fn;
}

void
dsm_cqueue_lock(dsm_cqueue *cq)
{
	Assert(cq->magic == MAGIC);
	LWLockAcquire(&cq->lock, LW_EXCLUSIVE);
}

void
dsm_cqueue_unlock(dsm_cqueue *cq)
{
	Assert(cq->magic == MAGIC);
	LWLockRelease(&cq->lock);
}

bool
dsm_cqueue_lock_nowait(dsm_cqueue *cq)
{
	Assert(cq->magic == MAGIC);
	return LWLockConditionalAcquire(&cq->lock, LW_EXCLUSIVE);
}
