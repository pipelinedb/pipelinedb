/*-------------------------------------------------------------------------
 *
 * cqueue.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/backend/pipeline/cqueue.c
 *
 *-------------------------------------------------------------------------
 */

#include "pipeline/cqueue.h"
#include "utils/memutils.h"
#include "storage/proc.h"

#define MAGIC 0x1CEB00DA

static dsm_cqueue *
dsm_cqueue_base_init(dsm_handle handle)
{
	dsm_segment *segment;
	Size size;
	dsm_cqueue *cq;

	segment = dsm_attach(handle);
	if (segment == NULL)
		elog(ERROR, "dsm segment missing");

	size = dsm_segment_map_length(segment);
	if (size <= sizeof(dsm_cqueue))
		elog(ERROR, "dsm_segment too small");

	cq = (dsm_cqueue *) dsm_segment_address(segment);
	if (cq->magic == MAGIC)
		elog(ERROR, "dsm_cqueue already initialized");

	MemSet((char *) cq, 0, size);

	cq->handle = handle;
	cq->size = size - sizeof(dsm_cqueue);

	/* Initialize atomic ints */
	atomic_init(&cq->head, 0);
	atomic_init(&cq->tail, 0);
	atomic_init(&cq->pos, 0);
	atomic_init(&cq->producer_latch, NULL);

	/* This marks the dsm_cqueue as initialized. */
	cq->magic = MAGIC;

	return cq;
}

void
dsm_cqueue_init(dsm_handle handle)
{
	dsm_cqueue *cq = dsm_cqueue_base_init(handle);
	/* Obtain a new LWLock and initialize it. */
	int tranche_id = LWLockNewTrancheId();
	LWLockInitialize(&cq->lock, tranche_id);
	cq->ext_lock = NULL;
}

void
dsm_cqueue_init_with_ext_lock(dsm_handle handle, LWLock *lock)
{
	dsm_cqueue *cq = dsm_cqueue_base_init(handle);
	cq->ext_lock = lock;
}

dsm_cqueue_handle *
dsm_cqueue_attach(dsm_handle handle)
{
	dsm_segment *segment;
	dsm_cqueue *cq;
	dsm_cqueue_handle *cq_handle;
	MemoryContext old;
	MemoryContext cxt;

	/* Allocate dsm_cqueue handles in long-lived memory contexts */
	cxt = AllocSetContextCreate(TopMemoryContext, "dsm_cqueue MemoryContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(cxt);

	/* Make sure we're not already attached. */
	segment = dsm_find_mapping(handle);
	if (segment == NULL)
		segment = dsm_attach(handle);

	if (segment == NULL)
		elog(ERROR, "dsm segment missing");

	cq = (dsm_cqueue *) dsm_segment_address(segment);
	if (cq->magic != MAGIC)
		elog(ERROR, "dsm_cqueue is not initialized");

	cq_handle = palloc0(sizeof(dsm_cqueue_handle));
	cq_handle->cxt = cxt;
	cq_handle->segment = segment;
	cq_handle->cqueue = cq;

	cq_handle->tranche = palloc0(sizeof(LWLockTranche));
	cq_handle->tranche->name = "dsm_cqueue LWLock";
	cq_handle->tranche->array_base = &cq->lock;
	cq_handle->tranche->array_stride = sizeof(LWLock);

	LWLockRegisterTranche(cq->lock.tranche, cq_handle->tranche);

	MemoryContextSwitchTo(old);

	return cq_handle;
}

void
dsm_cqueue_deattach(dsm_cqueue_handle *cq_handle)
{
	dsm_detach(cq_handle->segment);
	MemoryContextDelete(cq_handle->cxt);
}

static LWLock *
dsm_cqueue_get_lock(dsm_cqueue *cq)
{
	if (cq->ext_lock)
		return cq->ext_lock;
	return &cq->lock;
}

bool
dsm_cqueue_is_empty(dsm_cqueue *cq)
{
	return atomic_load(&cq->head) == atomic_load(&cq->tail);
}

void
dsm_cqueue_push(dsm_cqueue *cq, void *ptr, int len)
{
	dsm_cqueue_lock_head(cq);
	dsm_cqueue_push_nolock(cq, ptr, len);
	dsm_cqueue_unlock_head(cq);
}

void
dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len)
{
	LWLock *lock = dsm_cqueue_get_lock(cq);
	int head;
	int tail;
	int start;
	int end;
	Latch *my_latch;
	int slot_len = sizeof(dsm_cqueue_slot) + len;

	Assert(LWLockHeldByMe(lock));

	if (slot_len > cq->size)
		elog(ERROR, "item size %d exceeds dsm_cqueue size %d", len, cq->size);

	/* Set ourselves as the producer. */
	my_latch = &MyProc->procLatch;
	atomic_store(&cq->producer_latch, my_latch);

	/*
	 * head is only modified while holding the dsm_cqueue's lock, so we don't need any
	 * memory order guarantees.
	 */
	head = atomic_load(&cq->head);
	tail = atomic_load(&cq->tail);

	/* Is queue empty? */
	if (head == tail)
	{
		Assert(tail == cq->pos);

		start = 0;
		end = cq->size;
	}
	else
	{
		start = head;

		/* If the tail is in front of us, don't go beyond it. */
		if (tail > head)
			end = tail;
		else
		{
			end = cq->size;

			/* If there isn't enough space left in the buffer, wrap around. */
			if (end - start < slot_len)
			{
				start = 0;
				end = tail;
			}
		}
	}

	/* If we still don't have enough room, wait for tail to move forward. */
	while (end - start < len)
	{
		ResetLatch(my_latch);
		tail = atomic_load(&cq->tail);

		/* Has tail moved? */
		if (tail != end)
		{
			/* Is queue empty? */
			if (head == tail)
			{
				start = 0;
				end = cq->size;
			}
			else
				end = tail;
		}

		/* Still not enough room? */
		if (end - start < slot_len)
			WaitLatch(my_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
	}

	/* We have enough space to */
}

void *
dsm_cqueue_peek_next(dsm_cqueue *cq, int *len)
{
	return NULL;
}

void
dsm_cqueue_pop_seen(dsm_cqueue *cq)
{
	Latch *producer_latch;
	int tail = atomic_load(&cq->tail);

	/* Is there anything to pop? */
	if (tail == cq->pos)
		return;

	Assert(tail != atomic_load(&cq->head));
	producer_latch = (Latch *) atomic_load(&cq->producer_latch);

	/* TODO(usmanm): Call on pop on each slot */

	/*
	 * We must update tail before setting the latch, because the producer will call
	 * ResetLatch before getting the new value for tail. This guarantees that we don't
	 * miss a wake up. If ResetLatch clears a notification from us, then it must be the
	 * case that the producer sees an updated value for tail.
	 */
	atomic_store(&cq->tail, cq->pos);
	SetLatch(producer_latch);
}

void
dsm_cqueue_sleep_if_empty(dsm_cqueue *cq)
{

}

void
dsm_cqueue_set_pop_fn(dsm_cqueue *cq, dsm_cqueue_pop_fn *func)
{
	cq->pop_fn = func;
}

void
dsm_cqueue_set_consumer(dsm_cqueue *cq)
{
	cq->consumer_latch = &MyProc->procLatch;
}

void
dsm_cqueue_lock_head(dsm_cqueue *cq)
{
	LWLockAcquire(dsm_cqueue_get_lock(cq), LW_EXCLUSIVE);
}

void
dsm_cqueue_unlock_head(dsm_cqueue *cq)
{
	LWLockRelease(dsm_cqueue_get_lock(cq));
}

bool
dsm_cqueue_lock_head_nowait(dsm_cqueue *cq)
{
	return LWLockConditionalAcquire(dsm_cqueue_get_lock(cq), LW_EXCLUSIVE);
}
