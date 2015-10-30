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
#include "postmaster/bgworker.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "storage/proc.h"

#define MAGIC 0xDEADBABE /* x_x */
#define WAIT_SLEEP_NS 250
#define MAX_WAIT_SLEEP_NS 5000 /* 5ms */

static dsm_segment *
dsm_find_or_attach(dsm_handle handle)
{
	dsm_segment *segment = dsm_find_mapping(handle);
	if (segment == NULL)
		segment = dsm_attach(handle);
	return segment;
}

static dsm_cqueue *
dsm_cqueue_base_init(dsm_handle handle)
{
	dsm_segment *segment;
	Size size;
	dsm_cqueue *cq;

	segment = dsm_find_or_attach(handle);
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
	/* We leave enough space for a dsm_cqueue_slot to go at the end, so we never overflow the buffer. */
	cq->size = size - sizeof(dsm_cqueue) - sizeof(dsm_cqueue_slot);

	/* Initialize atomic types. */
	atomic_init(&cq->head, 0);
	atomic_init(&cq->tail, 0);
	atomic_init(&cq->cursor, 0);
	atomic_init(&cq->producer_latch, 0);
	atomic_init(&cq->consumer_latch, 0);

	/* This marks the dsm_cqueue as initialized. */
	cq->magic = MAGIC;

	return cq;
}

static inline LWLock *
dsm_cqueue_get_lock(dsm_cqueue *cq)
{
	if (cq->ext_lock)
		return cq->ext_lock;
	return &cq->lock;
}

static inline int
dsm_cqueue_offset(dsm_cqueue *cq, int ptr)
{
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
	return (dsm_cqueue_slot *) (cq->bytes + dsm_cqueue_offset(cq, ptr));
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

	segment = dsm_find_or_attach(handle);
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

	LWLockRegisterTranche(dsm_cqueue_get_lock(cq)->tranche, cq_handle->tranche);

	MemoryContextSwitchTo(old);

	return cq_handle;
}

void
dsm_cqueue_detach(dsm_cqueue_handle *cq_handle)
{
	dsm_detach(cq_handle->segment);
	MemoryContextDelete(cq_handle->cxt);
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
	dsm_cqueue_lock_head(cq);
	dsm_cqueue_push_nolock(cq, ptr, len);
	dsm_cqueue_unlock_head(cq);
}

void
dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len)
{
	LWLock *lock;
	uint64_t head;
	uint64_t tail;
	Latch *producer_latch;
	Latch *consumer_latch;
	dsm_cqueue_slot *slot;
	int len_needed;
	char *pos;
	bool needs_wrap;

	lock = dsm_cqueue_get_lock(cq);

	Assert(LWLockHeldByMe(lock));

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

	/*
	 * If we're wrapping around, copy into the start of buffer, otherwise copy
	 * ahead of this slot.
	 */
	if (needs_wrap)
		pos = cq->bytes;
	else
		pos = slot->bytes;

	/* Copy over data. */
	if (cq->cpy_fn)
		cq->cpy_fn(pos, ptr, len);
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

	*len = slot->len;

	atomic_store(&cq->cursor, slot->next);

	return pos;
}

void
dsm_cqueue_pop_seen(dsm_cqueue *cq)
{
	Latch *producer_latch;
	uint64_t tail = atomic_load(&cq->tail);
	uint64_t cur = atomic_load(&cq->cursor);

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

#ifdef CLOBBER_FREED_MEMORY
	{
		int cur_offset = dsm_cqueue_offset(cq, cur);
		int tail_offset = dsm_cqueue_offset(cq, tail);
		char *tail_pos = (char *) dsm_cqueue_slot_get(cq, tail);

		if (tail_offset > cur_offset)
		{
			MemSet(cq->bytes, 0x7F, cur_offset);
			MemSet(tail_pos, 0x7F, cq->size - tail_offset);
		}
		else
			MemSet(tail_pos, 0x7F, cur_offset - tail_offset);
	}
#endif

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
dsm_cqueue_sleep_if_empty(dsm_cqueue *cq)
{
	Latch *consumer_latch;
	uint64_t head = atomic_load(&cq->head);
	uint64_t tail = atomic_load(&cq->tail);

	Assert(tail <= head);

	if (tail < head)
		return;

	consumer_latch = &MyProc->procLatch;
	atomic_store(&cq->consumer_latch, consumer_latch);

	for (;;)
	{
		head = atomic_load(&cq->head);

		if (head > tail)
			break;

		WaitLatch(consumer_latch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
		CHECK_FOR_INTERRUPTS();
		ResetLatch(consumer_latch);
	}

	atomic_store(&cq->consumer_latch, NULL);
}

void
dsm_cqueue_set_handlers(dsm_cqueue *cq, dsm_cqueue_pop_fn pop_fn, dsm_cqueue_cpy_fn cpy_fn)
{
	cq->pop_fn = pop_fn;
	cq->cpy_fn = cpy_fn;
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

static void
dsm_cqueue_check_consistency(dsm_cqueue *cq)
{
	dsm_segment *segment;
	uint64_t head;
	uint64_t tail;
	uint64_t cursor;

	Assert(cq->magic == MAGIC);
	Assert(cq->handle > 0);

	segment = dsm_find_or_attach(cq->handle);

	Assert(cq->size == dsm_segment_map_length(segment) - sizeof(dsm_cqueue) - sizeof(dsm_cqueue_slot));

	head = atomic_load(&cq->head);
	tail = atomic_load(&cq->tail);
	cursor = atomic_load(&cq->cursor);

	Assert(cursor <= head);
	Assert(tail <= head);

	while (tail < head)
	{
		dsm_cqueue_slot *slot = dsm_cqueue_slot_get(cq, tail);

		if (!slot->wraps)
			Assert(slot->next == tail + slot->len + sizeof(dsm_cqueue_slot));
		else
		{
			Assert(slot->next % cq->size == slot->len);
			Assert(slot->next == tail + slot->len + cq->size - dsm_cqueue_offset(cq, tail));
		}

		tail = slot->next;
	}
}

static char *
dsm_cqueue_print(dsm_cqueue *cq)
{
	StringInfo buf = makeStringInfo();
	char *str;
	uint64_t head = atomic_load(&cq->head);
	uint64_t tail = atomic_load(&cq->tail);
	int nitems = 0;
	uint64_t start = tail;

	while (start < head)
	{
		dsm_cqueue_slot *slot = dsm_cqueue_slot_get(cq, start);
		start = slot->next;
		nitems++;
	}

	appendStringInfo(buf, "dsm_cqueue\n");
	appendStringInfo(buf, "  handle: %d\n", cq->handle);
	appendStringInfo(buf, "  size: %d\n", cq->size);
	appendStringInfo(buf, "  head: %ld\n", head);
	appendStringInfo(buf, "  tail: %ld\n", tail);
	appendStringInfo(buf, "  cursor: %ld\n", (uint64_t) atomic_load(&cq->cursor));
	appendStringInfo(buf, "  items: %d\n", nitems);
	appendStringInfo(buf, "  free_space: %ld", cq->size - (head - tail));

	str = buf->data;
	pfree(buf);

	return str;
}

static void
dsm_cqueue_test_serial(void)
{
	dsm_segment *segment = dsm_create(8192);
	dsm_handle handle = dsm_segment_handle(segment);
	dsm_cqueue_handle *cq_handle;
	dsm_cqueue *cq;
	int i;

	dsm_cqueue_init(handle);
	cq_handle = dsm_cqueue_attach(handle);
	cq = cq_handle->cqueue;

	dsm_cqueue_check_consistency(cq);

	for (i = 0; i < 1000; i++)
	{
		int nitems = rand() % 224; /* max items we can insert before the insert blocks */
		int j;
		char *hw = "hello world";
		int len;
		char *bytes;

		for (j = 0; j < nitems; j++)
		{
			dsm_cqueue_push(cq, hw, strlen(hw) + 1);
			dsm_cqueue_check_consistency(cq);
		}

		for (j = 0; j < nitems; j++)
		{
			bytes = dsm_cqueue_peek_next(cq, &len);
			Assert(len == strlen(hw) + 1);
			Assert(strcmp(bytes, hw) == 0);
			dsm_cqueue_check_consistency(cq);
		}

		Assert(!dsm_cqueue_has_unread(cq));
		bytes = dsm_cqueue_peek_next(cq, &len);
		Assert(len == 0);
		Assert(bytes == NULL);

		dsm_cqueue_pop_seen(cq);
		Assert(dsm_cqueue_is_empty(cq));
	}

	dsm_cqueue_detach(cq_handle);
}

static void
producer_main(Datum arg)
{
	dsm_handle handle = (dsm_handle) arg;
	dsm_cqueue_handle *cq_handle;
	dsm_cqueue *cq;
	ResourceOwner res;
	int i;
	char data[1024];

	BackgroundWorkerUnblockSignals();

	res = ResourceOwnerCreate(NULL, "producer_main");
	CurrentResourceOwner = res;

	cq_handle = dsm_cqueue_attach(handle);
	cq = cq_handle->cqueue;

	for (i = 0; i < 1000000; i++)
	{
		int len = rand() % 1024 + 1;
		dsm_cqueue_push(cq, data, len);

		pg_usleep(rand() % 10);

		if (i % 10000 == 0)
			elog(LOG, "produced: %d", i);
	}

	dsm_cqueue_detach(cq_handle);

	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(res);
}

static void
dsm_cqueue_test_concurrent(void)
{
	BackgroundWorker worker;
	BackgroundWorkerHandle *bg_handle;
	pid_t pid;
	dsm_segment *segment;
	dsm_handle seg_handle;
	dsm_cqueue_handle *cq_handle;
	dsm_cqueue *cq;
	int nitems;
	bool printed;

	worker.bgw_flags = BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_SHMEM_ACCESS;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = producer_main;
	worker.bgw_notify_pid = MyProcPid;
	sprintf(worker.bgw_name, "dsm_cqueue test producer process");

	segment = dsm_create(8192);
	seg_handle = dsm_segment_handle(segment);
	dsm_cqueue_init(seg_handle);
	cq_handle = dsm_cqueue_attach(seg_handle);
	cq = cq_handle->cqueue;

	worker.bgw_main_arg = seg_handle;
	Assert(RegisterDynamicBackgroundWorker(&worker, &bg_handle));
	Assert(WaitForBackgroundWorkerStartup(bg_handle, &pid) == BGWH_STARTED);

	for (nitems = 0; nitems < 1000000;)
	{
		int len;
		char *bytes = dsm_cqueue_peek_next(cq, &len);

		if (bytes == NULL)
			dsm_cqueue_pop_seen(cq);
		else
			nitems++;

		dsm_cqueue_sleep_if_empty(cq);

		pg_usleep(rand() % 10);

		if (nitems % 10000 == 0)
		{
			if (!printed)
				elog(LOG, "consumed: %d", nitems);
			printed = true;
		}
		else
			printed = false;
	}

	dsm_cqueue_print(cq);
	dsm_cqueue_detach(cq_handle);
}

void
dsm_cqueue_test(void)
{
	dsm_cqueue_test_serial();
	dsm_cqueue_test_concurrent();
}
