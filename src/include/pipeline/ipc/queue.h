/*-------------------------------------------------------------------------
 *
 * queue.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_QUEUE_H
#define IPC_QUEUE_H

#include "postgres.h"

#include "port/atomics.h"
#include "storage/lwlock.h"
#include "utils/timestamp.h"

typedef struct ipc_queue_slot
{
	TimestampTz time;

	uint64 next;

	bool wraps;
	bool peeked;

	int    len;
	char   bytes[1]; /* dynamically allocated */
} ipc_queue_slot;

typedef void (*ipc_queue_peek_fn) (void *ptr, int len);
typedef void (*ipc_queue_pop_fn) (void *ptr, int len);
typedef void (*ipc_queue_copy_fn) (void *dest, void *src, int len);

typedef struct ipc_queue
{
	int magic;

	LWLock *lock;
	bool produced_by_broker;
	bool consumed_by_broker;

	uint64 size; /* physical size of buffer */

	pg_atomic_uint64 head;
	pg_atomic_uint64 tail;
	uint64 cursor;

	pg_atomic_uint64 producer_latch;
	pg_atomic_uint64 consumer_latch;

	ipc_queue_peek_fn peek_fn;
	ipc_queue_pop_fn  pop_fn;
	ipc_queue_copy_fn copy_fn;

	char bytes[1]; /* length equal to size */
} ipc_queue;

extern void ipc_queue_init(void *ptr, Size size, LWLock *lock);
extern void ipc_queue_set_handlers(ipc_queue *ipcq, ipc_queue_peek_fn peek_fn,
		ipc_queue_pop_fn pop_fn, ipc_queue_copy_fn cpy_fn);

extern void *ipc_queue_peek_next(ipc_queue *ipcq, int *len);
extern void ipc_queue_unpeek_all(ipc_queue *ipcq);
extern void ipc_queue_pop_peeked(ipc_queue *ipcq);
extern void ipc_queue_wait_non_empty(ipc_queue *ipcq, int timeoutms);
extern void ipc_queue_pop_inserted_before(ipc_queue *ipcq, TimestampTz time);

extern bool ipc_queue_lock(ipc_queue *ipcq, bool wait);
extern void ipc_queue_unlock(ipc_queue *ipcq);

extern bool ipc_queue_push_nolock(ipc_queue *ipcq, void *ptr, int len, bool wait);
extern bool ipc_queue_push(ipc_queue *ipcq, void *ptr, int len, bool wait);
extern void ipc_queue_update_head(ipc_queue *ipcq, uint64 head);
extern void ipc_queue_update_tail(ipc_queue *ipcq, uint64 tail);

#define ipc_queue_offset(ipcq, ptr) ((ptr) % (ipcq)->size)
#define ipc_queue_check_overflow(ipcq, pos, len) \
	Assert((uintptr_t) (pos) + (len) < ((uintptr_t) (ipcq)->bytes + (ipcq)->size))
#define ipc_queue_free_size(ipcq, head, tail) ((int64) ipcq->size - (int64) ((head) - (tail)))
#define ipc_queue_needs_wrap(ipcq, start, len) (((start) % (ipcq)->size) + (len) > (ipcq)->size)
#define ipc_queue_slot_get(ipcq, ptr) ((ipc_queue_slot *) ((uintptr_t) (ipcq)->bytes + ipc_queue_offset((ipcq), (ptr))))
#define ipc_queue_is_empty(ipcq) (pg_atomic_read_u64(&(ipcq)->head) == pg_atomic_read_u64(&(ipcq)->tail))
#define ipc_queue_has_unread(ipcq) (pg_atomic_read_u64(&(ipcq)->head) > (ipcq)->cursor)

typedef struct ipc_multi_queue
{
	int pqueue;
	int nqueues;
	ipc_queue **queues;
} ipc_multi_queue;

extern ipc_multi_queue *ipc_multi_queue_init(ipc_queue *q1, ipc_queue *q2);
extern void ipc_multi_queue_set_priority_queue(ipc_multi_queue *ipcmq, int nq);
extern void ipc_multi_queue_wait_non_empty(ipc_multi_queue *ipcmq, int timeoutms);
extern void *ipc_multi_queue_peek_next(ipc_multi_queue *ipcmq, int *len);
extern void ipc_multi_queue_unpeek_all(ipc_multi_queue *ipcmq);
extern void ipc_multi_queue_pop_peeked(ipc_multi_queue *ipcmq);
extern bool ipc_multi_queue_is_empty(ipc_multi_queue *ipcmq);
extern bool ipc_multi_queue_has_unread(ipc_multi_queue *ipcmq);
extern void ipc_multi_queue_pop_inserted_before(ipc_multi_queue *ipcmq, TimestampTz time);

#endif
