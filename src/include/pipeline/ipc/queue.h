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

#include "storage/lwlock.h"

typedef struct ipc_queue_slot ipc_queue_slot;
typedef void (*ipc_queue_peek_fn) (void *ptr, int len);
typedef void (*ipc_queue_pop_fn) (void *ptr, int len);
typedef void (*ipc_queue_copy_fn) (void *dest, void *src, int len);

typedef struct ipc_queue
{
	int magic;

	LWLock *lock;
	bool used_by_router;

	Size size; /* physical size of buffer */

	pg_atomic_uint64 head;
	pg_atomic_uint64 tail;
	pg_atomic_uint64 cursor;

	pg_atomic_uint64 producer_latch;
	pg_atomic_uint64 consumer_latch;

	ipc_queue_peek_fn peek_fn;
	ipc_queue_pop_fn  pop_fn;
	ipc_queue_copy_fn copy_fn;

	char bytes[1]; /* length equal to size */
} ipc_queue;

extern void ipc_queue_init(void *ptr, Size size, LWLock *lock, bool used_by_router);
extern void ipc_queue_set_handlers(ipc_queue *ipcq, ipc_queue_peek_fn peek_fn,
		ipc_queue_pop_fn pop_fn, ipc_queue_copy_fn cpy_fn);

extern void *ipc_queue_peek_next(ipc_queue *ipcq, int *len);
extern void ipc_queue_unpeek(ipc_queue *ipcq);
extern void ipc_queue_pop_peeked(ipc_queue *ipcq);
extern bool ipc_queue_is_empty(ipc_queue *ipcq);
extern bool ipc_queue_has_unpopped(ipc_queue *ipcq);
extern bool ipc_queue_has_unread(ipc_queue *ipcq);
extern void ipc_queue_wait_non_empty(ipc_queue *ipcq, int timeoutms);

extern bool ipc_queue_lock(ipc_queue *ipcq, bool wait);
extern void ipc_queue_unlock(ipc_queue *ipcq);

extern bool ipc_queue_push_nolock(ipc_queue *ipcq, void *ptr, int len, bool wait);
extern bool ipc_queue_push(ipc_queue *ipcq, void *ptr, int len, bool wait);

#endif
