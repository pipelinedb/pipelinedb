/*-------------------------------------------------------------------------
 *
 * queue.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef _H
#define _H

#include "postgres.h"

#include "storage/lwlock.h"

typedef struct ipc_queue ipc_queue;

typedef struct ipc_queue mp_queue;
typedef struct ipc_queue sp_queue;

typedef void (*ipc_queue_peek_fn) (void *ptr, int len);
typedef void (*ipc_queue_pop_fn) (void *ptr, int len);
typedef void (*ipc_queue_copy_fn) (void *dest, void *src, int len);

extern void ipc_queue_init(void *ptr, Size size, LWLock *lock);
#define mp_queue_init(ptr, size, lock) ((mp_queue *) ipc_queue_init(ptr, size, lock))
#define sp_queue_init(ptr, size) ((mp_queue *) ipc_queue_init(ptr, size, NULL))

extern void ipc_queue_set_handlers(ipc_queue *ipcq, ipc_queue_peek_fn peek_fn,
		ipc_queue_pop_fn pop_fn, ipc_queue_copy_fn cpy_fn);

extern void *ipc_queue_peek_next(ipc_queue *ipcq, int *len);
extern void ipc_queue_unpeek(ipc_queue *ipcq);
extern void ipc_queue_pop_peeked(ipc_queue *ipcq);
extern bool ipc_queue_is_empty(ipc_queue *ipcq);
extern bool ipc_queue_has_unpopped(ipc_queue *ipcq);
extern bool ipc_queue_has_unread(ipc_queue *ipcq);
extern void ipc_queue_wait_non_empty(ipc_queue *ipcq, int timeoutms);

extern void mp_queue_lock(mp_queue *ipcq);
extern bool mp_queue_lock_nowait(mp_queue *ipcq);
extern void mp_queue_unlock(mp_queue *ipcq);
extern bool mp_queue_push(mp_queue *ipcq, void *ptr, int len, bool wait);

extern bool sp_queue_push(sp_queue *ipcq, void *ptr, int len, bool wait);

#endif
