/*-------------------------------------------------------------------------
 *
 * dsm_cqueue.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/dsm_cqueue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef DSM_CQUEUE_H
#define DSM_CQUEUE_H

#include <stdatomic.h>

#include "postgres.h"

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/spin.h"

typedef void (*dsm_cqueue_pop_fn) (void *ptr, int len);
typedef void (*dsm_cqueue_cpy_fn) (void *dest, void *src, int len);

typedef struct dsm_cqueue_slot
{
	uint64_t next;
	bool     wraps;
	int      len;
	char     bytes[1]; /* dynamically allocated */
} dsm_cqueue_slot;

typedef struct dsm_cqueue
{
	int magic;

	LWLock  lock;
	LWLock  *ext_lock;

	int        size; /* physical size of buffer */
	dsm_handle handle;

	/*
	 * These are offsets from the address of the dsm_cqueue. Pointers don't work with
	 * dsm because the memory segment can be mapped at different addresses.
	 */
	atomic_ullong  head;
	atomic_ullong  tail;
	atomic_ullong  cursor;

	atomic_uintptr_t producer_latch;
	atomic_uintptr_t consumer_latch;

	dsm_cqueue_pop_fn pop_fn;
	dsm_cqueue_cpy_fn cpy_fn;

	char bytes[1];
} dsm_cqueue;

typedef struct dsm_cqueue_handle
{
	MemoryContext cxt;
	dsm_segment   *segment;
	dsm_cqueue    *cqueue;
	LWLockTranche *tranche;
} dsm_cqueue_handle;

extern void dsm_cqueue_init(dsm_handle handle);
extern void dsm_cqueue_init_with_ext_lock(dsm_handle handle, LWLock *lock);

extern void dsm_cqueue_set_handlers(dsm_cqueue *cq, dsm_cqueue_pop_fn pop_fn, dsm_cqueue_cpy_fn func);

extern dsm_cqueue_handle *dsm_cqueue_attach(dsm_handle handle);
extern void dsm_cqueue_detach(dsm_cqueue_handle *cq_handle);

extern void dsm_cqueue_push(dsm_cqueue *cq, void *ptr, int len);
extern void *dsm_cqueue_peek_next(dsm_cqueue *cq, int *len);
extern void dsm_cqueue_pop_seen(dsm_cqueue *cq);
extern void dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len);
extern bool dsm_cqueue_is_empty(dsm_cqueue *cq);
extern bool dsm_cqueue_has_unread(dsm_cqueue *cq);
extern void dsm_cqueue_sleep_if_empty(dsm_cqueue *cq);

extern void dsm_cqueue_lock_head(dsm_cqueue *cq);
extern bool dsm_cqueue_lock_head_nowait(dsm_cqueue *cq);
extern void dsm_cqueue_unlock_head(dsm_cqueue *cq);

extern void dsm_cqueue_test(void);

#endif   /* DSM_CQUEUE_H */
