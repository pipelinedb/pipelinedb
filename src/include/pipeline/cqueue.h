/*-------------------------------------------------------------------------
 *
 * cqueue.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/cqueue.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CQUEUE_H
#define CQUEUE_H

#include <stdatomic.h>

#include "postgres.h"

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/spin.h"

typedef void (*dsm_cqueue_pop_fn) (void *ptr);
typedef void (*dsm_cqueue_cpy_fn) (void *dest, void *src);

typedef struct dsm_cqueue_slot
{
	int  magic;
	int  len;
	char bytes[1]; /* dynamically allocated */
} dsm_cqueue_slot;

typedef struct dsm_cqueue
{
	int magic;

	LWLock  lock;
	LWLock  *ext_lock;

	int        size;
	dsm_handle handle;

	/*
	 * These are offsets from the address of the dsm_cqueue. Pointers don't work with
	 * dsm because the memory segment can be mapped at different addresses.
	 */
	atomic_int head;
	atomic_int tail;
	int pos;

	atomic_uintptr_t producer_latch;
	volatile Latch   *consumer_latch;

	dsm_cqueue_pop_fn *pop_fn;
	dsm_cqueue_cpy_fn *cpy_fn;

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

extern void dsm_cqueue_set_pop_fn(dsm_cqueue *cq, dsm_cqueue_pop_fn *func);
extern void dsm_cqueue_set_cpy_fn(dsm_cqueue *cq, dsm_cqueue_cpy_fn *func);
extern void dsm_cqueue_set_consumer(dsm_cqueue *cq);

extern dsm_cqueue_handle *dsm_cqueue_attach(dsm_handle handle);
extern void dsm_cqueue_deattach(dsm_cqueue_handle *cq_handle);

extern void dsm_cqueue_push(dsm_cqueue *cq, void *ptr, int len);
extern void *dsm_cqueue_peek_next(dsm_cqueue *cq, int *len);
extern void dsm_cqueue_pop_seen(dsm_cqueue *cq);
extern void dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len);
extern bool dsm_cqueue_is_empty(dsm_cqueue *cq);
extern void dsm_cqueue_sleep_if_empty(dsm_cqueue *cq);

extern void dsm_cqueue_lock_head(dsm_cqueue *cq);
extern bool dsm_cqueue_lock_head_nowait(dsm_cqueue *cq);
extern void dsm_cqueue_unlock_head(dsm_cqueue *cq);

#endif   /* CQUEUE_H */
