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

#include "postgres.h"
#include "pg_config.h"

#ifdef HAVE_STDATOMIC_H
#include <stdatomic.h>
#else
#include "stdatomic.h"
#endif

#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/spin.h"

typedef void (*dsm_cqueue_peek_fn) (void *ptr, int len);
typedef void (*dsm_cqueue_pop_fn) (void *ptr, int len);
typedef void (*dsm_cqueue_copy_fn) (void *dest, void *src, int len);

typedef struct dsm_cqueue_slot
{
	uint64_t next;
	bool     wraps;
	bool     peeked;
	int      len;
	char     bytes[1]; /* dynamically allocated */
} dsm_cqueue_slot;

typedef struct dsm_cqueue
{
	int magic;

	LWLock  lock;

	int        size; /* physical size of buffer */

	atomic_ullong head;
	atomic_ullong tail;
	atomic_ullong cursor;

	atomic_uintptr_t producer_latch;
	atomic_uintptr_t consumer_latch;

	dsm_cqueue_peek_fn peek_fn;
	dsm_cqueue_pop_fn  pop_fn;
	dsm_cqueue_copy_fn copy_fn;

	char bytes[1];
} dsm_cqueue;

extern void dsm_cqueue_init(void *ptr, Size size, int tranche_id);
extern void dsm_cqueue_set_handlers(dsm_cqueue *cq, dsm_cqueue_peek_fn peek_fn,
		dsm_cqueue_pop_fn pop_fn, dsm_cqueue_copy_fn cpy_fn);

extern void dsm_cqueue_push(dsm_cqueue *cq, void *ptr, int len);
extern void dsm_cqueue_push_nolock(dsm_cqueue *cq, void *ptr, int len);
extern void *dsm_cqueue_peek_next(dsm_cqueue *cq, int *len);
extern void dsm_cqueue_unpeek(dsm_cqueue *cq);
extern void dsm_cqueue_pop_peeked(dsm_cqueue *cq);
extern bool dsm_cqueue_is_empty(dsm_cqueue *cq);
extern bool dsm_cqueue_has_unpopped(dsm_cqueue *cq);
extern bool dsm_cqueue_has_unread(dsm_cqueue *cq);
extern void dsm_cqueue_wait_non_empty(dsm_cqueue *cq, int timeoutms);

extern void dsm_cqueue_lock(dsm_cqueue *cq);
extern bool dsm_cqueue_lock_nowait(dsm_cqueue *cq);
extern void dsm_cqueue_unlock(dsm_cqueue *cq);

extern void dsm_cqueue_test(void);

#endif   /* DSM_CQUEUE_H */
