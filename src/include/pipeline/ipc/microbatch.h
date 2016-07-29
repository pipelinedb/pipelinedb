/*-------------------------------------------------------------------------
 *
 * microbatch.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_MICROBATCH_H
#define IPC_MICROBATCH_H

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/miscutils.h"
#include "port/atomics.h"

/* guc */
extern int continuous_query_num_batch;
extern int continuous_query_batch_size;

typedef enum microbatch_type_t
{
	WorkerTuple = 1,
	CombinerTuple
} microbatch_type_t;

typedef struct microbatch_ack_t
{
	int id;
	/* Number of acks from workers */
	pg_atomic_uint32 num_wacks;
	/* Number of acks from combiners */
	pg_atomic_uint32 num_cacks;
	/* Total number of tuples sent to workers */
	pg_atomic_uint32 num_wtups;
	/* Total number of tuples sent to combiners */
	pg_atomic_uint32 num_ctups;
} microbatch_ack_t;

extern microbatch_ack_t *microbatch_ack_new(void);
extern void microbatch_ack_wait_and_free(microbatch_ack_t *ack);
#define microbatch_ack_increment_wtups(ack, n) pg_atomic_fetch_add_u32(&(ack)->num_wtups, (n));
#define microbatch_ack_increment_ctups(ack, n) pg_atomic_fetch_add_u32(&(ack)->num_ctups, (n));
#define microbatch_ack_increment_acks(ack, n) \
	do \
	{ \
		if (IsContQueryWorkerProcess()) \
			pg_atomic_fetch_add_u32(&(ack)->num_wacks, (n)); \
		else \
			pg_atomic_fetch_add_u32(&(ack)->num_cacks, (n)); \
	} \
	while (0);
#define microbatch_ack_is_acked(ack) \
	(pg_atomic_read_u32(&(ack)->num_wacks) >= pg_atomic_read_u32(&(ack)->num_wtups) && \
			pg_atomic_read_u32(&(ack)->num_cacks) >= pg_atomic_read_u32(&(ack)->num_ctups))

typedef struct microbatch_t
{
	microbatch_type_t type;
	bool allow_iter;
	int packed_size;

	TupleDesc desc;
	Bitmapset *queries;

	List *acks;
	List *record_descs;

	tagged_ref_t *tups;
	int ntups;
	StringInfo buf;
} microbatch_t;

extern microbatch_t *microbatch_new(microbatch_type_t type, Bitmapset *queries, TupleDesc desc, uint64 hash);
extern void microbatch_destroy(microbatch_t *mb);
extern void microbatch_reset(microbatch_t *mb);

extern void microbatch_add_ack(microbatch_t *mb, microbatch_ack_t *ack);
extern bool microbatch_add_tuple(microbatch_t *mb, HeapTuple tup, uint64 group_hash);
#define microbatch_is_empty(mb) ((mb)->ntups == 0)
#define microbatch_is_writable(mb) (!(mb)->readonly)

extern char *microbatch_pack(microbatch_t *mb, int *len);
extern microbatch_t *microbatch_unpack(char *buf, int len);

#endif
