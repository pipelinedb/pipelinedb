/*-------------------------------------------------------------------------
 *
 * microbatch.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_MICROBATCH_H
#define IPC_MICROBATCH_H

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "miscutils.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "port/atomics.h"
#include "scheduler.h"

#define MAX_MICROBATCH_SIZE (continuous_query_batch_mem * 1024)

/* guc */
extern int continuous_query_batch_mem;
extern int continuous_query_batch_size;
extern int continuous_query_ipc_hwm;

extern Size MicrobatchAckShmemSize(void);
extern void MicrobatchAckShmemInit(void);

typedef struct microbatch_ack_t
{
	/* Zero if unused, top most bit indicates ack type */
	pg_atomic_uint64 id;

	/* Number of acks from workers */
	pg_atomic_uint32 num_wacks;
	/* Number of acks from combiners */
	pg_atomic_uint32 num_cacks;
	/* Total number of tuples read by workers */
	pg_atomic_uint32 num_wrecv;
	/* Total number of tuples sent to workers */
	pg_atomic_uint32 num_wtups;
	/* Total number of tuples sent to combiners */
	pg_atomic_uint32 num_ctups;
} microbatch_ack_t;

typedef enum
{
	STREAM_INSERT_ASYNCHRONOUS,
	STREAM_INSERT_SYNCHRONOUS_RECEIVE,
	STREAM_INSERT_SYNCHRONOUS_COMMIT,
	STREAM_INSERT_FLUSH /* internal */
} StreamInsertLevel;

extern microbatch_ack_t *microbatch_ack_new(StreamInsertLevel level);
extern void microbatch_ack_free(microbatch_ack_t *ack);

#define microbatch_ack_ref_is_valid(ref) ((ref)->tag == pg_atomic_read_u64(&((microbatch_ack_t *) ref->ptr)->id))
#define microbatch_ack_get_level(ack) (pg_atomic_read_u64(&ack->id) >> 62L)
#define microbatch_ack_increment_wtups(ack, n) pg_atomic_fetch_add_u32(&(ack)->num_wtups, (n))
#define microbatch_ack_increment_ctups(ack, n) pg_atomic_fetch_add_u32(&(ack)->num_ctups, (n))
#define microbatch_ack_increment_wrecv(ack, n) pg_atomic_fetch_add_u32(&(ack)->num_wrecv, (n))
#define microbatch_ack_increment_acks(ack, n) \
	do \
	{ \
		if (IsContQueryWorkerProcess()) \
			pg_atomic_fetch_add_u32(&(ack)->num_wacks, (n)); \
		else \
			pg_atomic_fetch_add_u32(&(ack)->num_cacks, (n)); \
	} \
	while (0);
#define microbatch_acks_check_and_exec(acks, fn, arg) \
	do \
	{ \
		ListCell *lc; \
		foreach(lc, (acks)) \
		{ \
			tagged_ref_t *ref = lfirst(lc); \
			microbatch_ack_t *ack = (microbatch_ack_t *) ref->ptr; \
			if (microbatch_ack_ref_is_valid(ref)) \
				fn(ack, (arg)); \
		}\
	} \
	while (0);
#define microbatch_ack_is_acked(ack) \
	(pg_atomic_read_u32(&(ack)->num_wacks) >= pg_atomic_read_u32(&(ack)->num_wtups) && \
			pg_atomic_read_u32(&(ack)->num_cacks) >= pg_atomic_read_u32(&(ack)->num_ctups))
#define microbatch_ack_is_received(ack) (pg_atomic_read_u32(&(ack)->num_wrecv) >= pg_atomic_read_u32(&(ack)->num_wtups))
extern bool microbatch_ack_wait(microbatch_ack_t *ack, ContQueryDatabaseMetadata *db_meta, uint64 start_generation);

typedef enum microbatch_type_t
{
	WorkerTuple = 1,
	CombinerTuple,
	FlushTuple
} microbatch_type_t;

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

extern microbatch_t *microbatch_new(microbatch_type_t type, Bitmapset *queries, TupleDesc desc);
extern void microbatch_destroy(microbatch_t *mb);
extern void microbatch_reset(microbatch_t *mb);

extern void microbatch_add_ack(microbatch_t *mb, microbatch_ack_t *ack);
extern bool microbatch_add_tuple(microbatch_t *mb, HeapTuple tup, uint64 group_hash);
#define microbatch_is_empty(mb) ((mb)->ntups == 0)
#define microbatch_is_writable(mb) (!(mb)->readonly)

extern char *microbatch_pack(microbatch_t *mb, int *len);
extern char *microbatch_pack_for_queue(uint64 recv_id, char *packed, int *len);
extern microbatch_t *microbatch_unpack(char *buf, int len);
extern void microbatch_send(microbatch_t *mb, uint64 recv_id, bool async, ContQueryDatabaseMetadata *db_meta);
extern void microbatch_add_acks(microbatch_t *mb, List *acks);
extern void microbatch_send_to_worker(microbatch_t *mb, int worker_id);
extern void microbatch_send_to_combiner(microbatch_t *mb, int combiner_id);

extern void microbatch_ipc_init(void);

#endif
