/*-------------------------------------------------------------------------
 *
 * cont_execute.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/pipeline/cont_execute.h
 *
 */
#ifndef CONT_EXECUTE_H
#define CONT_EXECUTE_H

#include "postgres.h"
#include "pg_config.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "catalog/pipeline_query_fn.h"
#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/dsm_cqueue.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* Represents a single batch of inserts made into a stream. */
typedef struct InsertBatch
{
	int id;
	/* Number of acks from workers */
	pg_atomic_uint32 num_wacks;
	/* Number of acks from combiners */
	pg_atomic_uint32 num_cacks;
	/* Total number of tuples sent to workers */
	int num_wtups;
	/* Total number of tuples sent to combiners */
	pg_atomic_uint32 num_ctups;
} InsertBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct InsertBatchAck
{
	int batch_id;
	InsertBatch *batch;
} InsertBatchAck;

extern InsertBatch *InsertBatchCreate(void);
extern void InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples);
extern void InsertBatchIncrementNumCTuples(InsertBatch *batch);
extern void InsertBatchAckTuple(InsertBatchAck *ack);

typedef struct RecordTupleDesc
{
	int32 typmod;
	bytea *desc;
} RecordTupleDesc;

typedef struct StreamTupleState
{
	TimestampTz arrival_time;

	bytea *desc;
	HeapTuple tup;

	int num_record_descs; /* number of tuple descriptors for RECORD types */
	RecordTupleDesc *record_descs; /* RECORD type cached tuple descriptors */

	InsertBatchAck *ack; /* the ack this tuple is responsible for */
	Bitmapset *queries;
} StreamTupleState;

extern void StreamTupleStatePopFn(void *ptr, int len);
extern void StreamTupleStatePeekFn(void *ptr, int len);
extern void StreamTupleStateCopyFn(void *dest, void *src, int len);
extern StreamTupleState *StreamTupleStateCreate(HeapTuple tup, TupleDesc desc, bytea *packed_desc,
		Bitmapset *queries, InsertBatchAck *ack, int *len);

typedef struct PartialTupleState
{
	HeapTuple tup;

	int nacks;
	InsertBatchAck *acks;

	uint64 hash;
	Oid query_id;

	/* For pipelinedb_enterprise */
	NameData cv;
	NameData namespace;
} PartialTupleState;

extern void PartialTupleStatePopFn(void *ptr, int len);
extern void PartialTupleStatePeekFn(void *ptr, int len);
extern void PartialTupleStateCopyFn(void *dest, void *src, int len);

extern dsm_cqueue *GetWorkerQueue(void);
extern dsm_cqueue *GetCombinerQueue(PartialTupleState *pts);

typedef struct ContQueryState
{
	Oid query_id;
	ContQuery *query;
	MemoryContext state_cxt;
	MemoryContext tmp_cxt;
	CQStatEntry stats;
} ContQueryState;

typedef struct ContExecutor ContExecutor;
typedef ContQueryState *(*ContQueryStateInit) (ContExecutor *exec, ContQueryState *state);

struct ContExecutor
{
	MemoryContext cxt;
	MemoryContext exec_cxt;

	ContQueryProcType ptype;

	dsm_cqueue *cqueue;
	Bitmapset *queries;
	bool update_queries;

	Bitmapset *exec_queries;
	void *cursor;
	int nitems;

	Timestamp start_time;
	bool started;
	bool timedout;
	bool depleted;
	List *yielded;

	Bitmapset *queries_seen;

	Oid current_query_id;
	ContQueryState *current_query;
	ContQueryState *states[MAX_CQS];
	ContQueryStateInit initfn;
};

extern ContExecutor *ContExecutorNew(ContQueryProcType type, ContQueryStateInit initfn);
extern void ContExecutorDestroy(ContExecutor *exec);
extern void ContExecutorStartBatch(ContExecutor *exec);
extern Oid ContExecutorStartNextQuery(ContExecutor *exec);
extern void ContExecutorPurgeQuery(ContExecutor *exec);
extern void *ContExecutorYieldItem(ContExecutor *exec, int *len);
extern void ContExecutorEndQuery(ContExecutor *exec);
extern void ContExecutorEndBatch(ContExecutor *exec, bool commit);

#endif
