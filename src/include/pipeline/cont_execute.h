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

#ifdef HAVE_STDATOMIC_H
#include <stdatomic.h>
#else
#include "stdatomic.h"
#endif

#include "access/htup.h"
#include "nodes/bitmapset.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* Represents a single batch of inserts made into a stream. */
typedef struct InsertBatch
{
	int id;
	/* Number of acks from workers */
	atomic_int num_wacks;
	/* Number of acks from combiners */
	atomic_int num_cacks;
	/* Total number of tuples sent to workers */
	int num_wtups;
	/* Total number of tuples sent to combiners */
	atomic_int num_ctups;
} InsertBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct InsertBatchAck
{
	int batch_id;
	InsertBatch *batch;
} InsertBatchAck;

extern InsertBatch *InsertBatchCreate(void);
extern void InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples);
extern void InsertBatchWaitAndRemoveActive(InsertBatch *batch, int num_tuples,
							   int *active, int cq_id);
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

	InsertBatchAck *ack; /* the ack this tuple is responsible for */

	int num_record_descs; /* number of tuple descriptors for RECORD types */
	RecordTupleDesc *record_descs; /* RECORD type cached tuple descriptors */

	Bitmapset *queries;
} StreamTupleState;

extern void StreamTupleStatePopFn(void *ptr, int len);
extern void *StreamTupleStatePeekFn(void *ptr, int len);
extern void StreamTupleStateCopyFn(void *dest, void *src, int len);

typedef struct PartialTupleState
{
	HeapTuple tup;

	int nacks;
	InsertBatchAck *acks;

	uint64 hash;
	Oid query_id;
} PartialTupleState;

extern void PartialTupleStatePopFn(void *ptr, int len);
extern void *PartialTupleStatePeekFn(void *ptr, int len);
extern void PartialTupleStateCopyFn(void *dest, void *src, int len);

#endif
