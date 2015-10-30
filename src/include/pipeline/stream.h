/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 * Functions for handling event streams
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAM_H
#define STREAM_H

#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

/*
 * Hooks that can feed raw data to COPY when we need more flexibility than a simple file descriptor.
 */
extern int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread);
extern void *copy_iter_arg;

#define QueryIsStreaming(query) ((query)->isContinuous)
#define QueryIsCombine(query) ((query)->isCombine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool synchronous_stream_insert;
extern char *stream_targets;

extern uint64 CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples);

/* Represents a single batch of inserts made into a stream. */
typedef struct InsertBatch
{
	int id;
	/* Number of acks from workers */
	int num_wacks;
	/* Number of acks from combiners */
	int num_cacks;
	/* Total number of tuples sent to workers */
	int num_wtups;
	/* Total number of tuples sent to combiners */
	int num_ctups;
	slock_t mutex;
} InsertBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct InsertBatchAck
{
	int batch_id;
	InsertBatch *batch;
} InsertBatchAck;

typedef struct AdhocQuery
{
	int cq_id;
	int *active_flag;
	int count; 
	InsertBatchAck ack;
} AdhocQuery;

typedef struct AdhocData
{
	int num_adhoc;
	AdhocQuery *queries;

} AdhocData;

extern Bitmapset *GetStreamReaders(Oid relid);
extern InsertBatch *InsertBatchCreate(void);
extern void InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples);

extern void InsertBatchWaitAndRemoveActive(InsertBatch *batch, int num_tuples, 
							   int *active, int cq_id);

extern void InsertBatchIncrementNumCTuples(InsertBatch *batch);
extern void InsertBatchAckTuple(InsertBatchAck *ack);
extern int SendTupleToAdhoc(AdhocData *data,
							HeapTuple tup,
							TupleDesc desc,
							Size *bytes);

#endif
