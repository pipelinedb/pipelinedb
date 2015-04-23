/*-------------------------------------------------------------------------
 *
 * cont_xact.h
 *
 *	  Interface for transactional behavior for continuous queries
 *
 * src/include/pipeline/cont_xact.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_XACT_H
#define CONT_XACT_H

#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "storage/spin.h"

/* Represents a single batch of inserts made into a stream. */
typedef struct StreamBatch {
	int id;
	/* CQ ids that need to read this batch */
	Bitmapset *readers;
	/* Num tuples */
	int num_tups;
	/* Num proc starts */
	int *proc_runs;
	/* Number of reads from workers */
	int num_wreads;
	/* Number of reads from combiners */
	int num_creads;
	/* Number of acks from workers */
	int num_wacks;
	/* Number of acks from combiners */
	int num_cacks;
	/* Total number of tuples sent to workers */
	int num_wtups;
	/* Total number of tuples sent to combiners */
	int num_ctups;
	slock_t mutex;
} StreamBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct StreamBatchAck {
	int batch_id;
	StreamBatch *batch;
	int count;
} StreamBatchAck;

extern List *MyAcks;

extern StreamBatch *StreamBatchCreate(Bitmapset *readers, int num_tuples);
extern void StreamBatchWaitAndRemove(StreamBatch *batch);
extern void StreamBatchIncrementNumCTuples(StreamBatch *batch);
extern void StreamBatchIncrementNumReads(StreamBatch *batch);
extern void StreamBatchMarkAcked(StreamBatchAck *batch);

#endif
