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

#include "nodes/pg_list.h"
#include "storage/spin.h"

/* Represents a single batch of inserts made into a stream. */
typedef struct StreamBatch {
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
} StreamBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct StreamBatchAck {
	StreamBatch *batch;
	int count;
} StreamBatchAck;

extern List *MyAcks;

extern StreamBatch *StreamBatchCreate(int num_readers, int num_tuples);
extern void StreamBatchWaitAndRemove(StreamBatch *batch);
extern void StreamBatchIncrementNumCTuples(StreamBatch *batch);
extern void StreamBatchMarkAcked(StreamBatchAck *batch);

#endif
