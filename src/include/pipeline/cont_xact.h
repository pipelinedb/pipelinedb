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

#include "storage/spin.h"

typedef struct StreamBatch {
	int id;
	int count;
} StreamBatch;

typedef struct StreamBatchEntry {
	int id;
	int num_wacks;
	int num_cacks;
	int total_wacks;
	int total_cacks;
	slock_t mutex;
} StreamBatchEntry;

extern void InitStreamBatchState(void);
extern StreamBatchEntry *StreamBatchEntryCreate(int num_readers, int num_tuples);
extern void StreamBatchEntryWaitAndRemove(StreamBatchEntry *entry);
extern void StreamBatchEntryIncrementTotalCAcks(StreamBatch *batch);
extern void StreamBatchEntryMarkProcessed(StreamBatch *batch, bool is_worker);

#endif
