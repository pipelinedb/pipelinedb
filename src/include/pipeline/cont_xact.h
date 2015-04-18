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
	int num_processed;
	int total;
	slock_t mutex;
} StreamBatchEntry;

extern void InitStreamBatchState(void);
extern StreamBatchEntry *StreamBatchEntryCreate(int total);
extern void StreamBatchEntryWaitAndRemove(StreamBatchEntry *entry);
extern void StreamBatchEntryMarkProcessed(StreamBatch *batch);

#endif
