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

typedef struct CQBatchEntry {
	int id;
	bool touched;
	int num_processing;
	slock_t mutex;
} CQBatchEntry;

extern void InitCQBatchState(void);
extern CQBatchEntry *BatchEntryCreate(void);
extern void BatchEntryWaitAndRemove(CQBatchEntry *entry);
extern void BatchEntryIncrementProcessors(int id);
extern void BatchEntryDecrementProcessors(int id);

#endif
