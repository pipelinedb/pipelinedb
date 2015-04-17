/*-------------------------------------------------------------------------
 *
 * cont_xact.c
 *
 *	  Transactional behavior for continuous queries
 *
 * src/backend/pipeline/cont_xact.c
 *
 *-------------------------------------------------------------------------
 */
#include <stdlib.h>
#include "postgres.h"

#include "miscadmin.h"
#include "pipeline/cont_xact.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/hsearch.h"

#define SLEEP_MS 5

static HTAB *CQBatchTable = NULL;

void InitCQBatchState(void)
{
	HASHCTL info;

	info.keysize = sizeof(int);
	info.entrysize = sizeof(CQBatchEntry);

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	CQBatchTable = ShmemInitHash("CQBatchTable",
			max_worker_processes / 8, max_worker_processes,
			&info,
			HASH_ELEM);

	LWLockRelease(PipelineMetadataLock);
}

CQBatchEntry *BatchEntryCreate(void)
{
	int id = rand();
	CQBatchEntry *entry;
	bool found = true;

	while (found)
		entry = (CQBatchEntry *) hash_search(CQBatchTable, &id, HASH_ENTER, &found);

	entry->id = id;
	entry->touched = false;
	entry->num_processing = 0;
	SpinLockInit(&entry->mutex);

	return entry;
}

void BatchEntryWaitAndRemove(CQBatchEntry *entry)
{
	while (!entry->touched || entry->num_processing)
		pg_usleep(SLEEP_MS * 1000);
	hash_search(CQBatchTable, &entry->id, HASH_REMOVE, NULL);
}

void BatchEntryIncrementProcessors(int id)
{
	bool found;
	CQBatchEntry *entry = (CQBatchEntry *) hash_search(CQBatchTable, &id, HASH_FIND, &found);
	SpinLockAcquire(&entry->mutex);
	entry->num_processing++;
	entry->touched = true;
	SpinLockRelease(&entry->mutex);
}

void BatchEntryDecrementProcessors(int id)
{
	bool found;
	CQBatchEntry *entry = (CQBatchEntry *) hash_search(CQBatchTable, &id, HASH_FIND, &found);
	SpinLockAcquire(&entry->mutex);
	entry->num_processing--;
	SpinLockRelease(&entry->mutex);
}
