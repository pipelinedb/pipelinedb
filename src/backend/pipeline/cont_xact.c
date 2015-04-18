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

void InitStreamBatchState(void)
{
	HASHCTL info;

	info.keysize = sizeof(int);
	info.entrysize = sizeof(StreamBatchEntry);

	LWLockAcquire(PipelineMetadataLock, LW_EXCLUSIVE);

	CQBatchTable = ShmemInitHash("CQBatchTable",
			max_worker_processes / 8, max_worker_processes,
			&info,
			HASH_ELEM);

	LWLockRelease(PipelineMetadataLock);
}

StreamBatchEntry *StreamBatchEntryCreate(int num_readers, int num_tuples)
{
	int id;
	StreamBatchEntry *entry;
	bool found = true;

	while (found)
	{
		id = rand();
		entry = (StreamBatchEntry *) hash_search(CQBatchTable, &id, HASH_ENTER, &found);
	}

	entry->id = id;
	entry->num_wacks = 0;
	entry->num_cacks = 0;
	entry->total_wacks = num_readers * num_tuples;
	entry->total_cacks = 0;
	SpinLockInit(&entry->mutex);

	return entry;
}

void StreamBatchEntryWaitAndRemove(StreamBatchEntry *entry)
{
	while (entry->num_wacks < entry->total_wacks || entry->num_cacks < entry->total_cacks)
		pg_usleep(SLEEP_MS * 1000);
	hash_search(CQBatchTable, &entry->id, HASH_REMOVE, NULL);
}

void StreamBatchEntryIncrementTotalCAcks(StreamBatch *batch)
{
	bool found;
	StreamBatchEntry *entry = (StreamBatchEntry *) hash_search(CQBatchTable, &batch->id, HASH_FIND, &found);
	SpinLockAcquire(&entry->mutex);
	entry->total_cacks++;
	SpinLockRelease(&entry->mutex);
}

void StreamBatchEntryMarkProcessed(StreamBatch *batch)
{
	bool found;
	StreamBatchEntry *entry = (StreamBatchEntry *) hash_search(CQBatchTable, &batch->id, HASH_FIND, &found);
	SpinLockAcquire(&entry->mutex);
	if (IsWorker)
		entry->num_wacks += batch->count;
	else
		entry->num_cacks += batch->count;
	SpinLockRelease(&entry->mutex);
}
