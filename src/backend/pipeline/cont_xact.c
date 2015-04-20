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
#include "storage/shmem.h"
#include "storage/spalloc.h"

#define SLEEP_MS 5

StreamBatchEntry *StreamBatchEntryCreate(int num_readers, int num_tuples)
{
	StreamBatchEntry *entry = spalloc0(sizeof(StreamBatchEntry));

	entry->id = MyProcPid;
	entry->total_wacks = num_readers * num_tuples;
	SpinLockInit(&entry->mutex);

	return entry;
}

void StreamBatchEntryWaitAndRemove(StreamBatchEntry *entry)
{
	while (entry->num_wacks < entry->total_wacks || entry->num_cacks < entry->total_cacks)
		pg_usleep(SLEEP_MS * 1000);
	spfree(entry);
}

void StreamBatchEntryIncrementTotalCAcks(StreamBatch *batch)
{
	SpinLockAcquire(&batch->entry->mutex);
	batch->entry->total_cacks++;
	SpinLockRelease(&batch->entry->mutex);
}

void StreamBatchEntryMarkProcessed(StreamBatch *batch)
{
	SpinLockAcquire(&batch->entry->mutex);
	if (IsWorker)
		batch->entry->num_wacks += batch->count;
	else
		batch->entry->num_cacks += batch->count;
	SpinLockRelease(&batch->entry->mutex);
}
