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

List *MyAcks = NIL;

StreamBatch *StreamBatchCreate(int num_readers, int num_tuples)
{
	StreamBatch *entry = spalloc0(sizeof(StreamBatch));

	entry->id = MyProcPid;
	entry->total_wacks = num_readers * num_tuples;
	SpinLockInit(&entry->mutex);

	return entry;
}

void StreamBatchWaitAndRemove(StreamBatch *batch)
{
	while (batch->num_wacks < batch->total_wacks || batch->num_cacks < batch->total_cacks)
		pg_usleep(SLEEP_MS * 1000);
	spfree(batch);
}

void StreamBatchIncrementTotalCAcks(StreamBatch *batch)
{
	SpinLockAcquire(&batch->mutex);
	batch->total_cacks++;
	SpinLockRelease(&batch->mutex);
}

void StreamBatchMarkAcked(StreamBatchAck *ack)
{
	SpinLockAcquire(&ack->batch->mutex);
	if (IsWorker)
		ack->batch->num_wacks += ack->count;
	else
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
