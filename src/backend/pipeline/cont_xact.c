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
#include "nodes/bitmapset.h"
#include "pipeline/cont_xact.h"
#include "storage/shmem.h"
#include "storage/spalloc.h"

#define SLEEP_MS 5

List *MyAcks = NIL;

StreamBatch *StreamBatchCreate(Bitmapset *readers, int num_tuples)
{
	StreamBatch *entry = spalloc0(sizeof(StreamBatch));

	entry->id = MyProcPid;
	entry->num_wtups = bms_num_members(readers) * num_tuples;
	SpinLockInit(&entry->mutex);

	return entry;
}

void StreamBatchWaitAndRemove(StreamBatch *batch)
{
	while (batch->num_wacks < batch->num_wtups || batch->num_cacks < batch->num_ctups)
		pg_usleep(SLEEP_MS * 1000);
	spfree(batch);
}

void StreamBatchIncrementNumCTuples(StreamBatch *batch)
{
	SpinLockAcquire(&batch->mutex);
	batch->num_ctups++;
	SpinLockRelease(&batch->mutex);
}

void StreamBatchIncrementNumReads(StreamBatch *batch)
{
	SpinLockAcquire(&batch->mutex);
	if (IsWorker)
		batch->num_wacks++;
	else
		batch->num_cacks++;
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
