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
#include "storage/dsm_alloc.h"

#define SLEEP_MS 5

List *MyAcks = NIL;

StreamBatch *StreamBatchCreate(int num_readers, int num_tuples)
{
	StreamBatch *entry = dsm_alloc0(sizeof(StreamBatch));

	entry->id = MyProcPid;
	entry->num_wtups = num_readers * num_tuples;
	SpinLockInit(&entry->mutex);

	return entry;
}

void StreamBatchWaitAndRemove(StreamBatch *batch)
{
	while (batch->num_wacks < batch->num_wtups || batch->num_cacks < batch->num_ctups)
		pg_usleep(SLEEP_MS * 1000);
	dsm_free(batch);
}

void StreamBatchIncrementNumCTuples(StreamBatch *batch)
{
	SpinLockAcquire(&batch->mutex);
	batch->num_ctups++;
	SpinLockRelease(&batch->mutex);
}

//void StreamBatchIncrementNumReads(StreamBatch *batch)
//{
//	SpinLockAcquire(&batch->mutex);
//	if (IsWorker)
//		batch->num_wacks++;
//	else
//		batch->num_cacks++;
//	SpinLockRelease(&batch->mutex);
//}

void StreamBatchMarkAcked(StreamBatchAck *ack)
{
	SpinLockAcquire(&ack->batch->mutex);
	if (IsWorker)
		ack->batch->num_wacks += ack->count;
	else
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
