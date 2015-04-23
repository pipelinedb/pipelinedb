/* Copyright (c) 2013-2015 PipelineDB */
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
#include "pipeline/cqproc.h"
#include "storage/dsm_alloc.h"

#define SLEEP_MS 5
#define CHECK_CRASH_CYCLES 5

List *MyAcks = NIL;

#define StreamBatchAllAcked(batch) ((batch)->num_wacks == (batch)->num_wtups && (batch)->num_cacks == (batch)->num_ctups)
#define StreamBatchAllRead(batch) ((batch)->num_wreads == (batch)->num_wtups && (batch)->num_creads == (batch)->num_ctups)

StreamBatch *StreamBatchCreate(Bitmapset *readers, int num_tuples)
{
	char *ptr = dsm_alloc0(sizeof(StreamBatch) + BITMAPSET_SIZE(readers->nwords) + (bms_num_members(readers) * sizeof(int)));
	StreamBatch *batch = (StreamBatch *) ptr;
	int cq_id;
	int i = 0;

	batch->id = rand() ^ (int) MyProcPid;
	batch->num_tups = num_tuples;
	batch->num_wtups = bms_num_members(readers) * num_tuples;
	SpinLockInit(&batch->mutex);

	ptr += sizeof(StreamBatch);
	batch->readers = (Bitmapset *) ptr;
	memcpy(batch->readers, readers, BITMAPSET_SIZE(readers->nwords));


	ptr += BITMAPSET_SIZE(readers->nwords);
	batch->proc_runs = (int *) ptr;

	readers = bms_copy(readers);
	while ((cq_id = bms_first_member(readers)) != -1)
	{
		CQProcEntry *pentry = GetCQProcEntry(cq_id);
		batch->proc_runs[i] = Max(pentry->proc_runs, pentry->pg_size);
		i++;
	}
	pfree(readers);

	return batch;
}

static int num_cq_crashes(StreamBatch *batch)
{
	int cq_id;
	int num_crashes = 0;
	Bitmapset *readers = bms_copy(batch->readers);
	int i = 0;

	while ((cq_id = bms_first_member(readers)) != -1)
	{
		CQProcEntry *pentry = GetCQProcEntry(cq_id);

		if (!pentry)
			num_crashes++;
		else if (batch->proc_runs[i] < pentry->proc_runs)
			num_crashes++;
		i++;
	}

	pfree(readers);

	return num_crashes;
}

void StreamBatchWaitAndRemove(StreamBatch *batch)
{
	int cycle = 0;

	while (!StreamBatchAllAcked(batch))
	{
		if (cycle % CHECK_CRASH_CYCLES == 0)
		{
			int num_crashes = num_cq_crashes(batch);

			cycle = 0;

			if (num_crashes == 0)
				continue;

			// All tuples have been read, and we've received acks from all workers that didn't crash.
			if (StreamBatchAllRead(batch) &&
					batch->num_wacks >= (batch->num_tups * (bms_num_members(batch->readers) - num_crashes)))
				break;
		}

		pg_usleep(SLEEP_MS * 1000);

		cycle++;
	}

	dsm_free(batch);
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
		batch->num_wreads++;
	else
		batch->num_creads++;
	SpinLockRelease(&batch->mutex);
}

void StreamBatchMarkAcked(StreamBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	SpinLockAcquire(&ack->batch->mutex);
	if (IsWorker)
		ack->batch->num_wacks += ack->count;
	else
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
