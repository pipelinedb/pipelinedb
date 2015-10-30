/*-------------------------------------------------------------------------
 *
 * cont_execute.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/cont_execute.c
 *
 */

#include "postgres.h"

#include "access/htup.h"
#include "miscadmin.h"
#include "pipeline/cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/miscutils.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"

#define SLEEP_MS 1

void
PartialTupleStateCopyFn(void *dest, void *src, int len)
{
	PartialTupleState *pts = (PartialTupleState *) src;
	PartialTupleState *cpypts = (PartialTupleState *) dest;
	char *pos = (char *) dest;

	memcpy(pos, pts, sizeof(PartialTupleState));
	pos += sizeof(PartialTupleState);

	cpypts->tup = ptr_difference(dest, pos);
	memcpy(pos, pts->tup, HEAPTUPLESIZE);
	pos += HEAPTUPLESIZE;
	memcpy(pos, pts->tup->t_data, pts->tup->t_len);
	pos += pts->tup->t_len;

	if (synchronous_stream_insert)
	{
		cpypts->acks = ptr_difference(dest, pos);
		memcpy(pos, pts->acks, sizeof(InsertBatchAck) * pts->nacks);
		pos += sizeof(InsertBatchAck) * pts->nacks;
	}

	Assert((uintptr_t) ptr_difference(dest, pos) == len);
}

void *
PartialTupleStatePeekFn(void *ptr, int len)
{
	PartialTupleState *pts = (PartialTupleState *) ptr;
	pts->acks = ptr_offset(pts, pts->acks);
	pts->tup = ptr_offset(pts, pts->tup);
	pts->tup->t_data = (HeapTupleHeader) (((char *) pts->tup) + HEAPTUPLESIZE);

	return pts;
}

void
PartialTupleStatePopFn(void *ptr, int len)
{
	PartialTupleState *pts = (PartialTupleState *) ptr;
	InsertBatchAck *acks = ptr_offset(pts, pts->acks);
	int i;

	for (i = 0; i < pts->nacks; i++)
		InsertBatchAckTuple(&acks[i]);
}

void
StreamTupleStateCopyFn(void *dest, void *src, int len)
{
	StreamTupleState *sts = (StreamTupleState *) src;
	StreamTupleState *cpysts = (StreamTupleState *) dest;
	char *pos = (char *) dest;

	memcpy(pos, sts, sizeof(StreamTupleState));
	pos += sizeof(StreamTupleState);

	cpysts->desc = ptr_difference(dest, pos);
	memcpy(pos, sts->desc, VARSIZE(sts->desc));
	pos += VARSIZE(sts->desc);

	cpysts->tup = ptr_difference(dest, pos);
	memcpy(pos, sts->tup, HEAPTUPLESIZE);
	pos += HEAPTUPLESIZE;
	memcpy(pos, sts->tup->t_data, sts->tup->t_len);
	pos += sts->tup->t_len;

	if (synchronous_stream_insert)
	{
		cpysts->ack = ptr_difference(dest, pos);
		memcpy(pos, sts->ack, sizeof(InsertBatchAck));
		pos += sizeof(InsertBatchAck);
	}

	if (sts->num_record_descs)
	{
		int i;

		Assert(!IsContQueryProcess());

		cpysts->record_descs = ptr_difference(dest, pos);

		for (i = 0; i < sts->num_record_descs; i++)
		{
			RecordTupleDesc *rdesc = &sts->record_descs[i];
			memcpy(pos, rdesc, sizeof(RecordTupleDesc));
			pos += sizeof(RecordTupleDesc);
			memcpy(pos, rdesc->desc, VARSIZE(rdesc->desc));
			pos += VARSIZE(rdesc->desc);
		}
	}

	cpysts->queries = ptr_difference(dest, pos);
	memcpy(pos, sts->queries, BITMAPSET_SIZE(sts->queries->nwords));
	pos += BITMAPSET_SIZE(sts->queries->nwords);

	Assert((uintptr_t) ptr_difference(dest, pos) == len);
}

void *
StreamTupleStatePeekFn(void *ptr, int len)
{
	StreamTupleState *sts = (StreamTupleState *) ptr;
	sts->ack = ptr_offset(sts, sts->ack);
	sts->desc =  ptr_offset(sts, sts->desc);
	sts->queries = ptr_offset(sts, sts->queries);
	sts->record_descs = ptr_offset(sts, sts->record_descs);
	sts->tup = ptr_offset(sts, sts->tup);
	sts->tup->t_data = (HeapTupleHeader) (((char *) sts->tup) + HEAPTUPLESIZE);

	return sts;
}

void
StreamTupleStatePopFn(void *ptr, int len)
{
	StreamTupleState *sts = (StreamTupleState *) ptr;
	InsertBatchAck *ack = ptr_offset(sts, sts->ack);
	InsertBatchAckTuple(ack);
}


InsertBatch *
InsertBatchCreate(void)
{
	InsertBatch *batch = (InsertBatch *) ShmemDynAlloc0(sizeof(InsertBatch));
	batch->id = rand() ^ (int) MyProcPid;
	return batch;
}

static inline bool
InsertBatchAllAcked(InsertBatch *batch)
{
	return (atomic_load(&batch->num_wacks) >= batch->num_wtups &&
			atomic_load(&batch->num_cacks) >= atomic_load(&batch->num_ctups));
}

void
InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples)
{
	if (num_tuples)
	{
		batch->num_wtups = num_tuples;
		while (!InsertBatchAllAcked(batch))
		{
			pg_usleep(SLEEP_MS * 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	ShmemDynFree(batch);
}

void
InsertBatchIncrementNumCTuples(InsertBatch *batch)
{
	atomic_fetch_add(&batch->num_ctups, 1);
}

void
InsertBatchAckTuple(InsertBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	if (IsContQueryWorkerProcess())
		atomic_fetch_add(&ack->batch->num_wacks, 1);
	else if (IsContQueryCombinerProcess())
		atomic_fetch_add(&ack->batch->num_cacks, 1);
}
