/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Functions for handling event streams
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/backend/pipeline/stream.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "funcapi.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/analyze.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_target.h"
#include "pgstat.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/stream.h"
#include "storage/shm_alloc.h"
#include "storage/ipc.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "utils/typcache.h"

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets;

int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread) = NULL;
void *copy_iter_arg = NULL;

uint64
SendTuplesToContWorkers(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples, InsertBatchAck *acks, int nacks)
{
	ipc_queue *ipcq;
	Bitmapset *all_targets = GetLocalStreamReaders(RelationGetRelid(stream));
	Bitmapset *adhoc = GetAdhocContinuousViewIds();
	Bitmapset *targets = bms_difference(all_targets, adhoc);
	bytea *packed_desc;
	int i;
	int free;
	uint64 tail;
	uint64 head;
	int nbatches = 1;
	uint64 size = 0;
	int ninserted;
	TimestampTz now = GetCurrentTimestamp();

	bms_free(all_targets);
	bms_free(adhoc);

	/* No reader? Noop. */
	if (bms_is_empty(targets))
		return 0;

	packed_desc = PackTupleDesc(desc);

	ipcq = get_worker_queue_with_lock();
	head = pg_atomic_read_u64(&ipcq->head);
	tail = pg_atomic_read_u64(&ipcq->tail);
	free = ipc_queue_free_size(ipcq, head, tail);
	ninserted = 0;

	Assert(free >= 0);

	for (i = 0; i < ntuples; i++)
	{
		HeapTuple tup = tuples[i];
		int len;
		StreamTupleState *sts = StreamTupleStateCreate(tup, desc, packed_desc, targets, acks, nacks, &len);
		ipc_queue_slot *slot = ipc_queue_slot_get(ipcq, head);
		int len_needed = sizeof(ipc_queue_slot) + len;
		bool needs_wrap = ipc_queue_needs_wrap(ipcq, head, len_needed);
		char *dest_bytes;

		Assert(IsContQueryWorkerProcess() ? ipcq->consumed_by_broker : true);
		Assert(tail <= head);

		if (needs_wrap)
			len_needed = len + ipcq->size - ipc_queue_offset(ipcq, head);

		if (free < len_needed)
		{
			tail = pg_atomic_read_u64(&ipcq->tail);
			free = ipc_queue_free_size(ipcq, head, tail);
		}

		if (free < len_needed || ninserted >= continuous_query_batch_size)
		{
			ipc_queue_update_head(ipcq, head);
			ipc_queue_unlock(ipcq);

			ipcq = get_worker_queue_with_lock();

			head = pg_atomic_read_u64(&ipcq->head);
			tail = pg_atomic_read_u64(&ipcq->tail);
			free = ipc_queue_free_size(ipcq, head, tail);

			if (ninserted)
				nbatches++;

			now = GetCurrentTimestamp();
			ninserted = 0;

			/* retry this tuple */
			i--;

			continue;
		}

		size += len;
		ninserted++;

		free -= len_needed;
		head += len_needed;

		slot->time = now;
		slot->len = len;
		slot->peeked = false;
		slot->wraps = needs_wrap;
		slot->next = head;

		dest_bytes = needs_wrap ? ipcq->bytes : slot->bytes;
		ipc_queue_check_overflow(ipcq, dest_bytes, len);

		StreamTupleStateCopyFn(dest_bytes, sts, len);

		if (sts->record_descs)
			pfree(sts->record_descs);
		pfree(sts);
	}

	ipc_queue_update_head(ipcq, head);
	ipc_queue_unlock(ipcq);

	pgstat_increment_stream_insert(RelationGetRelid(stream), ntuples, nbatches, size);

	bms_free(targets);
	pfree(packed_desc);

	return size;
}

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
void
CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	InsertBatchAck *ack = NULL;
	InsertBatch *batch = NULL;
	bool snap = ActiveSnapshotSet();

	if (snap)
		PopActiveSnapshot();

	if (synchronous_stream_insert)
	{
		batch = InsertBatchCreate();
		ack = palloc0(sizeof(InsertBatchAck));
		ack->batch_id = batch->id;
		ack->batch = batch;
	}

	SendTuplesToContWorkers(stream, desc, tuples, ntuples, ack, ack ? 1 : 0);

	if (batch)
	{
		pfree(ack);
		InsertBatchWaitAndRemove(batch, ntuples);
	}

	if (snap)
		PushActiveSnapshot(GetTransactionSnapshot());
}

extern
Datum pipeline_stream_insert(PG_FUNCTION_ARGS)
{
	elog(ERROR, "pipeline_stream_insert can only be used by continuous transforms");
}
