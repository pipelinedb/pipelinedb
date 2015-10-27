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
#include "pipeline/streamReceiver.h"
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
#include "pipeline/cont_adhoc_mgr.h"

#define SLEEP_MS 2

#define StreamBatchAllAcked(batch) ((batch)->num_wacks >= (batch)->num_wtups && (batch)->num_cacks >= (batch)->num_ctups)

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets = NULL;

int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread) = NULL;
void *copy_iter_arg = NULL;

Bitmapset *
GetStreamReaders(Oid relid)
{
	Bitmapset *targets = GetLocalStreamReaders(relid);
	char *name = get_rel_name(relid);

	if (targets == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", name),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.", name)));
	}

	if (!continuous_queries_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot insert into stream %s since continuous queries are disabled", name),
				 errhint("Enable continuous queries using the \"continuous_queries_enabled\" parameter.")));

	return targets;
}

/*
 * Initialize data structures to support sending data to adhoc tuple buffer
 */
static void
init_adhoc_data(AdhocData *data, Bitmapset *adhoc_targets)
{
	int num_adhoc = bms_num_members(adhoc_targets);
	int ctr = 0;
	int target = 0;
	Bitmapset *tmp_targets;

	memset(data, 0, sizeof(AdhocData));
	data->num_adhoc = num_adhoc;

	if (!data->num_adhoc)
		return;

	tmp_targets = bms_copy(adhoc_targets);
	data->queries = palloc0(sizeof(AdhocQuery) * num_adhoc);

	while ((target = bms_first_member(tmp_targets)) >= 0)
	{
		AdhocQuery *query = &data->queries[ctr++];

		query->cq_id = target;
		query->active_flag = AdhocMgrGetActiveFlag(target);
		query->count = 0;

		if (synchronous_stream_insert)
		{
			query->ack.batch = InsertBatchCreate();
			query->ack.batch_id = query->ack.batch->id;
			query->ack.count = 1;
		}
	}
}

/*
 * Send a heap tuple to the adhoc buffer, by making a new stream tuple
 * for each adhoc query that is listening
 */
int
SendTupleToAdhoc(AdhocData *adhoc_data, HeapTuple tup, TupleDesc desc,
					Size *bytes)
{
	int i = 0;
	int count = 0;
	*bytes = 0;

	for (i = 0; i < adhoc_data->num_adhoc; ++i)
	{
		AdhocQuery *query = &adhoc_data->queries[i];

		StreamTuple *tuple = 
			MakeStreamTuple(tup, desc, 1, &query->ack);
		Bitmapset *single = bms_make_singleton(query->cq_id);

		tuple->group_hash = query->cq_id;

		if (TupleBufferInsert(AdhocTupleBuffer, tuple, single))
		{
			query->count++;

			if (i == 0)
			{
				count++;
				(*bytes) += tuple->heaptup->t_len + HEAPTUPLESIZE;
			}
		}
	}

	return count;
}

static void
WaitForAdhoc(AdhocData *adhoc_data)
{
	int i = 0;
	for (i = 0; i < adhoc_data->num_adhoc; ++i)
	{
		AdhocQuery *query = &adhoc_data->queries[i];

		InsertBatchWaitAndRemoveActive(query->ack.batch,
									   query->count,
									   query->active_flag,
									   query->cq_id);
	}
}

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
uint64
CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	uint64 count = 0;
	int i;
	InsertBatchAck acks[1];
	InsertBatch *batch = NULL;
	int num_batches = 0;
	Size size = 0;
	bool snap = ActiveSnapshotSet();

	Bitmapset *all_targets = GetStreamReaders(RelationGetRelid(stream));
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();

	Bitmapset *targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = continuous_queries_adhoc_enabled ? 
		bms_difference(all_targets, targets) : NULL;

	int num_worker = bms_num_members(targets);

	AdhocData adhoc_data;
	init_adhoc_data(&adhoc_data, adhoc_targets);

	if (synchronous_stream_insert && num_worker)
	{
		batch = InsertBatchCreate();
		num_batches = 1;

		acks[0].batch_id = batch->id;
		acks[0].batch = batch;
		acks[0].count = 1;
	}

	if (snap)
		PopActiveSnapshot();

	for (i=0; i<ntuples; i++)
	{
		HeapTuple htup = tuples[i];
		StreamTuple *tuple;

		if (num_worker)
		{
			tuple = MakeStreamTuple(htup, desc, num_batches, acks);
			TupleBufferInsert(WorkerTupleBuffer, tuple, targets);
			count++;
			size += tuple->heaptup->t_len + HEAPTUPLESIZE;
		}

		if (adhoc_data.num_adhoc)
		{
			int acount = 0;
			Size abytes = 0;

			acount = SendTupleToAdhoc(&adhoc_data, htup, desc, &abytes);

			if (!num_worker)
			{
				count += acount;
				size += abytes;
			}
		}
	}

	stream_stat_report(RelationGetRelid(stream), count, 1, size);

	/*
	 * Wait till the last event has been consumed by a CV before returning.
	 */
	if (synchronous_stream_insert)
	{
		if (num_worker)
		{
			InsertBatchWaitAndRemove(batch, count);
		}

		if (adhoc_data.num_adhoc)
		{
			WaitForAdhoc(&adhoc_data);
		}
	}

	if (snap)
		PushActiveSnapshot(GetTransactionSnapshot());

	return count;
}

InsertBatch *
InsertBatchCreate(void)
{
	char *ptr = ShmemDynAlloc0(sizeof(InsertBatch));
	InsertBatch *batch = (InsertBatch *) ptr;

	batch->id = rand() ^ (int) MyProcPid;
	SpinLockInit(&batch->mutex);

	return batch;
}

void
InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples)
{
	if (num_tuples)
	{
		batch->num_wtups = num_tuples;
		while (!StreamBatchAllAcked(batch))
		{
			pg_usleep(SLEEP_MS * 1000);
			CHECK_FOR_INTERRUPTS();
		}
	}

	ShmemDynFree(batch);
}

/* 
 * Waits for a batch to be acked, but breaks early if it 
 * detects that the adhoc cq is no longer active. 
 */
void
InsertBatchWaitAndRemoveActive(InsertBatch *batch, int num_tuples, 
							   int *active, int cq_id)
{
	if (num_tuples && active)
	{
		batch->num_wtups = num_tuples;

		while (!StreamBatchAllAcked(batch) && (*active == cq_id))
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
	SpinLockAcquire(&batch->mutex);
	batch->num_ctups++;
	SpinLockRelease(&batch->mutex);
}

void
InsertBatchMarkAcked(InsertBatchAck *ack)
{
	if (ack->batch_id != ack->batch->id)
		return;

	SpinLockAcquire(&ack->batch->mutex);
	if (IsContQueryWorkerProcess() || IsContQueryAdhocProcess())
		ack->batch->num_wacks += ack->count;
	else if (IsContQueryCombinerProcess())
		ack->batch->num_cacks += ack->count;
	SpinLockRelease(&ack->batch->mutex);
}
