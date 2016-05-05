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
#include "pipeline/cont_execute.h"
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

/*
 * CopyIntoStream
 *
 * COPY events to a stream from an input source
 */
void
CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples)
{
	int i;
	InsertBatchAck *ack = NULL;
	InsertBatch *batch = NULL;
	Size size = 0;
	bool snap = ActiveSnapshotSet();
	Bitmapset *all_targets = GetLocalStreamReaders(RelationGetRelid(stream));
	Bitmapset *adhoc = GetAdhocContinuousViewIds();
	Bitmapset *targets = bms_difference(all_targets, adhoc);
	ipc_queue *ipcq = NULL;
	bytea *packed_desc;
	int nbatches = 1;

	if (snap)
		PopActiveSnapshot();

	packed_desc = PackTupleDesc(desc);

	if (!bms_is_empty(targets))
	{
		if (synchronous_stream_insert)
		{
			batch = InsertBatchCreate();
			ack = palloc0(sizeof(InsertBatchAck));
			ack->batch_id = batch->id;
			ack->batch = batch;
		}

		ipcq = get_worker_queue_with_lock();

		for (i=0; i<ntuples; i++)
		{
			StreamTupleState *sts;
			HeapTuple tup = tuples[i];
			int len;

			sts = StreamTupleStateCreate(tup, desc, packed_desc, targets, ack, synchronous_stream_insert ? 1 : 0, &len);

			if (!ipc_queue_push_nolock(ipcq, sts, len, false))
			{
				int ntries = 0;
				nbatches++;

				do
				{
					ntries++;
					ipc_queue_unlock(ipcq);
					ipcq = get_worker_queue_with_lock();
				}
				while (!ipc_queue_push_nolock(ipcq, sts, len, ntries == continuous_query_num_workers));
			}

			size += len;
		}

		ipc_queue_unlock(ipcq);
	}

	pfree(packed_desc);

	pgstat_increment_stream_insert(RelationGetRelid(stream), ntuples, nbatches, size);

	if (batch)
	{
		pfree(ack);
		InsertBatchWaitAndRemove(batch, ntuples);
	}

	if (snap)
		PushActiveSnapshot(GetTransactionSnapshot());

	bms_free(all_targets);
	bms_free(adhoc);
	bms_free(targets);
}

extern
Datum pipeline_stream_insert(PG_FUNCTION_ARGS)
{
	elog(ERROR, "pipeline_stream_insert can only be used by continuous transforms");
}
