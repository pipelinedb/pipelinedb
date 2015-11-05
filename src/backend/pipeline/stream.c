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

Bitmapset *
GetStreamReaders(Oid relid)
{
	Bitmapset *targets;
	char *name = get_rel_name(relid);;

	if (!continuous_queries_enabled)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot insert into stream %s since continuous queries are disabled", name),
				 errhint("Enable continuous queries using the \"continuous_queries_enabled\" parameter.")));

	targets = GetLocalStreamReaders(relid);

	if (targets == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", name),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.", name)));

	return targets;
}

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
	Bitmapset *all_targets = GetStreamReaders(RelationGetRelid(stream));
	Bitmapset *adhoc = GetAdhocContinuousViewIds();
	Bitmapset *targets = bms_difference(all_targets, adhoc);
	dsm_cqueue *cq = NULL;
	bytea *packed_desc;

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

		cq = GetWorkerQueue();
	}

	for (i=0; i<ntuples; i++)
	{
		StreamTupleState *sts;
		HeapTuple tup = tuples[i];
		int len;

		sts = StreamTupleStateCreate(tup, desc, packed_desc, targets, ack, &len);

		if (cq)
		{
			dsm_cqueue_push_nolock(cq, sts, len);
			size += len;
		}
	}

	pfree(packed_desc);

	if (cq)
		dsm_cqueue_unlock(cq);

	stream_stat_report(RelationGetRelid(stream), ntuples, 1, size);

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
