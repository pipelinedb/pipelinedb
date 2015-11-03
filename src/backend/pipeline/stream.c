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
#include "pipeline/tuplebuf.h"
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

/* guc parameters */
bool synchronous_stream_insert;
char *stream_targets;

int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread) = NULL;
void *copy_iter_arg = NULL;

Bitmapset *
GetStreamReaders(Oid relid)
{
	Bitmapset *targets = GetLocalStreamReaders(relid);
	char *name = get_rel_name(relid);

	if (targets == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no continuous views are currently reading from stream %s", name),
				 errhint("Use CREATE CONTINUOUS VIEW to create a continuous view that includes %s in its FROM clause.", name)));

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
			MakeStreamTuple(tup, desc, 1, NULL);
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
	Bitmapset *all_adhoc = GetAdhocContinuousViewIds();
	Bitmapset *targets = bms_difference(all_targets, all_adhoc);
	Bitmapset *adhoc_targets = continuous_queries_adhoc_enabled ? 
		bms_difference(all_targets, targets) : NULL;
	dsm_cqueue *cq = NULL;
	bytea *packed_desc;
	AdhocData adhoc_data;

	init_adhoc_data(&adhoc_data, adhoc_targets);

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

		if (adhoc_data.num_adhoc)
		{
			Size abytes = 0;
			SendTupleToAdhoc(&adhoc_data, tup, desc, &abytes);
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
}
