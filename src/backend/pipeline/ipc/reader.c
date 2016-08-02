/*-------------------------------------------------------------------------
 *
 * reader.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include <zmq.h>

#include "postgres.h"

#include "pipeline/ipc/microbatch.h"
#include "pipeline/ipc/pzmq.h"
#include "pipeline/ipc/reader.h"
#include "pipeline/miscutils.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

typedef struct ipc_tuple_reader_scan
{
	ListCell *batch;
	int tup_idx;
	bool scan_started;
	bool exhausted;
} ipc_tuple_reader_scan;

static ipc_tuple_reader_scan my_rscan = { NULL, -1, false, false };
static ipc_tuple_reader_batch my_rbatch = { NULL, NULL, 0, 0 };
static ipc_tuple my_rscan_tup;

typedef struct ipc_tuple_reader
{
	MemoryContext cxt;
	List *batches;
} ipc_tuple_reader;

static ipc_tuple_reader *my_reader = NULL;

void
ipc_tuple_reader_init(void)
{
	MemoryContext cxt;
	MemoryContext old;
	ipc_tuple_reader *reader;

	old = MemoryContextSwitchTo(TopMemoryContext);

	cxt = AllocSetContextCreate(TopMemoryContext, "ipc_tuple_reader MemoryContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	reader = palloc0(sizeof(ipc_tuple_reader));
	reader->cxt = cxt;

	MemoryContextSwitchTo(old);

	my_reader = reader;
}

void
ipc_tuple_reader_destroy(void)
{
	ipc_tuple_reader_ack();
	ipc_tuple_reader_reset();

	MemoryContextDelete(my_reader->cxt);
	my_reader->cxt = NULL;
}

ipc_tuple_reader_batch *
ipc_tuple_reader_pull(void)
{
	TimestampTz start = GetCurrentTimestamp();
	MemoryContext old;
	long secs;
	int usecs;
	int ntups = 0;
	int nbytes = 0;
	Bitmapset *queries = NULL;

	Assert(my_reader->batches == NIL);

	old = MemoryContextSwitchTo(my_reader->cxt);

	while (true)
	{
		int len;
		char *buf;
		microbatch_t *mb;
		int timeout;

		if (ntups >= continuous_query_num_batch ||
				nbytes >= (continuous_query_batch_size * 1024))
			break;

		TimestampDifference(start, GetCurrentTimestamp(), &secs, &usecs);
		timeout = continuous_query_max_wait - (secs * 1000 + usecs / 1000);
		if (timeout <= 0)
			break;

		buf = pzmq_recv(&len, timeout);
		if (!buf)
			continue;

		mb = microbatch_unpack(buf, len);
		ntups += mb->ntups;
		nbytes += len;

		my_reader->batches = lappend(my_reader->batches, mb);

		queries = bms_union(queries, mb->queries);
	}

	MemoryContextSwitchTo(old);

	my_rbatch.ntups = ntups;
	my_rbatch.nbytes = nbytes;
	my_rbatch.queries = queries;
	my_rbatch.acks = NIL;

	return &my_rbatch;
}

void
ipc_tuple_reader_reset(void)
{
	MemoryContextReset(my_reader->cxt);
	my_reader->batches = NIL;
	ipc_tuple_reader_rewind();
}

void
ipc_tuple_reader_ack(void)
{
	ListCell *lc;

	foreach(lc, my_reader->batches)
	{
		microbatch_t *mb = lfirst(lc);
		microbatch_acks_check_and_exec(mb->acks, microbatch_ack_increment_acks, mb->ntups);
	}
}

static inline ipc_tuple *
read_from_next_batch(Oid query_id)
{
	my_rscan.batch = lnext(my_rscan.batch);
	my_rscan.tup_idx = -1;
	return ipc_tuple_reader_next(query_id);
}

ipc_tuple *
ipc_tuple_reader_next(Oid query_id)
{
	microbatch_t *mb;
	tagged_ref_t *ref;

	if (my_rscan.exhausted)
		return NULL;

	/* Are we started a new reader scan? */
	if (!my_rscan.scan_started)
	{
		my_rscan.scan_started = true;
		my_rscan.batch = list_head(my_reader->batches);
	}

	/* Have we read all microbatches? */
	if (my_rscan.batch == NULL)
	{
		my_rscan.exhausted = true;
		return NULL;
	}

	mb = lfirst(my_rscan.batch);
	Assert(mb->allow_iter);
	Assert(mb->ntups >= my_rscan.tup_idx);

	/* If this microbatch isn't for the desired query, skip it */
	if (!bms_is_member(query_id, mb->queries) || !mb->ntups)
		return read_from_next_batch(query_id);

	/* Have we started reading this microbatch? */
	if (my_rscan.tup_idx == -1)
	{
		my_rscan.tup_idx = 0;
		my_rscan_tup.desc = mb->desc;

		if (mb->acks)
		{
			ListCell *lc;
			MemoryContext old;

			/*
			 * Instead of using the ContQueryBatchContext we use the CacheMemoryContext
			 * since these acks could be used by the combiner across batches and transactions
			 * when in async mode.
			 */
			old = MemoryContextSwitchTo(ContQueryTransactionContext);

			foreach(lc, mb->acks)
			{
				tagged_ref_t *ref = palloc(sizeof(tagged_ref_t));
				*ref = *(tagged_ref_t *) lfirst(lc);

				my_rbatch.acks = lappend(my_rbatch.acks, ref);
			}

			MemoryContextSwitchTo(old);
		}
	}
	else
	{
		my_rscan.tup_idx++;
		if (my_rscan.tup_idx == mb->ntups)
			return read_from_next_batch(query_id);
	}

	Assert(my_rscan.tup_idx < mb->ntups);
	ref = &mb->tups[my_rscan.tup_idx];

	my_rscan_tup.tup = (HeapTuple) ref->ptr;
	my_rscan_tup.hash = ref->tag;

	return &my_rscan_tup;
}

void
ipc_tuple_reader_rewind(void)
{
	MemSet(&my_rscan, 0, sizeof(ipc_tuple_reader_scan));
	my_rscan.tup_idx = -1;
	my_rbatch.acks = NIL;
}
