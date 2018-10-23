/*-------------------------------------------------------------------------
 *
 * reader.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <zmq.h>

#include "postgres.h"

#include "executor.h"
#include "microbatch.h"
#include "miscutils.h"
#include "pzmq.h"
#include "reader.h"
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
static ipc_tuple_reader_batch my_rbatch = { NULL, false, NULL, 0, 0 };
static ipc_tuple my_rscan_tup;

typedef struct ipc_tuple_reader
{
	MemoryContext cxt;
	List *batches;
	List *flush_acks;
} ipc_tuple_reader;

static ipc_tuple_reader *my_reader = NULL;

/*
 * ipc_tuple_reader_init
 */
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

/*
 * ipc_tuple_reader_destroy
 */
void
ipc_tuple_reader_destroy(void)
{
	ipc_tuple_reader_ack();
	ipc_tuple_reader_reset();

	MemoryContextDelete(my_reader->cxt);
	my_reader->cxt = NULL;
}

/*
 * ipc_tuple_reader_pull
 */
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
	List *flush_acks = NIL;

	Assert(my_reader->batches == NIL);

	old = MemoryContextSwitchTo(my_reader->cxt);

	my_rbatch.has_acks = false;

	while (true)
	{
		int len;
		char *buf;
		microbatch_t *mb;
		int timeout;

		if (ntups >= continuous_query_batch_size || nbytes >= MAX_MICROBATCH_SIZE)
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

		/*
		 * If this is a FlushTuple microbatch, don't add it to the list of microbatches to
		 * process, just accumulate the ack so we can broadcast it later on.
		 */
		if (mb->type == FlushTuple)
		{
			tagged_ref_t *ref;
#ifdef USE_ASSERT_CHECKING
			microbatch_ack_t *ack;
#endif

			Assert(list_length(mb->acks) == 1);

			ref = linitial(mb->acks);
			if (!microbatch_ack_ref_is_valid(ref))
				continue;
#ifdef USE_ASSERT_CHECKING
			ack = (microbatch_ack_t *) ref->ptr;
			Assert(microbatch_ack_get_level(ack) == STREAM_INSERT_FLUSH);
#endif

			flush_acks = lappend(flush_acks, ref);
			my_rbatch.has_acks = true;

			continue;
		}

		my_reader->batches = lappend(my_reader->batches, mb);

		queries = bms_union(queries, mb->queries);

		/*
		 * Hot path for STREAM_INSERT_SYNCHRONOUS_RECEIVE inserts. If we're receiving a microbatch from the insert process
		 * (we are a worker and microbatch has a single ack of level STREAM_INSERT_SYNCHRONOUS_RECEIVE), mark it as read
		 * and don't pass the ack downstream.
		 */
		if (list_length(mb->acks) == 1 && IsContQueryWorkerProcess())
		{
			tagged_ref_t *ref = linitial(mb->acks);
			microbatch_ack_t *ack = (microbatch_ack_t *) ref->ptr;

			if (microbatch_ack_ref_is_valid(ref) && microbatch_ack_get_level(ack) == STREAM_INSERT_SYNCHRONOUS_RECEIVE)
			{
				microbatch_ack_increment_wrecv(ack, mb->ntups);

				/*
				 * Set the acks for this microbatch to be NIL so that we don't pass them around to
				 * downstream processes.
				 */
				mb->acks = NIL;
			}
		}

		my_rbatch.has_acks |= list_length(mb->acks) > 0;
	}

	MemoryContextSwitchTo(old);

	my_rbatch.ntups = ntups;
	my_rbatch.nbytes = nbytes;
	my_rbatch.queries = queries;
	my_rbatch.sync_acks = NIL;
	my_rbatch.flush_acks = flush_acks;

	my_reader->flush_acks = flush_acks;

	return &my_rbatch;
}

/*
 * ipc_tuple_reader_reset
 */
void
ipc_tuple_reader_reset(void)
{
	MemoryContextReset(my_reader->cxt);
	my_reader->batches = NIL;
	my_reader->flush_acks = NIL;
	ipc_tuple_reader_rewind();
}

/*
 * ipc_tuple_reader_ack
 */
void
ipc_tuple_reader_ack(void)
{
	ListCell *lc;

	foreach(lc, my_reader->batches)
	{
		microbatch_t *mb = lfirst(lc);
		microbatch_acks_check_and_exec(mb->acks, microbatch_ack_increment_acks, mb->ntups);
	}

	microbatch_acks_check_and_exec(my_reader->flush_acks, microbatch_ack_increment_acks, 1);
}

/*
 * read_from_next_batch
 */
static inline ipc_tuple *
read_from_next_batch(Oid query_id)
{
	my_rscan.batch = lnext(my_rscan.batch);
	my_rscan.tup_idx = -1;
	return ipc_tuple_reader_next(query_id);
}

/*
 * ipc_tuple_reader_next
 */
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

				my_rbatch.sync_acks = lappend(my_rbatch.sync_acks, ref);
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

/*
 * ipc_tuple_reader_rewind
 */
void
ipc_tuple_reader_rewind(void)
{
	MemSet(&my_rscan, 0, sizeof(ipc_tuple_reader_scan));
	my_rscan.tup_idx = -1;
	my_rbatch.sync_acks = NIL;
}
