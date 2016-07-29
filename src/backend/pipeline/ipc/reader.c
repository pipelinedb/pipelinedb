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
#include "utils/memutils.h"

typedef struct ipc_tuple_reader_scan
{
	ListCell *batch;
	ListCell *tup;

	bool batch_started;
	bool scan_started;
	bool exhausted;
} ipc_tuple_reader_scan;

static ipc_tuple_reader_scan my_rscan = { NULL, NULL, false };
static ipc_tuple_reader_batch my_rbatch = { NULL, 0, 0 };
static ipc_tuple my_rscan_tup;

typedef struct ipc_tuple_reader
{
	MemoryContext cxt;

	int num_tuples;
	Size num_bytes;

	Bitmapset *queries;
	List *batches;
} ipc_tuple_reader;

static ipc_tuple_reader *my_reader = NULL;

void
ipc_tuple_reader_init(uint64 id)
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

	pzmq_init();
	pzmq_bind(id);
}

void
ipc_tuple_reader_destroy(void)
{
	pzmq_destroy();

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
	List *acks = NIL;

	Assert(my_reader->batches == NIL);

	old = MemoryContextSwitchTo(my_reader->cxt);

	while (true)
	{
		int len;
		char *buf;
		microbatch_t *mb;
		int timeout;

		if (my_reader->num_tuples >= continuous_query_num_batch ||
				my_reader->num_bytes >= (continuous_query_batch_size * 1024))
			break;

		TimestampDifference(start, GetCurrentTimestamp(), &secs, &usecs);
		timeout = continuous_query_max_wait - (secs * 1000 + usecs / 1000);
		if (timeout <= 0)
			break;

		elog(LOG, "recv");

		buf = pzmq_recv(&len, timeout);
		if (!buf)
			continue;

		mb = microbatch_unpack(buf, len);
		ntups += mb->ntups;
		nbytes += len;

		my_reader->batches = lappend(my_reader->batches, mb);
		my_reader->queries = bms_union(my_reader->queries, mb->queries);
		acks = list_concat(acks, mb->acks);
	}

	elog(LOG, "done recv");

	MemoryContextSwitchTo(old);

	my_rbatch.ntups = ntups;
	my_rbatch.nbytes = nbytes;
	my_rbatch.queries = my_reader->queries;
	my_rbatch.acks = acks;

	elog(LOG, "pull msgs: %s,  %d", bms_print(my_reader->queries), ntups);

	return &my_rbatch;
}

void
ipc_tuple_reader_reset(void)
{
	MemoryContextReset(my_reader->cxt);
	my_reader->queries = NULL;
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
		ListCell *lc2;

		foreach(lc2, mb->acks)
		{
			microbatch_ack_t *ack = lfirst(lc);
			microbatch_ack_increment_acks(ack, mb->ntups);
		}
	}
}

ipc_tuple *
ipc_tuple_reader_next(Oid query_id)
{
	microbatch_t *mb;

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

	/* If this microbatch isn't for the desired query, skip it */
	if (!bms_is_member(query_id, mb->queries))
	{
		my_rscan.batch = lnext(my_rscan.batch);
		return ipc_tuple_reader_next(query_id);
	}

	/* Have we started reading this microbatch? */
	if (!my_rscan.batch_started)
	{
		Assert(my_rscan.tup == NULL);

		my_rscan.batch_started = true;
		my_rscan.tup = list_head(mb->tups);
		my_rscan_tup.desc = mb->desc;
	}
	else
		my_rscan.tup = lnext(my_rscan.tup);

	/* Have we exhausted this microbatch? */
	if (my_rscan.tup == NULL)
	{
		my_rscan.batch = lnext(my_rscan.batch);

		my_rscan.tup = NULL;
		my_rscan.batch_started = false;
		return ipc_tuple_reader_next(query_id);
	}

	my_rscan_tup.tup = lfirst(my_rscan.tup);
	return &my_rscan_tup;
}

void
ipc_tuple_reader_rewind(void)
{
	MemSet(&my_rscan, 0, sizeof(ipc_tuple_reader_scan));
}
