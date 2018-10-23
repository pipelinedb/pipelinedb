/*-------------------------------------------------------------------------
 *
 * queue.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "microbatch.h"
#include "pzmq.h"
#include "miscutils.h"
#include "scheduler.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

typedef struct pending_microbatch_t
{
	char *batch;
	char *buf;
	uint64 recv_id;
	int len;
} pending_microbatch_t;

static HTAB *pending = NULL;

#define MAX_QUEUE_FLUSH_ATTEMPTS 10
#define QUEUE_RECV_TIMEOUT 2 * 1000 /* 2s */
#define PENDING_MICROBATCH_SIZE(mb) (mb->len + sizeof(pending_microbatch_t))

/*
 * peek_microbatch
 */
static char *
peek_microbatch(char *buf, uint64 *recv_id, int *len)
{
	char *result;

	/* The message header is the destination recv_id */
	Assert(recv_id);
	memcpy(recv_id, buf, sizeof(uint64));

	result = buf + sizeof(uint64);
	*len -= sizeof(uint64);

	return result;
}

/*
 * send_microbatch
 */
static bool
send_microbatch(uint64_t recv_id, char *batch, int len)
{
	pzmq_connect(recv_id);
	return pzmq_send(recv_id, batch, len, false);
}

/*
 * retry_pending
 */
static int
retry_pending(Size *memory_freed)
{
	Size freed = 0;
	HASH_SEQ_STATUS iter;
	pending_microbatch_t *entry;
	List *to_delete = NIL;
	ListCell *lc;
	int count = 0;

	Assert(pending);

	if (!hash_get_num_entries(pending))
		return 0;

	hash_seq_init(&iter, pending);

	while ((entry = (pending_microbatch_t *) hash_seq_search(&iter)) != NULL)
	{
		if (send_microbatch(entry->recv_id, entry->batch, entry->len))
			to_delete = lappend(to_delete, entry);
		count++;
	}

	foreach(lc, to_delete)
	{
		pending_microbatch_t *entry = (pending_microbatch_t *) lfirst(lc);
		bool found;

		pfree(entry->buf);
		hash_search(pending, &entry->batch, HASH_REMOVE, &found);
		Assert(found);

		freed += PENDING_MICROBATCH_SIZE(entry);
		count--;
	}

	list_free(to_delete);
	*memory_freed -= freed;

	return count;
}

/*
 * ContinuousQueryQueueMain
 */
void
ContinuousQueryQueueMain(void)
{
	Size memory_consumed = 0;
	HASHCTL ctl;
	MemoryContext old;
	MemoryContext cxt = AllocSetContextCreate(TopMemoryContext, "ContinuousQueryQueueContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(cxt);

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(char *);
	ctl.entrysize = sizeof(pending_microbatch_t);
	ctl.hcxt = cxt;
	ctl.hash = tag_hash;
	pending = hash_create("PendingHash", 8092, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	MemoryContextSwitchTo(old);

	for (;;)
	{
		int len;
		char *buf;
		char *batch;
		int timeout = 0;
		int remaining;
		int flush_attempts = 0;
		uint64 recv_id;

		CHECK_FOR_INTERRUPTS();

		if (get_sigterm_flag())
			break;

		remaining = retry_pending(&memory_consumed);

		/*
		 * If we're above memory capacity, aggressively try to send all batches until
		 * we're under capacity, but don't try forever
		 */
		while (memory_consumed >= continuous_query_queue_mem * 1024 &&
				flush_attempts < MAX_QUEUE_FLUSH_ATTEMPTS)
		{
			remaining = retry_pending(&memory_consumed);
			flush_attempts++;
		}

		timeout = remaining ? 0 : QUEUE_RECV_TIMEOUT;
		buf = pzmq_recv(&len, timeout);

		if (!buf)
			continue;

		batch = peek_microbatch(buf, &recv_id, &len);
		if (send_microbatch(recv_id, batch, len))
		{
			/*
			 * Nonblocking send was successful, we're done with this microbatch
			 */
			pfree(buf);
		}
		else
		{
			/*
			 * Nonblocking send was not successful, enqueue the batch for a later attempt
			 */
			bool found;
			pending_microbatch_t *entry = (pending_microbatch_t *) hash_search(pending, &batch, HASH_ENTER, &found);
			entry->batch = batch;
			entry->buf = buf;
			entry->recv_id = recv_id;
			entry->len = len;
			memory_consumed += PENDING_MICROBATCH_SIZE(entry);
		}
	}
}
