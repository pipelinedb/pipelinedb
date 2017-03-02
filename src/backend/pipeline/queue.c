/*-------------------------------------------------------------------------
 *
 * queue.c
 *
 * Copyright (c) 2017, PipelineDB
 *
 * IDENTIFICATION
 *    src/include/pipeline/queue.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xact.h"
#include "miscadmin.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/miscutils.h"
#include "pipeline/scheduler.h"
#include "utils/memutils.h"

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

typedef struct pending_microbatch_t
{
	char *batch;
	uint64 recv_id;
	int len;
} pending_microbatch_t;

static HTAB *pending = NULL;

#define PENDING_MICROBATCH_SIZE(mb) (mb->len + sizeof(pending_microbatch_t))

static bool
send_microbatch(uint64_t recv_id, char *batch, int len)
{
	pzmq_connect(recv_id);
	return pzmq_send(recv_id, batch, len, false);
}

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
		hash_search(pending, &entry->batch, HASH_REMOVE, &found);
		Assert(found);

		freed += PENDING_MICROBATCH_SIZE(entry);
		count--;
	}

	list_free(to_delete);
	*memory_freed -= freed;

	return count;
}

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
		uint64 recv_id;

		CHECK_FOR_INTERRUPTS();
		if (get_sigterm_flag())
			break;

		remaining = retry_pending(&memory_consumed);

		buf = pzmq_recv(&len, 100);
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
			entry->recv_id = recv_id;
			entry->len = len;
			memory_consumed += PENDING_MICROBATCH_SIZE(entry);
		}
	}
}
