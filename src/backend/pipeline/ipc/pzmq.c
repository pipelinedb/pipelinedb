/*-------------------------------------------------------------------------
 *
 * zmq.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include <zmq.h>
#include "postgres.h"

#include "pipeline/cont_scheduler.h"
#include "pipeline/ipc/pzmq.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#define ERRNO_IS_SAFE() (errno == EINTR || errno == EAGAIN)

typedef struct pzmq_socket_t
{
	uint64 id;
	char type;
	char addr[64];
	void *sock;
} pzmq_socket_t;

typedef struct pzmq_state_t
{
	void *zmq_cxt;
	MemoryContext mem_cxt;
	pzmq_socket_t *me;
	HTAB *dests;
	char *buf;
} pzmq_state_t;

static pzmq_state_t *zmq_state = NULL;

void
pzmq_init(void)
{
	MemoryContext old;
	pzmq_state_t *zs;
	HASHCTL ctl;

	if (zmq_state != NULL)
	{
		elog(WARNING, "pzmq is already initialized");
		return;
	}

	old = MemoryContextSwitchTo(TopMemoryContext);

	zs = palloc0(sizeof(pzmq_state_t));
	zs->mem_cxt = AllocSetContextCreate(TopMemoryContext, "pzmq MemoryContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);
	zs->buf = palloc0(sizeof(continuous_query_batch_size));

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(pzmq_socket_t);
	ctl.hcxt = zs->mem_cxt;
	ctl.hash = tag_hash;
	zs->dests = hash_create("pzmq dests HTAB", continuous_query_num_workers + continuous_query_num_combiners + 8,
			&ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	zs->zmq_cxt = zmq_ctx_new();

	MemoryContextSwitchTo(old);

	zmq_state = zs;
}

void
pzmq_destroy(void)
{
	HASH_SEQ_STATUS iter;
	pzmq_socket_t *zsock;

	if (zmq_state == NULL)
		elog(ERROR, "pzmq is not initialized");


	hash_seq_init(&iter, zmq_state->dests);
	while ((zsock = (pzmq_socket_t *) hash_seq_search(&iter)) != NULL)
	{
		Assert(zsock->type == ZMQ_PUSH);
		zmq_disconnect(zsock->sock, zsock->addr);
		zmq_close(zsock->sock);
	}

	if (zmq_state->me)
		zmq_close(zsock->sock);

	zmq_ctx_shutdown(zmq_state->zmq_cxt);
	zmq_ctx_term(zmq_state->zmq_cxt);

	MemoryContextDelete(zmq_state->mem_cxt);
	pfree(zmq_state);

	zmq_state = NULL;
}

void
pzmq_bind(uint64 id)
{
	MemoryContext old;
	pzmq_socket_t *zsock;

	if (zmq_state == NULL)
		elog(ERROR, "pzmq is not initialized");

	if (zmq_state->me != NULL)
		elog(ERROR, "pzmq is already binded");

	old = MemoryContextSwitchTo(zmq_state->mem_cxt);

	zsock = palloc0(sizeof(pzmq_socket_t));
	zsock->id = id;
	zsock->type = ZMQ_PULL;
	sprintf(zsock->addr, "ipc:///tmp/%ld", id);
	zsock->sock = zmq_socket(zmq_state->zmq_cxt, zsock->type);

	if (zmq_bind(zsock->sock, zsock->addr) != 0)
	{
		zmq_close(zsock->sock);
		pfree(zsock);
		elog(ERROR, "pzmq_bind failed: %m");
	}

	MemoryContextSwitchTo(old);

	zmq_state->me = zsock;
}

void
pzmq_connect(uint64 id, int hwm)
{
	pzmq_socket_t *zsock;
	bool found;

	if (zmq_state == NULL)
		elog(ERROR, "pzmq is not initialized");

	zsock = (pzmq_socket_t *) hash_search(zmq_state->dests, &id, HASH_ENTER, &found);

	if (!found)
	{
		MemSet(zsock, 0, sizeof(pzmq_socket_t));

		zsock->id = id;
		zsock->type = ZMQ_PUSH;
		sprintf(zsock->addr, "ipc:///tmp/%ld", id);
		zsock->sock = zmq_socket(zmq_state->zmq_cxt, zsock->type);

		if (zmq_connect(zsock->sock, zsock->addr) != 0)
		{
			zmq_close(zsock->sock);
			hash_search(zmq_state->dests, &id, HASH_REMOVE, &found);
			elog(ERROR, "pzmq_connect failed: %m");
		}

		zmq_setsockopt(zsock->sock, ZMQ_SNDHWM, &hwm, sizeof(int));
	}
}

char *
pzmq_recv(int *len, bool wait)
{
	int ret;

	if (zmq_state == NULL)
		elog(ERROR, "pzmq is not initialized");

	if (zmq_state->me != NULL)
		elog(ERROR, "pzmq is not binded");

	ret = zmq_recv(zmq_state->me->sock, zmq_state->buf, continuous_query_batch_size, wait ? 0 : ZMQ_DONTWAIT);

	if (ret == -1)
	{
		if (!ERRNO_IS_SAFE())
			elog(ERROR, "pzmq failed to recv msg: %m");

		*len = 0;
		return NULL;
	}

	Assert(ret <= continuous_query_batch_size);
	*len = ret;
	return zmq_state->buf;
}

bool
pzmq_send(uint64 id, char *msg, int len, bool wait)
{
	pzmq_socket_t *zsock;
	bool found;
	int ret;

	Assert(len <= continuous_query_batch_size);

	if (zmq_state == NULL)
		elog(ERROR, "pzmq is not initialized");

	zsock = (pzmq_socket_t *) hash_search(zmq_state->dests, &id, HASH_ENTER, &found);

	if (!found)
		elog(ERROR, "pzmq is not connected to %ld", id);

	ret = zmq_send(zsock->sock, msg, len, wait ? 0 : ZMQ_DONTWAIT);

	if (ret == -1)
	{
		if (!ERRNO_IS_SAFE())
			elog(ERROR, "pzmq failed to send msg: %m");

		return false;
	}

	Assert(ret == len);
	return true;
}
