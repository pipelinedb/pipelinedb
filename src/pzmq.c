/*-------------------------------------------------------------------------
 *
 * zmq.c
 *
  * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <dirent.h>
#include <stdio.h>
#include <zmq.h>
#include "postgres.h"

#include "miscadmin.h"
#include "pzmq.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#define ERRNO_IS_SAFE() (errno == EINTR || errno == EAGAIN || !errno)

typedef struct pzmq_socket_t
{
	uint64 id;
	char type;
	void *sock;
	char addr[MAXPGPATH];
} pzmq_socket_t;

typedef struct pzmq_state_t
{
	void *zmq_cxt;
	MemoryContext mem_cxt;
	pzmq_socket_t *me;
	HTAB *dests;
	int max_msg_size;
	int hwm;
	bool enqueue;
} pzmq_state_t;

char *socket_dir = NULL;

static pzmq_state_t *zmq_state = NULL;

void
pzmq_init(int max_msg_size, int hwm, int num_destinations, bool enqueue)
{
	MemoryContext old;
	MemoryContext cxt;
	pzmq_state_t *zs;
	HASHCTL ctl;

	if (zmq_state)
		return;

	if (socket_dir == NULL)
		socket_dir = DataDir;

	cxt = AllocSetContextCreate(TopMemoryContext, "pzmq MemoryContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	old = MemoryContextSwitchTo(cxt);

	zs = palloc0(sizeof(pzmq_state_t));
	zs->mem_cxt = cxt;

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(pzmq_socket_t);
	ctl.hcxt = zs->mem_cxt;
	ctl.hash = tag_hash;
	zs->dests = hash_create("pzmq dests HTAB", num_destinations + 8, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	zs->zmq_cxt = zmq_ctx_new();
	zs->max_msg_size = max_msg_size;
	zs->hwm = hwm;
	zs->enqueue = enqueue;

	MemoryContextSwitchTo(old);

	zmq_state = zs;
}

void
pzmq_destroy(void)
{
	HASH_SEQ_STATUS iter;
	pzmq_socket_t *zsock;

	if (!zmq_state)
		return;

	hash_seq_init(&iter, zmq_state->dests);
	while ((zsock = (pzmq_socket_t *) hash_seq_search(&iter)) != NULL)
	{
		Assert(zsock->type == ZMQ_PUSH);
		zmq_disconnect(zsock->sock, zsock->addr);
		zmq_close(zsock->sock);
	}

	if (zmq_state->me)
	{
		zmq_close(zmq_state->me->sock);
		remove(&zmq_state->me->addr[6]);
	}

	zmq_ctx_shutdown(zmq_state->zmq_cxt);
	zmq_ctx_term(zmq_state->zmq_cxt);

	MemoryContextDelete(zmq_state->mem_cxt);

	zmq_state = NULL;
}

void
pzmq_bind(uint64 id)
{
	MemoryContext old;
	pzmq_socket_t *zsock;
	int optval;

	if (!zmq_state)
		elog(ERROR, "pzmq is not initialized");

	if (zmq_state->me)
		elog(ERROR, "pzmq is already binded");

	old = MemoryContextSwitchTo(zmq_state->mem_cxt);

	zsock = palloc0(sizeof(pzmq_socket_t));
	zsock->id = id;
	zsock->type = ZMQ_PULL;
	sprintf(zsock->addr, PZMQ_SOCKNAME_STR, socket_dir, id);
	zsock->sock = zmq_socket(zmq_state->zmq_cxt, ZMQ_PULL);

	optval = zmq_state->hwm;
	if (zmq_setsockopt(zsock->sock, ZMQ_RCVHWM, &optval, sizeof(int)) != 0)
		elog(WARNING, "pzmq_bind failed to set rcvhwm: %s", zmq_strerror(errno));

	if (zmq_bind(zsock->sock, zsock->addr) != 0)
	{
		zmq_close(zsock->sock);
		pfree(zsock);
		elog(ERROR, "pzmq_bind failed: %s", zmq_strerror(errno));
	}

	MemoryContextSwitchTo(old);

	zmq_state->me = zsock;
}

void
pzmq_connect(uint64 id)
{
	pzmq_socket_t *zsock;
	bool found;

	if (!zmq_state)
		elog(ERROR, "pzmq is not initialized");

	zsock = (pzmq_socket_t *) hash_search(zmq_state->dests, &id, HASH_ENTER, &found);

	if (!found)
	{
		int optval;

		MemSet(zsock, 0, sizeof(pzmq_socket_t));

		zsock->id = id;
		zsock->type = ZMQ_PUSH;
		sprintf(zsock->addr, PZMQ_SOCKNAME_STR, socket_dir, id);
		zsock->sock = zmq_socket(zmq_state->zmq_cxt, ZMQ_PUSH);

		optval = zmq_state->hwm;
		if (zmq_setsockopt(zsock->sock, ZMQ_SNDHWM, &optval, sizeof(int)) != 0)
			elog(WARNING, "pzmq_connect failed to set sndhwm: %s", zmq_strerror(errno));

		if (zmq_state->enqueue)
		{
			/* Enqueue messages immediately, even if the connection isn't ready yet */
			optval = 1;
			if (zmq_setsockopt(zsock->sock, ZMQ_IMMEDIATE, &optval, sizeof(int)) != 0)
				elog(WARNING, "pzmq_connect failed to set immediate: %s", zmq_strerror(errno));

			/* Leave messages in memory until they're consumed */
			optval = -1;
			if (zmq_setsockopt(zsock->sock, ZMQ_LINGER, &optval, sizeof(int)) != 0)
				elog(WARNING, "pzmq_connect failed to set linger: %s", zmq_strerror(errno));
		}
		else
		{
			/* We're a CQ process, don't attempt to send messages if the connection isn't ready yet */
			optval = 0;
			if (zmq_setsockopt(zsock->sock, ZMQ_LINGER, &optval, sizeof(int)) != 0)
				elog(WARNING, "pzmq_connect failed to set linger: %s", zmq_strerror(errno));
		}

		if (zmq_connect(zsock->sock, zsock->addr) != 0)
		{
			zmq_close(zsock->sock);
			hash_search(zmq_state->dests, &id, HASH_REMOVE, &found);
			elog(ERROR, "pzmq_connect failed: %s", zmq_strerror(errno));
		}
	}
}

bool
pzmq_poll(int timeout)
{
	zmq_pollitem_t item;
	int rc;

	if (!zmq_state)
		elog(ERROR, "pzmq is not initialized");

	if (!zmq_state->me)
		elog(ERROR, "pzmq is not binded");

	item.events = ZMQ_POLLIN;
	item.revents = 0;
	item.socket = zmq_state->me->sock;
	item.fd = 0;

	if (!timeout)
		timeout = -1;

	rc = zmq_poll(&item, 1, timeout);

	if (rc == -1 && !ERRNO_IS_SAFE())
		elog(ERROR, "pzmq failed to pollin: %s", zmq_strerror(errno));

	return item.revents & ZMQ_POLLIN;
}

char *
pzmq_recv(int *len, int timeout)
{
	int ret;
	char *buf;
	zmq_msg_t msg;

	if (!zmq_state)
		elog(ERROR, "pzmq is not initialized");

	if (!zmq_state->me)
		elog(ERROR, "pzmq is not binded");

	if (timeout && !pzmq_poll(timeout))
	{
		*len = 0;
		return NULL;
	}

	zmq_msg_init(&msg);

	ret = zmq_msg_recv(&msg, zmq_state->me->sock, ZMQ_DONTWAIT);
	if (ret == -1)
	{
		if (!ERRNO_IS_SAFE())
			elog(ERROR, "pzmq failed to recv msg: %s %d", zmq_strerror(errno), errno);

		*len = 0;
		return NULL;
	}

	Assert(ret <= zmq_state->max_msg_size);
	Assert(ret == zmq_msg_size(&msg));

	*len = ret;
	buf = palloc(ret);
	memcpy(buf, zmq_msg_data(&msg), ret);
	zmq_msg_close(&msg);

	return buf;
}

bool
pzmq_send(uint64 id, char *msg, int len, bool wait)
{
	pzmq_socket_t *zsock;
	bool found;
	int ret;

	if (!zmq_state)
		elog(ERROR, "pzmq is not initialized");

	Assert(len <= zmq_state->max_msg_size);
	zsock = (pzmq_socket_t *) hash_search(zmq_state->dests, &id, HASH_ENTER, &found);

	if (!found)
		elog(ERROR, "pzmq is not connected to %ld", id);

	ret = zmq_send(zsock->sock, msg, len, wait ? 0 : ZMQ_DONTWAIT);

	if (ret == -1)
	{
		if (!ERRNO_IS_SAFE())
			elog(ERROR, "pzmq failed to send msg: %s", zmq_strerror(errno));

		return false;
	}

	Assert(ret == len);
	return true;
}

void
pzmq_purge_sock_files(void)
{
	char dpath[MAXPGPATH];
	char fpath[MAXPGPATH];
	DIR *dir;
	struct dirent *file;

	sprintf(dpath, "%s/pipeline/zmq", socket_dir);
	dir = opendir(dpath);

	/* It may not exist initially */
	if (dir == NULL)
		return;

	while ((file = readdir(dir)) != NULL)
	{
		sprintf(fpath, "%s/%s", dpath, file->d_name);
		remove(fpath);
	}

	closedir(dir);
}
