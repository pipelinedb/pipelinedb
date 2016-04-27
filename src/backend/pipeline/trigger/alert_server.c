/*-------------------------------------------------------------------------
 *
 * alert_server.c
 *	  Functionality for alert server
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include <postgres.h>

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <ctype.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <stddef.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include <fcntl.h>
#include <errno.h>

#include "access/xact.h"
#include "commands/dbcommands.h"
#include "lib/stringinfo.h"
#include "lib/ilist.h"
#include "pipeline/trigger/tuple_formatter.h"
#include "pipeline/trigger/trigger.h"
#include "pipeline/trigger/alert_server.h"

#include "utils/memutils.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/trigger/mirror_ringbuf.h"
#include "pipeline/trigger/triggerfuncs.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"

#ifndef SOL_TCP
#define SOL_TCP IPPROTO_TCP
#endif

#define TS_MSG_SUBSCRIBE "subscribe"
#define TS_MSG_SUBSCRIBE_OK "subscribe_ok"
#define TS_MSG_SUBSCRIBE_FAIL "subscribe_fail"
#define TS_MSG_UNSUBSCRIBE "unsubscribe"
#define TS_MSG_UNSUBSCRIBE_OK "unsubscribe_ok"
#define TS_MSG_UNSUBSCRIBE_FAIL "unsubscribe_fail"
#define TS_MSG_ALERT "alert"
#define TS_MSG_DROPPED "dropped"
#define TS_MSG_HEARTBEAT "heartbeat"
#define TS_MSG_HEARTBEAT_ACK "heartbeat_ack"

#define TS_HEARTBEAT_TIMEOUT 5
#define TS_READ_TIMEOUT 10

typedef enum
{
	Acceptor,
	Client
} StreamType;

typedef struct ListenSocket
{
	int fd;
} ListenSocket;

typedef struct ClientSocket
{
	dlist_node list_node;

	int fd;
	StringInfo recv_buf;
	StringInfo frame_buf;

	StringInfo scratch;
	mirror_ringbuf *ringbuf;

	/* TODO: support list of subscriptions */
	void *subscription;
	double last_read_time;

} ClientSocket;

typedef void (*HandleFunc)(ClientSocket *cli,
						   StringInfo frame,
						   void *data);

static void client_socket_write(ClientSocket *cli, const char *str);
static void client_socket_write_msg(ClientSocket *cli, StringInfo msg);
static void client_socket_write_bytes(ClientSocket *cli, const char *bytes, int n);

/*
 * get_time
 *
 * Return a double representing seconds since epoch
 */
static double
get_time()
{
	struct timeval tv;
	gettimeofday(&tv, 0);

	return tv.tv_sec + (tv.tv_usec / 1000000.0);
}

/*
 * chomp
 *
 * In-place remove any \r or \n from the end of a StringInfo
 */
static void
chomp(StringInfo info)
{
	int i = 0;
	int len = info->len;

	for (i = 0; i < len; ++i)
	{
		char *c = info->data + (len - i - 1);

		if (*c == '\r' || *c == '\n')
		{
			*c = '\0';
			info->len--;
		}
	}
}

/*
 * create_listen_socket
 */
static ListenSocket *
create_listen_socket()
{
	ContQueryDatabaseMetadata *meta;
	int rc;
	int opt;
	int i;
	struct sockaddr_in tcp_addr;
	int aport;

	static const int backlog = 100;
	ListenSocket *sock = palloc0(sizeof(ListenSocket));

	sock->fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	Assert(sock->fd);

	opt = 1;
	rc = setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	if (rc != 0)
		elog(ERROR, "could not setsockopt %m");

	tcp_addr.sin_family = AF_INET;
	tcp_addr.sin_port = htons(alert_server_port);
	tcp_addr.sin_addr.s_addr = INADDR_ANY;

	/* try and allocate a port from the range base ->
	 * base + max_worker_processes */

	for (i = 0; i < max_worker_processes; ++i)
	{
		aport = alert_server_port + i;
		tcp_addr.sin_port = htons(aport);
		rc = bind(sock->fd, (struct sockaddr *) &tcp_addr, sizeof(tcp_addr));

		if (rc == 0)
		{
			MemoryContext old = CurrentMemoryContext;

			StartTransactionCommand();
			elog(LOG, "continuous query process \"alert server [%s]\" listening on port %d",
					get_database_name(MyDatabaseId), aport);
			CommitTransactionCommand();

			MemoryContextSwitchTo(old);
			break;
		}

		pg_usleep(100000);
	}

	if (rc != 0)
		elog(ERROR, "failed to assign alert server port %d %m", aport);

	rc = listen(sock->fd, backlog);

	if (rc != 0)
		elog(ERROR, "failed to listen on alert server port %d %m", aport);

	meta = GetContQueryDatabaseMetadata(MyDatabaseId);
	meta->alert_server_port = aport;

	return sock;
}

/*
 * destroy_listen_socket
 */
static void
destroy_listen_socket(ListenSocket *sock)
{
	close(sock->fd);
	pfree(sock);
}

/*
 * make_ringbuf
 *
 * Make a mirrored ring buffer of optimal size.
 */
static mirror_ringbuf *
make_ringbuf(int min_kb)
{
	int size = mirror_ringbuf_calc_mem(min_kb * 1024);
	void *mem = palloc0(size);

	Assert(mem != NULL);
	return mirror_ringbuf_init(mem, size);
}

/*
 * create_client_socket
 *
 * Setup client socket and resources.
 * Socket is nonblocking, and has TCP_NODELAY turned on.
 */
static ClientSocket *
create_client_socket(int fd)
{
	ClientSocket *cli;
	int on = 1;
	int rc = 0;
	int flags = 0;

	flags = fcntl(fd, F_GETFL);
	Assert(flags != -1);

	rc = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
	Assert(rc != -1);

	rc = setsockopt(fd, SOL_TCP, TCP_NODELAY, &on, sizeof (on));
	Assert(rc == 0);

	cli = palloc0(sizeof(ClientSocket));
	cli->fd = fd;
	cli->recv_buf = makeStringInfo();
	cli->frame_buf = makeStringInfo();
	cli->scratch = makeStringInfo();
	cli->ringbuf = make_ringbuf(alert_socket_mem);
	cli->last_read_time = get_time();

	return cli;
}

/*
 * client_disconnect
 *
 * Disconnect the client socket and log the reason
 */
static void
client_disconnect(ClientSocket *cli, const char *reason)
{
	if (cli->fd != -1)
	{
		elog(LOG, "alert server disconnecting client: %s", reason);
		close(cli->fd);
		cli->fd = -1;
	}
}

/*
 * destroy_client
 *
 * Clean up client resources and free
 */
static void
destroy_client(ClientSocket *cli)
{
	client_disconnect(cli, "teardown");

	pfree(cli->recv_buf->data);
	pfree(cli->recv_buf);

	pfree(cli->frame_buf->data);
	pfree(cli->frame_buf);

	pfree(cli->scratch->data);
	pfree(cli->scratch);
	pfree(cli->ringbuf);

	pfree(cli);
}

/*
 * client_socket_try_send
 *
 * Try and send any available data from the ringbuf.
 */
static void
client_socket_try_send(ClientSocket *cli)
{
	int num_written = send(cli->fd, mirror_ringbuf_peek(cli->ringbuf),
			mirror_ringbuf_avail_read(cli->ringbuf), 0);

	int errcode = errno;

	if (num_written == -1)
	{
		if (errcode != EAGAIN)
			client_disconnect(cli, "send error %m");

		return;
	}

	mirror_ringbuf_consume(cli->ringbuf, num_written);
}

/*
 * client_socket_write_bytes
 *
 * Writes data to the ringbuf, does not send.
 */
static void
client_socket_write_bytes(ClientSocket *cli, const char *bytes, int n)
{
	if (cli->fd == -1)
		return;

	if (n > mirror_ringbuf_avail_write(cli->ringbuf))
	{
		client_disconnect(cli, "hit watermark");
		return;
	}

	mirror_ringbuf_write(cli->ringbuf, bytes, n);
}

static void
client_socket_write(ClientSocket *cli, const char *str)
{
	client_socket_write_bytes(cli, str, strlen(str));
}

static void
client_socket_write_msg(ClientSocket *cli, StringInfo msg)
{
	client_socket_write_bytes(cli, msg->data, msg->len);
}

static void
client_socket_frame(ClientSocket *cli,
					HandleFunc handle_client_frame, void *data)
{
	int maxn = cli->recv_buf->len;
	int i = 0;

	for (i = 0; i < maxn; ++i)
	{
		appendBinaryStringInfo(cli->frame_buf,
							   cli->recv_buf->data + i, 1);

		if (cli->recv_buf->data[i] == '\n')
		{
			chomp(cli->frame_buf);
			handle_client_frame(cli, cli->frame_buf, data);
			resetStringInfo(cli->frame_buf);
		}
	}
}

/*
 * client_socket_read
 *
 * Try to read as much data as possible from a non blocking socket and
 * append to the client recv buffer.
 *
 * Returns true on eof or error
 */
static bool
do_client_socket_read(ClientSocket *cli)
{
	char read_buf[4096];
	int errcode = 0;

	Assert(cli->recv_buf->len == 0);
	Assert(cli->fd != -1);

	while (true)
	{
		ssize_t nr = read(cli->fd, read_buf, 4096);
		errcode = errno;

		if (nr == 0)
			return 1;

		if (nr == -1)
			return errcode != EAGAIN;

		appendBinaryStringInfo(cli->recv_buf, read_buf, nr);
	}

	return false;
}

static bool
client_socket_read(ClientSocket *cli)
{
	bool hit_eof = false;

	if (cli->fd == -1)
		return true;

	hit_eof = do_client_socket_read(cli);

	return hit_eof;
}

typedef struct Stream
{
	int type;
	int id;

	int fd;
	int ev;
	void *data;

	struct pollfd *pfd;
	dlist_node list_node;
} Stream;

typedef struct Selector
{
	dlist_head streams;

	struct pollfd *pfd;
	int list_n;
	int poll_n;

	Stream **ready;
	int ready_n;
	int ready_cap;

	int id;

} Selector;

static Selector *
create_selector()
{
	Selector *selector = palloc0(sizeof(Selector));

	selector->pfd = palloc0(sizeof(struct pollfd));
	selector->ready = palloc0(sizeof(Stream *));

	return selector;
}

static void
destroy_selector(Selector *sel)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &sel->streams)
	{
		Stream *s = dlist_container(Stream, list_node, iter.cur);
		dlist_delete(iter.cur);
		pfree(s);
	}

	pfree(sel->ready);
	pfree(sel->pfd);
	pfree(sel);
}

static void
add_stream(Selector *selector, int type, int fd, int ev, void *data)
{
	Stream *stream = palloc0(sizeof(Stream));

	stream->type = type;
	stream->id = selector->id++;
	stream->fd = fd;
	stream->ev = ev;
	stream->data = data;

	dlist_push_tail(&selector->streams, &stream->list_node);
	selector->list_n++;
}

static int
selector_poll(Selector *selector, int timeout)
{
	int rc;
	int i = 0;
	dlist_iter iter;

	if (selector->poll_n < selector->list_n)
	{
		selector->pfd = repalloc(selector->pfd,
								 sizeof(struct pollfd) * selector->list_n);
	}

	selector->poll_n = selector->list_n;

	dlist_foreach(iter, &selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		stream->pfd = selector->pfd + i;

		selector->pfd[i].fd = stream->fd;
		selector->pfd[i].events = stream->ev;
		selector->pfd[i].revents = 0;

		i++;
	}

	rc = poll(selector->pfd, selector->poll_n, timeout);

	if (selector->ready_cap < selector->poll_n)
	{
		selector->ready = repalloc(selector->ready, selector->poll_n * sizeof(Stream **));
		selector->ready_cap = selector->poll_n;
	}

	i = 0;

	dlist_foreach(iter, &selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		if (!stream->pfd->revents)
		{
			continue;
		}

		selector->ready[i++] = stream;
	}

	selector->ready_n = i;

	return rc;
}

static void
remove_stream(Selector *selector, Stream *stream)
{
	dlist_mutable_iter iter;

	dlist_foreach_modify(iter, &selector->streams)
	{
		Stream *s = dlist_container(Stream, list_node, iter.cur);

		if (s != stream)
		{
			continue;
		}

		dlist_delete(iter.cur);
		pfree(stream);
		selector->list_n--;
	}
}

struct AlertServer
{
	MemoryContext mem_cxt;
	dlist_head subscriptions;
	int send_sock;
	StringInfo cmd_buffer;
	char *tokens[64];

	Selector *selector;
	ListenSocket *lsock;
	double last_heartbeat;

	void *data;
};

typedef struct Subscription
{
	Oid oid;
	const char *name;
	dlist_node list_node;
	StringInfo msg_buf;
	dlist_head clients;

} Subscription;

/*
 * destroy_alert_server
 */
void
destroy_alert_server(AlertServer *server)
{
	destroy_listen_socket(server->lsock);

	/* TODO streams should be destroyed here too */

	destroy_selector(server->selector);
}

/*
 * find_subscription
 */
static Subscription *
find_subscription_by_name(AlertServer *trigger, const char *name)
{
	dlist_iter iter;

	dlist_foreach(iter, &trigger->subscriptions)
	{
		Subscription *subscription =
			dlist_container(Subscription, list_node, iter.cur);

		if (strcasecmp(subscription->name, name) == 0)
			return subscription;
	}

	return NULL;
}

/*
 * find_subscription_by_oid
 */
static Subscription *
find_subscription_by_oid(AlertServer *trigger, Oid oid)
{
	dlist_iter iter;

	dlist_foreach(iter, &trigger->subscriptions)
	{
		Subscription *subscription;
		subscription = dlist_container(Subscription, list_node, iter.cur);

		if (subscription->oid == oid)
			return subscription;
	}

	return NULL;
}

/*
 * subscription_add_client
 */
static void
subscription_add_client(Subscription *subscription, ClientSocket *socket)
{
	dlist_push_tail(&subscription->clients, &socket->list_node);
}

/*
 * subscription_remove_client
 */
static void
subscription_remove_client(Subscription *subscription, ClientSocket *socket)
{
	dlist_delete(&socket->list_node);
}

/*
 * handle_client_frame
 */
static void
handle_client_frame(ClientSocket *client,
					StringInfo info,
					void *data)
{
	int ntoks = 0;
	char *sptr;
	char *saveptr = 0;

	AlertServer *ts = (AlertServer *) (data);

	resetStringInfo(ts->cmd_buffer);
	appendBinaryStringInfo(ts->cmd_buffer, info->data, info->len);
	sptr = ts->cmd_buffer->data;
	client->last_read_time = get_time();

	while (ntoks < 64)
	{
		char *tok = strtok_r(sptr, "\t", &saveptr);

		if (!tok)
			break;

		ts->tokens[ntoks++] = tok;
		sptr = 0;
	}

	if (ntoks < 1)
	{
		/* bad msg */
		return;
	}

	if (strcasecmp(ts->tokens[0], TS_MSG_SUBSCRIBE) == 0)
	{
		Subscription *subscription;
		Assert(ntoks == 2);

		subscription = find_subscription_by_name(ts, ts->tokens[1]);

		if (!subscription)
		{
			/* handle the edge case where the trigger exists but
			 * no WAL changes have been seen for it */
			trigger_check_catalog(ts->data, ts->tokens[1]);
			subscription = find_subscription_by_name(ts, ts->tokens[1]);
		}

		/* We only allow one subscription per client */
		if (subscription && !client->subscription)
		{
			subscription_add_client(subscription, client);
			client->subscription = subscription;

			client_socket_write(client, TS_MSG_SUBSCRIBE_OK "\n");
		}
		else
		{
			client_socket_write(client, TS_MSG_SUBSCRIBE_FAIL "\n");
		}
	}
	else if (strcasecmp(ts->tokens[0], TS_MSG_UNSUBSCRIBE) == 0)
	{
		Subscription *subscription;
		Assert(ntoks == 2);

		subscription = find_subscription_by_name(ts, ts->tokens[1]);

		if (subscription)
		{
			subscription_remove_client(client->subscription, client);
			client->subscription = 0;

			client_socket_write(client, TS_MSG_UNSUBSCRIBE_OK "\n");
		}
		else
		{
			client_socket_write(client, TS_MSG_UNSUBSCRIBE_FAIL "\n");
		}
	}
}

static void
alert_server_heartbeat(AlertServer *server)
{
	dlist_iter iter;
	double time_now = get_time();
	double delta = time_now - server->last_heartbeat;

	if (delta < TS_HEARTBEAT_TIMEOUT)
		return;

	dlist_foreach(iter, &server->selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		if (stream->type == Client)
		{
			client_socket_write(stream->data, TS_MSG_HEARTBEAT "\n");
			client_socket_try_send(stream->data);
		}
	}

	server->last_heartbeat = time_now;
}

/*
 * check_client_timeouts
 *
 * Check the last read times of all clients.
 */
static void
check_client_timeouts(AlertServer *server)
{
	dlist_iter iter;
	double time_now = get_time();

	dlist_foreach(iter, &server->selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		if (stream->type == Client)
		{
			ClientSocket *cli = stream->data;
			double delta = time_now - cli->last_read_time;

			if (delta > TS_READ_TIMEOUT)
				client_disconnect(cli, "timed out");
		}
	}
}

/*
 * check_client_pollout
 *
 * Mark stream as wanting pollout if there is data to write
 */
static void
check_client_pollout(AlertServer *server)
{
	dlist_iter iter;

	dlist_foreach(iter, &server->selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		if (stream->type == Client)
		{
			ClientSocket *cli = stream->data;

			int n = mirror_ringbuf_avail_read(cli->ringbuf);

			if (n > 0)
			{
				stream->ev = POLLIN | POLLOUT;
			}
			else
			{
				stream->ev = POLLIN;
			}
		}
	}
}

/*
 * alert_server_handle
 */
void
alert_server_handle(AlertServer *server)
{
	ListenSocket *sock;
	Selector *selector;
	int i = 0;
	int rc = 0;

	check_client_pollout(server);
	rc = selector_poll(server->selector, 1);

	(void)(rc);

	selector = server->selector;
	sock = server->lsock;

	for (i = 0; i < selector->ready_n; ++i)
	{
		Stream *stream = selector->ready[i];

		if (stream->type == Acceptor)
		{
			int conn = accept(sock->fd, 0, 0);
			ClientSocket *cli = create_client_socket(conn);
			add_stream(selector, Client, conn, POLLIN, cli);
		}
		else if (stream->type == Client)
		{
			ClientSocket *cli = stream->data;

			if (stream->pfd->revents & POLLOUT)
				client_socket_try_send(cli);

			if (stream->pfd->revents & POLLIN)
			{
				bool eof = client_socket_read(cli);
				client_socket_frame(cli, handle_client_frame, server);
				resetStringInfo(cli->recv_buf);

				if (eof)
				{
					if (cli->subscription)
						subscription_remove_client(cli->subscription, cli);
					remove_stream(selector, stream);
					destroy_client(cli);
				}
			}
		}
	}

	check_client_timeouts(server);
	alert_server_heartbeat(server);
}

/*
 * alert_server_add
 */
void
alert_server_add(AlertServer *server, Oid oid, const char *name)
{
	Subscription *s = find_subscription_by_oid(server, oid);

	if (s)
		return;

	s = palloc0(sizeof(Subscription));

	s->oid = oid;
	s->name = name;
	s->msg_buf = makeStringInfo();

	dlist_push_tail(&server->subscriptions, &s->list_node);
}

/*
 * alert_server_remove
 */
void
alert_server_remove(AlertServer *server, Oid oid)
{
	dlist_iter iter;
	Subscription *s = find_subscription_by_oid(server, oid);

	if (!s)
		return;

	dlist_foreach(iter, &s->clients)
	{
		ClientSocket *client;
		client = dlist_container(ClientSocket, list_node, iter.cur);

		client_socket_write(client, TS_MSG_DROPPED "\n");
		client_socket_try_send(client);

		/* client will get cleaned up in poll loop */
		client->subscription = NULL;
	}

	pfree(s->msg_buf->data);
	pfree(s->msg_buf);

	dlist_delete(&s->list_node);
	pfree(s);
}

/*
 * alert_server_push
 *
 * Push msg to all interested clients.
 */
void
alert_server_push(AlertServer *server, Oid oid, StringInfo msg)
{
	dlist_iter iter;
	Subscription *s = find_subscription_by_oid(server, oid);
	Assert(s);

	dlist_foreach(iter, &s->clients)
	{
		StringInfo out;
		ClientSocket *client =
			dlist_container(ClientSocket, list_node, iter.cur);

		resetStringInfo(client->scratch);
		out = client->scratch;

		/* TODO - optimize into a writev type form */
		appendStringInfo(out, TS_MSG_ALERT "\t");
		appendBinaryStringInfo(out, msg->data, msg->len);
		appendStringInfo(out, "\n");

		client_socket_write_msg(client, out);
	}
}

/*
 * create_alert_server
 */
AlertServer *
create_alert_server(void *data)
{
	AlertServer *server = palloc0(sizeof(AlertServer));

	server->mem_cxt = AllocSetContextCreate(CurrentMemoryContext,
				"AlertServerContext",
				ALLOCSET_DEFAULT_MINSIZE,
				ALLOCSET_DEFAULT_INITSIZE,
				ALLOCSET_DEFAULT_MAXSIZE);

	server->cmd_buffer = makeStringInfo();
	server->send_sock = -1;

	server->selector = create_selector();
	server->lsock = create_listen_socket();
	server->data = data;

	add_stream(server->selector, Acceptor,
			server->lsock->fd, POLLIN, server->lsock);

	return server;
}

/*
 * alert_server_flush
 *
 * Try and send all buffered up data. This is typically done at the
 * end of a transaction.
 */
void
alert_server_flush(AlertServer *server)
{
	dlist_iter iter;

	dlist_foreach(iter, &server->selector->streams)
	{
		Stream *stream = dlist_container(Stream, list_node, iter.cur);

		if (stream->type == Client)
			client_socket_try_send(stream->data);
	}
}
