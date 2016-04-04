/*-------------------------------------------------------------------------
 *
 * recv_alerts.c
 *	  Functionality for recv_alerts client
 *
 * This client is repsonsible for connecting to alert server(s) and
 * subscribing to a given alert.
 *
 * Alert messages are output to stdout in 'COPY' format
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#include "compat.h"

#include <arpa/inet.h>
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pwd.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

#include "options.h"

#define MAX_NODES 64
#define MAX_MSG_TYPE_LEN 63
#define READ_BUF_LEN 4096

#define CONN_TIMEOUT 5
#define READ_TIMEOUT 10

typedef struct Client
{
	const char *host;
	uint16_t port;
	const char *subs;
	int fd;
	int num_conns;
	double last_read_time;
	double last_conn_time;

	StringInfo recv_buf;
	StringInfo frame_buf;
	StringInfo send_buf;
} Client;

typedef struct ProcData
{
	int num_nodes;
	int num_dropped;
} ProcData;

typedef void (*HandleFunc)(Client *cli,
						   StringInfo frame,
						   void *data);

#define STOP_BUF_SIZE 256

volatile bool keep_running = true;
volatile int sig_recv = 0;
char stop_reason[STOP_BUF_SIZE] = {0};

#define stop_running(format, ...) \
{ \
	keep_running = false; \
	snprintf(stop_reason, STOP_BUF_SIZE, format, ##__VA_ARGS__); \
}

static void
sighandle(int x)
{
	sig_recv = x;
	keep_running = false;
}

static void
client_disconnect(Client *cli, const char *reason);

/*
 * client_init
 */
static void
client_init(Client *cli, const char* host, int port, const char* subs)
{
	memset(cli, 0, sizeof(Client));

	cli->host = host;
	cli->port = port;
	cli->subs = subs;
	cli->fd = -1;

	cli->recv_buf = makeStringInfo();
	cli->frame_buf = makeStringInfo();
	cli->send_buf = makeStringInfo();
}

/*
 * tcp_connect
 *
 * Connect and configure a tcp socket
 * Returns -1 on failure
 */
static int
tcp_connect(const char *host, uint16_t port)
{
	struct sockaddr_in serv_addr;
	struct hostent *server;
	int rc;
	int on = 1;

	int fd = socket(AF_INET, SOCK_STREAM, 0);

	if (fd < 0)
		return -1;

	memset(&serv_addr, 0, sizeof(serv_addr));
	server = gethostbyname(host);

	if (!server)
		goto close_fd;

	serv_addr.sin_family = AF_INET;
	serv_addr.sin_port = htons(port);
	memcpy(&serv_addr.sin_addr.s_addr, server->h_addr, server->h_length);

	rc = connect(fd, (struct sockaddr*) &serv_addr, sizeof(serv_addr));

	if (rc != 0)
		goto close_fd;

	rc = fcntl(fd, F_SETFL, O_NONBLOCK);

	if (rc != 0)
		goto close_fd;

	rc = setsockopt(fd, SOL_TCP, TCP_NODELAY, &on, sizeof(on));

	if (rc != 0)
		goto close_fd;

	return fd;

close_fd:

	close(fd);
	return -1;
}

/*
 * client_connect
 *
 * Connect to the specified server
 */
static void
client_connect(Client *cli, double time_now)
{
	cli->fd = tcp_connect(cli->host, cli->port);
	cli->last_conn_time = time_now;
	cli->last_read_time = time_now;
}

/*
 * teardown
 *
 * Disconnect and clean up client memory
 */
static void
client_teardown(Client* cli)
{
	client_disconnect(cli, "client teardown");

	pfree(cli->recv_buf->data);
	pfree(cli->recv_buf);

	pfree(cli->frame_buf->data);
	pfree(cli->frame_buf);

	pfree(cli->send_buf->data);
	pfree(cli->send_buf);
}

/*
 * client_append_data
 *
 * Append data to the client's recv buf
 */
static void
client_append_data(Client *cli, const char* s, int nr)
{
	cli->last_read_time = get_time();
	appendBinaryStringInfo(cli->recv_buf, s, nr);
}

/*
 * chomp
 *
 * Strip any \r \n off the end of a string info.
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
 * client_disconnect
 *
 * Disconnect client if it is not already disconnected.
 */
static void
client_disconnect(Client *cli, const char *reason)
{
	if (cli->fd != -1)
	{
		debugmsg("client disconnect: %s", reason);
		close(cli->fd);
		cli->fd = -1;
	}
}

/*
 * client_send_buffer
 *
 * Send client send_buf to the server.
 * Clears send buffer afterward.
 */
static void
client_send_buffer(Client *cli)
{
	if (cli->fd != -1)
	{
		int bytes;
		appendStringInfo(cli->send_buf, "\n");
		bytes = send(cli->fd, cli->send_buf->data, cli->send_buf->len, 0);

		if (bytes != cli->send_buf->len)
			client_disconnect(cli, "server write fail");
	}

	resetStringInfo(cli->send_buf);
}

/*
 * client_parse_frames
 *
 * Parse frames from the recv buffer, and pass them to a function
 */
static void
client_parse_frames(Client *cli, HandleFunc handle_fn, void* data)
{
	int maxn = cli->recv_buf->len;
	int i = 0;

	for (i = 0; i < maxn; ++i)
	{
		appendBinaryStringInfo(cli->frame_buf, cli->recv_buf->data + i, 1);

		if (cli->recv_buf->data[i] == '\n')
		{
			chomp(cli->frame_buf);
			handle_fn(cli, cli->frame_buf, data);
			resetStringInfo(cli->frame_buf);
		}
	}
}

/*
 * client_read
 *
 * Read small chunks of data from a non blocking socket, and append them
 * to client recv buffer.
 */
static int
client_read(Client *cli)
{
	char read_buf[READ_BUF_LEN];

	if (cli->fd == -1)
		return 1;

	while (true)
	{
		ssize_t nr = read(cli->fd, read_buf, READ_BUF_LEN);

		if (nr == 0)
		{
			return 1;
		}

		if (nr == -1)
		{
			if (errno == EAGAIN || errno == EINTR)
			{
				return 0;
			}
			else
			{
				return 1;
			}
		}
		else
		{
			client_append_data(cli, read_buf, nr);
		}
	}

	return 0;
}

/*
 * get_msg_type
 *
 * Parse the message type. Copy it to passed in buffer and return len.
 */
static int
get_msg_type(StringInfo info, char* msg_type)
{
	int type_len;
	char* split = strchr(info->data, '\t');

	if (split == NULL)
		split = info->data + info->len;

	type_len = split - info->data;

	if (type_len > MAX_MSG_TYPE_LEN)
	{
		type_len = MAX_MSG_TYPE_LEN;
	}

	memcpy(msg_type, info->data, type_len);
	msg_type[type_len] = '\0';

	return type_len;
}

/*
 * client_send_heartbeat_ack
 *
 * Send a heartbeat ack message to server
 */
static void
client_send_heartbeat_ack(Client *cli)
{
	appendStringInfo(cli->send_buf, "heartbeat_ack");
	client_send_buffer(cli);
}

/*
 * handle_client_data
 *
 * Handles frames of client data.
 */
static void
handle_client_data(Client *cli, StringInfo frame, void *data)
{
	char msg_type[MAX_MSG_TYPE_LEN + 1];
	int n = get_msg_type(frame, msg_type);

	ProcData *proc_data = (ProcData*)(data);

	if (strcasecmp(msg_type, "subscribe_fail") == 0)
	{
		stop_running("failed to subscribe to %s:%d", cli->host, cli->port);
	}
	else if (strcasecmp(msg_type, "dropped") == 0)
	{
		proc_data->num_dropped++;

		if (proc_data->num_dropped == proc_data->num_nodes)
			stop_running("all nodes dropped alert");
	}
	else if (strcasecmp(msg_type, "alert") == 0)
	{
		frame->data[frame->len] = '\n';
		write(STDOUT_FILENO, frame->data + n + 1, frame->len - n);
		frame->data[frame->len] = '\0';
	}
	else if (strcasecmp(msg_type, "heartbeat") == 0)
	{
		client_send_heartbeat_ack(cli);
	}
	else
	{
		/* status messages */
	}
}

/*
 * client_subscriber
 *
 * Send a subscribe message to the server
 */
static void
client_subscribe(Client *cli)
{
	appendStringInfo(cli->send_buf, "subscribe\t%s", cli->subs);
	client_send_buffer(cli);
}

/*
 * try_connection
 *
 * Try connecting to the server (if enough time has elapsed since last time)
 */
static void
try_connection(Client *cli, double time_now)
{
	double delta = (time_now - cli->last_conn_time);
	bool allowed = cli->num_conns == 0 || (delta > CONN_TIMEOUT);

	if (!allowed)
		return;

	client_connect(cli, time_now);
	cli->num_conns++;

	debugmsg("client connect attempt %d result fd %d", cli->num_conns, cli->fd);

	if (cli->fd != -1)
	{
		client_subscribe(cli);
	}
	else
	{
		/* couldn't connect */
	}
}

/*
 * client_process_events
 *
 * Process client poll events
 */
static void
client_process_events(Client *cli, double time_now, short revents, void *data)
{
	bool hit_eof = client_read(cli);
	client_parse_frames(cli, handle_client_data, data);
	resetStringInfo(cli->recv_buf);

	if (hit_eof)
	{
		client_disconnect(cli, "client read hit eof");

		/* any buffered up partial frames are invalid now */
		resetStringInfo(cli->frame_buf);
	}
}

/*
 * client_process_timeouts
 *
 * Check the read timeout, disconnect if exceeded.
 */
static void
client_process_timeouts(Client *cli, double time_now, void *data)
{
	double delta = (time_now - cli->last_read_time);

	if (delta < READ_TIMEOUT)
		return;

	client_disconnect(cli, "client read timeout");
}

/*
 * client_tick
 *
 * Called once per poll loop. Checks reconnect logic, and handled any
 * polled events or timeouts.
 */
static void
client_tick(Client *cli, double time_now, short revents, void *data)
{
	if (cli->fd == -1)
	{
		try_connection(cli, time_now);
	}
	else
	{
		if (revents)
		{
			client_process_events(cli, time_now, revents, data);
		}
		else
		{
			client_process_timeouts(cli, time_now, data);
		}
	}
}

/*
 * main
 *
 * Connect to trigger server(s), and set up a polling loop to process
 * events.
 */
int
main(int argc, char** argv)
{
	struct pollfd *pfds;
	Client *clients;
	ProcData proc_data;
	Args args;
	int i = 0;
	int timeout = 0;
	int started = 0;

	parse_args(&args, argc, argv);

	if (args.subs == 0)
		die_with_help("must specify an alert\n");

	auto_configure(&args);

	if (args.num_nodes == 0)
		die("alert \"%s\" does not exist", args.subs);

	proc_data.num_nodes = args.num_nodes;
	proc_data.num_dropped = 0;

	signal(SIGINT, sighandle);
	signal(SIGTERM, sighandle);
	signal(SIGQUIT, sighandle);
	signal(SIGPIPE, sighandle);
	signal(SIGUSR1, sighandle);
	signal(SIGUSR2, sighandle);
	signal(SIGHUP, sighandle);

	clients = palloc0(args.num_nodes * sizeof(Client));
	pfds = palloc0(args.num_nodes * sizeof(struct pollfd));

	for (i = 0; i < args.num_nodes; ++i)
	{
		client_init(&clients[i],
				args.nodes[i].host,
				args.nodes[i].port,
				args.subs);
	}

	while (keep_running)
	{
		double time_now;
		int rc;

		for (i = 0; i < args.num_nodes; ++i)
		{
			pfds[i].fd = clients[i].fd;
			pfds[i].events = POLLIN | POLLHUP;
			pfds[i].revents = 0;
		}

		/* initial timeout is zero, so this falls thru */
		rc = poll(pfds, args.num_nodes, timeout);
		timeout = 1000;

		if (rc == -1)
		{
			if (errno == EINTR)
				continue;

			stop_running("poll error %s", strerror(errno));
			continue;
		}

		time_now = get_time();

		for (i = 0; i < args.num_nodes; ++i)
		{
			client_tick(&clients[i], time_now, pfds[i].revents,
					&proc_data);
		}

		/* for the first loop, make it a precondition that all nodes
		 * are connected */

		if (!started)
		{
			for (i = 0; i < args.num_nodes; ++i)
			{
				if (clients[i].fd == -1)
				{
					stop_running("failed initial connection to %s:%d",
								 clients[i].host, clients[i].port);
					break;
				}
			}

			started = 1;
		}
	}

	if (sig_recv)
		snprintf(stop_reason, STOP_BUF_SIZE, "sig_recv %d", sig_recv);

	print_to_err("stopped running: %s", stop_reason);

	for (i = 0; i < args.num_nodes; ++i)
		client_teardown(&clients[i]);

	pfree(clients);
	pfree(pfds);
	cleanup_args(&args);

	return 0;
}
