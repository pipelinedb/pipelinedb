/*-------------------------------------------------------------------------
 *
 * poolcomm.c
 *
 *	  Communication functions between the pool manager and session
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stddef.h>
#include "c.h"
#include "postgres.h"
#include "pgxc/poolcomm.h"
#include "storage/ipc.h"
#include "utils/elog.h"
#include "miscadmin.h"

static int	pool_recvbuf(PoolPort *port);
static int	pool_discardbytes(PoolPort *port, size_t len);

#ifdef HAVE_UNIX_SOCKETS

#define POOLER_UNIXSOCK_PATH(path, port, sockdir) \
	snprintf(path, sizeof(path), "%s/.s.PGPOOL.%d", \
			((sockdir) && *(sockdir) != '\0') ? (sockdir) : \
			DEFAULT_PGSOCKET_DIR, \
			(port))

static char sock_path[MAXPGPATH];

static void StreamDoUnlink(int code, Datum arg);

static int	Lock_AF_UNIX(unsigned short port, const char *unixSocketName);
#endif

/*
 * Open server socket on specified port to accept connection from sessions
 */
int
pool_listen(unsigned short port, const char *unixSocketName)
{
	int			fd,
				len;
	struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS
	if (Lock_AF_UNIX(port, unixSocketName) < 0)
		return -1;

	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	/* fill in socket address structure */
	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	/* bind the name to the descriptor */
	if (bind(fd, (struct sockaddr *) & unix_addr, len) < 0)
		return -1;

	/* tell kernel we're a server */
	if (listen(fd, 5) < 0)
		return -1;

	/* Arrange to unlink the socket file at exit */
	on_proc_exit(StreamDoUnlink, 0);

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}

/* StreamDoUnlink()
 * Shutdown routine for pooler connection
 * If a Unix socket is used for communication, explicitly close it.
 */
#ifdef HAVE_UNIX_SOCKETS
static void
StreamDoUnlink(int code, Datum arg)
{
	Assert(sock_path[0]);
	unlink(sock_path);
}
#endif   /* HAVE_UNIX_SOCKETS */

#ifdef HAVE_UNIX_SOCKETS
static int
Lock_AF_UNIX(unsigned short port, const char *unixSocketName)
{
	POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

	CreateSocketLockFile(sock_path, true, NULL);

	unlink(sock_path);

	return 0;
}
#endif

/*
 * Connect to pooler listening on specified port
 */
int
pool_connect(unsigned short port, const char *unixSocketName)
{
	int			fd,
				len;
	struct sockaddr_un unix_addr;

#ifdef HAVE_UNIX_SOCKETS
	/* create a Unix domain stream socket */
	if ((fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
		return -1;

	/* fill socket address structure w/server's addr */
	POOLER_UNIXSOCK_PATH(sock_path, port, unixSocketName);

	memset(&unix_addr, 0, sizeof(unix_addr));
	unix_addr.sun_family = AF_UNIX;
	strcpy(unix_addr.sun_path, sock_path);
	len = sizeof(unix_addr.sun_family) +
		strlen(unix_addr.sun_path) + 1;

	if (connect(fd, (struct sockaddr *) & unix_addr, len) < 0)
		return -1;

	return fd;
#else
	/* TODO support for non-unix platform */
	ereport(FATAL,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("pool manager only supports UNIX socket")));
	return -1;
#endif
}


/*
 * Get one byte from the buffer, read data from the connection if buffer is empty
 */
int
pool_getbyte(PoolPort *port)
{
	while (port->RecvPointer >= port->RecvLength)
	{
		if (pool_recvbuf(port)) /* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Get one byte from the buffer if it is not empty
 */
int
pool_pollbyte(PoolPort *port)
{
	if (port->RecvPointer >= port->RecvLength)
	{
		return EOF;				/* Empty buffer */
	}
	return (unsigned char) port->RecvBuffer[port->RecvPointer++];
}


/*
 * Read pooler protocol message from the buffer.
 */
int
pool_getmessage(PoolPort *port, StringInfo s, int maxlen)
{
	int32		len;

	resetStringInfo(s);

	/* Read message length word */
	if (pool_getbytes(port, (char *) &len, 4) == EOF)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = ntohl(len);

	if (len < 4 ||
		(maxlen > 0 && len > maxlen))
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.	If we run out of room (ridiculously
		 * large message), we will elog(ERROR)
		 */
		PG_TRY();
		{
			enlargeStringInfo(s, len);
		}
		PG_CATCH();
		{
			if (pool_discardbytes(port, len) == EOF)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("incomplete message from client")));
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* And grab the message */
		if (pool_getbytes(port, s->data, len) == EOF)
		{
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("incomplete message from client")));
			return EOF;
		}
		s->len = len;
		/* Place a trailing null per StringInfo convention */
		s->data[len] = '\0';
	}

	return 0;
}


/* --------------------------------
 * pool_getbytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_getbytes(PoolPort *port, char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
		if (amount > len)
			amount = len;
		memcpy(s, port->RecvBuffer + port->RecvPointer, amount);
		port->RecvPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_discardbytes - discard a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_discardbytes(PoolPort *port, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (port->RecvPointer >= port->RecvLength)
		{
			if (pool_recvbuf(port))		/* If nothing in buffer, then recv
										 * some */
				return EOF;		/* Failed to recv data */
		}
		amount = port->RecvLength - port->RecvPointer;
		if (amount > len)
			amount = len;
		port->RecvPointer += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 * pool_recvbuf - load some bytes into the input buffer
 *
 * returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pool_recvbuf(PoolPort *port)
{
	if (port->RecvPointer > 0)
	{
		if (port->RecvLength > port->RecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(port->RecvBuffer, port->RecvBuffer + port->RecvPointer,
					port->RecvLength - port->RecvPointer);
			port->RecvLength -= port->RecvPointer;
			port->RecvPointer = 0;
		}
		else
			port->RecvLength = port->RecvPointer = 0;
	}

	/* Can fill buffer from PqRecvLength and upwards */
	for (;;)
	{
		int			r;

		r = recv(Socket(*port), port->RecvBuffer + port->RecvLength,
				 POOL_BUFFER_SIZE - port->RecvLength, 0);

		if (r < 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if interrupted */

			/*
			 * Report broken connection
			 */
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("could not receive data from client: %m")));
			return EOF;
		}
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return EOF;
		}
		/* r contains number of bytes read, so just incr length */
		port->RecvLength += r;
		return 0;
	}
}


/*
 * Put a known number of bytes into the connection buffer
 */
int
pool_putbytes(PoolPort *port, const char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if (port->SendPointer >= POOL_BUFFER_SIZE)
			if (pool_flush(port))
				return EOF;
		amount = POOL_BUFFER_SIZE - port->SendPointer;
		if (amount > len)
			amount = len;
		memcpy(port->SendBuffer + port->SendPointer, s, amount);
		port->SendPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}


/* --------------------------------
 *		pool_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pool_flush(PoolPort *port)
{
	static int	last_reported_send_errno = 0;

	char	   *bufptr = port->SendBuffer;
	char	   *bufend = port->SendBuffer + port->SendPointer;

	while (bufptr < bufend)
	{
		int			r;

		r = send(Socket(*port), bufptr, bufend - bufptr, 0);

		if (r <= 0)
		{
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;

				/*
				 * Handle a seg fault that may later occur in proc array
				 * when this fails when we are already shutting down
				 * If shutting down already, do not call.
				 */
				if (!proc_exit_inprogress)
					return 0;
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon.
			 */
			port->SendPointer = 0;
			return EOF;
		}

		last_reported_send_errno = 0;	/* reset after any successful send */
		bufptr += r;
	}

	port->SendPointer = 0;
	return 0;
}


/*
 * Put the pooler protocol message into the connection buffer
 */
int
pool_putmessage(PoolPort *port, char msgtype, const char *s, size_t len)
{
	uint		n32;

	if (pool_putbytes(port, &msgtype, 1))
		return EOF;

	n32 = htonl((uint32) (len + 4));
	if (pool_putbytes(port, (char *) &n32, 4))
		return EOF;

	if (pool_putbytes(port, s, len))
		return EOF;

	return 0;
}

/* message code('f'), size(8), node_count */
#define SEND_MSG_BUFFER_SIZE 9
/* message code('s'), result */
#define SEND_RES_BUFFER_SIZE 5
#define SEND_PID_BUFFER_SIZE (5 + (MaxConnections - 1) * 4)

/*
 * Build up a message carrying file descriptors or process numbers and send them over specified
 * connection
 */
int
pool_sendfds(PoolPort *port, int *fds, int count)
{
	struct iovec iov[1];
	struct msghdr msg;
	char		buf[SEND_MSG_BUFFER_SIZE];
	uint		n32;
	int			controllen = sizeof(struct cmsghdr) + count * sizeof(int);
	struct cmsghdr *cmptr = NULL;

	buf[0] = 'f';
	n32 = htonl((uint32) 8);
	memcpy(buf + 1, &n32, 4);
	n32 = htonl((uint32) count);
	memcpy(buf + 5, &n32, 4);

	iov[0].iov_base = buf;
	iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	if (count == 0)
	{
		msg.msg_control = NULL;
		msg.msg_controllen = 0;
	}
	else
	{
		if ((cmptr = malloc(controllen)) == NULL)
			return EOF;
		cmptr->cmsg_level = SOL_SOCKET;
		cmptr->cmsg_type = SCM_RIGHTS;
		cmptr->cmsg_len = controllen;
		msg.msg_control = (caddr_t) cmptr;
		msg.msg_controllen = controllen;
		/* the fd to pass */
		memcpy(CMSG_DATA(cmptr), fds, count * sizeof(int));
	}

	if (sendmsg(Socket(*port), &msg, 0) != SEND_MSG_BUFFER_SIZE)
	{
		if (cmptr)
			free(cmptr);
		return EOF;
	}

	if (cmptr)
		free(cmptr);

	return 0;
}


/*
 * Read a message from the specified connection carrying file descriptors
 */
int
pool_recvfds(PoolPort *port, int *fds, int count)
{
	int			r;
	uint		n32;
	char		buf[SEND_MSG_BUFFER_SIZE];
	struct iovec iov[1];
	struct msghdr msg;
	int			controllen = sizeof(struct cmsghdr) + count * sizeof(int);
	struct cmsghdr *cmptr = malloc(controllen);

	if (cmptr == NULL)
		return EOF;

	iov[0].iov_base = buf;
	iov[0].iov_len = SEND_MSG_BUFFER_SIZE;
	msg.msg_iov = iov;
	msg.msg_iovlen = 1;
	msg.msg_name = NULL;
	msg.msg_namelen = 0;
	msg.msg_control = (caddr_t) cmptr;
	msg.msg_controllen = controllen;

	r = recvmsg(Socket(*port), &msg, 0);
	if (r < 0)
	{
		/*
		 * Report broken connection
		 */
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not receive data from client: %m")));
		goto failure;
	}
	else if (r == 0)
	{
		goto failure;
	}
	else if (r != SEND_MSG_BUFFER_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete message from client")));
		goto failure;
	}

	/* Verify response */
	if (buf[0] != 'f')
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = ntohl(n32);
	if (n32 != 8)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("invalid message size")));
		goto failure;
	}

	/*
	 * If connection count is 0 it means pool does not have connections
	 * to  fulfill request. Otherwise number of returned connections
	 * should be equal to requested count. If it not the case consider this
	 * a protocol violation. (Probably connection went out of sync)
	 */
	memcpy(&n32, buf + 5, 4);
	n32 = ntohl(n32);
	if (n32 == 0)
	{
		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("failed to acquire connections")));
		goto failure;
	}

	if (n32 != count)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected connection count")));
		goto failure;
	}

	memcpy(fds, CMSG_DATA(cmptr), count * sizeof(int));
	free(cmptr);
	return 0;
failure:
	free(cmptr);
	return EOF;
}

/*
 * Send result to specified connection
 */
int
pool_sendres(PoolPort *port, int res)
{
	char		buf[SEND_RES_BUFFER_SIZE];
	uint		n32;

	/* Header */
	buf[0] = 's';
	/* Result */
	n32 = htonl(res);
	memcpy(buf + 1, &n32, 4);

	if (send(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0) != SEND_RES_BUFFER_SIZE)
		return EOF;

	return 0;
}

/*
 * Read result from specified connection.
 * Return 0 at success or EOF at error.
 */
int
pool_recvres(PoolPort *port)
{
	int			r;
	int			res = 0;
	uint		n32;
	char		buf[SEND_RES_BUFFER_SIZE];

	r = recv(Socket(*port), &buf, SEND_RES_BUFFER_SIZE, 0);
	if (r < 0)
	{
		/*
		 * Report broken connection
		 */
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not receive data from client: %m")));
		goto failure;
	}
	else if (r == 0)
	{
		goto failure;
	}
	else if (r != SEND_RES_BUFFER_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete message from client")));
		goto failure;
	}

	/* Verify response */
	if (buf[0] != 's')
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = ntohl(n32);
	if (n32 != 0)
		return EOF;

	return res;

failure:
	return EOF;
}

/*
 * Read a message from the specified connection carrying pid numbers
 * of transactions interacting with pooler
 */
int
pool_recvpids(PoolPort *port, int **pids)
{
	int			r, i;
	uint		n32;
	char		buf[SEND_PID_BUFFER_SIZE];

	/*
	 * Buffer size is upper bounded by the maximum number of connections,
	 * as in the pooler each connection has one Pooler Agent.
	 */

	r = recv(Socket(*port), &buf, SEND_PID_BUFFER_SIZE, 0);
	if (r < 0)
	{
		/*
		 * Report broken connection
		 */
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("could not receive data from client: %m")));
		goto failure;
	}
	else if (r == 0)
	{
		goto failure;
	}
	else if (r != SEND_PID_BUFFER_SIZE)
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("incomplete message from client")));
		goto failure;
	}

	/* Verify response */
	if (buf[0] != 'p')
	{
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("unexpected message code")));
		goto failure;
	}

	memcpy(&n32, buf + 1, 4);
	n32 = ntohl(n32);
	if (n32 == 0)
	{
		elog(WARNING, "No transaction to abort");
		return n32;
	}

	*pids = (int *) palloc(sizeof(int) * n32);

	for (i = 0; i < n32; i++)
	{
		int n;
		memcpy(&n, buf + 5 + i * sizeof(int), sizeof(int));
		*pids[i] = ntohl(n);
	}
	return n32;

failure:
	return 0;
}

/*
 * Send a message containing pid numbers to the specified connection
 */
int
pool_sendpids(PoolPort *port, int *pids, int count)
{
	int res = 0;
	int i;
	char		buf[SEND_PID_BUFFER_SIZE];
	uint		n32;

	buf[0] = 'p';
	n32 = htonl((uint32) count);
	memcpy(buf + 1, &n32, 4);
	for (i = 0; i < count; i++)
	{
		int n;
		n = htonl((uint32) pids[i]);
		memcpy(buf + 5 + i * sizeof(int), &n, 4);
	}

	if (send(Socket(*port), &buf, SEND_PID_BUFFER_SIZE,0) != SEND_PID_BUFFER_SIZE)
	{
		res = EOF;
	}

	return res;
}
