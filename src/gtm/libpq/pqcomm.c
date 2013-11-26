/*-------------------------------------------------------------------------
 *
 * pqcomm.c
 *	  Communication functions between the Frontend and the Backend
 *
 * These routines handle the low-level details of communication between
 * frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data --- or would be,
 * except for major brain damage in the design of the old COPY OUT protocol.
 * Unfortunately, COPY OUT was designed to commandeer the communication
 * channel (it just transfers data without wrapping it into messages).
 * No other messages can be sent while COPY OUT is in progress; and if the
 * copy is aborted by an ereport(ERROR), we need to close out the copy so that
 * the frontend gets back into sync.  Therefore, these routines have to be
 * aware of COPY OUT state.  (New COPY-OUT is message-based and does *not*
 * set the DoingCopyOut flag.)
 *
 * NOTE: generally, it's a bad idea to emit outgoing messages directly with
 * pq_putbytes(), especially if the message would require multiple calls
 * to send.  Instead, use the routines in pqformat.c to construct the message
 * in a buffer and then emit it in one call to pq_putmessage.  This ensures
 * that the channel will not be clogged by an incomplete message if execution
 * is aborted by ereport(ERROR) partway through the message.  The only
 * non-libpq code that should call pq_putbytes directly is old-style COPY OUT.
 *
 * At one time, libpq was shared between frontend and backend, but now
 * the backend's "backend/libpq" is quite separate from "interfaces/libpq".
 * All that remains is similarities of names to trap the unwary...
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *	$PostgreSQL: pgsql/src/backend/libpq/pqcomm.c,v 1.198 2008/01/01 19:45:49 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

/*------------------------
 * INTERFACE ROUTINES
 *
 * setup/teardown:
 *		StreamServerPort	- Open postmaster's server port
 *		StreamConnection	- Create new connection with client
 *		StreamClose			- Close a client/backend connection
 *		TouchSocketFile		- Protect socket file against /tmp cleaners
 *		pq_init			- initialize libpq at backend startup
 *		pq_comm_reset	- reset libpq during error recovery
 *		pq_close		- shutdown libpq at backend exit
 *
 * low-level I/O:
 *		pq_getbytes		- get a known number of bytes from connection
 *		pq_getstring	- get a null terminated string from connection
 *		pq_getmessage	- get a message with length word from connection
 *		pq_getbyte		- get next byte from connection
 *		pq_peekbyte		- peek at next byte from connection
 *		pq_putbytes		- send bytes to connection (not flushed until pq_flush)
 *		pq_flush		- flush pending output
 *
 * message-level I/O (and old-style-COPY-OUT cruft):
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *		pq_startcopyout - inform libpq that a COPY OUT transfer is beginning
 *		pq_endcopyout	- end a COPY OUT transfer
 *
 *------------------------
 */

#include "pg_config.h"

#include <signal.h>
#include <fcntl.h>
#include <grp.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <netdb.h>
#include <netinet/in.h>
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>
#ifdef HAVE_UTIME_H
#include <utime.h>
#endif

#include "gtm/gtm_c.h"
#include "gtm/ip.h"
#include "gtm/libpq.h"
#include "gtm/libpq-be.h"
#include "gtm/elog.h"

#define MAXGTMPATH	256

/* Where the Unix socket file is */
static char sock_path[MAXGTMPATH];

extern int         tcp_keepalives_idle;
extern int         tcp_keepalives_interval;
extern int         tcp_keepalives_count;


/*
 * Buffers for low-level I/O
 */

/* Internal functions */
static int	internal_putbytes(Port *myport, const char *s, size_t len);
static int	internal_flush(Port *myport);

/*
 * Streams -- wrapper around Unix socket system calls
 *
 *
 *		Stream functions are used for vanilla TCP connection protocol.
 */


/*
 * StreamServerPort -- open a "listening" port to accept connections.
 *
 * Successfully opened sockets are added to the ListenSocket[] array,
 * at the first position that isn't -1.
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */

int
StreamServerPort(int family, char *hostName, unsigned short portNumber,
				 int ListenSocket[], int MaxListen)
{
	int			fd,
				err;
	int			maxconn;
	int			ret;
	char		portNumberStr[32];
	const char *familyDesc;
	char		familyDescBuf[64];
	char	   *service;
	struct addrinfo *addrs = NULL,
			   *addr;
	struct addrinfo hint;
	int			listen_index = 0;
	int			added = 0;

#if !defined(WIN32) || defined(IPV6_V6ONLY)
	int			one = 1;
#endif

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = family;
	hint.ai_flags = AI_PASSIVE;
	hint.ai_socktype = SOCK_STREAM;

	{
		snprintf(portNumberStr, sizeof(portNumberStr), "%d", portNumber);
		service = portNumberStr;
	}

	ret = pg_getaddrinfo_all(hostName, service, &hint, &addrs);
	if (ret || !addrs)
	{
		if (hostName)
			ereport(LOG,
					(errmsg("could not translate host name \"%s\", service \"%s\" to address: %s",
							hostName, service, gai_strerror(ret))));
		else
			ereport(LOG,
				 (errmsg("could not translate service \"%s\" to address: %s",
						 service, gai_strerror(ret))));
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	for (addr = addrs; addr; addr = addr->ai_next)
	{
		if (!IS_AF_UNIX(family) && IS_AF_UNIX(addr->ai_family))
		{
			/*
			 * Only set up a unix domain socket when they really asked for it.
			 * The service/port is different in that case.
			 */
			continue;
		}

		/* See if there is still room to add 1 more socket. */
		for (; listen_index < MaxListen; listen_index++)
		{
			if (ListenSocket[listen_index] == -1)
				break;
		}
		if (listen_index >= MaxListen)
		{
			ereport(LOG,
					(errmsg("could not bind to all requested addresses: MAXLISTEN (%d) exceeded",
							MaxListen)));
			break;
		}

		/* set up family name for possible error messages */
		switch (addr->ai_family)
		{
			case AF_INET:
				familyDesc = "IPv4";
				break;
#ifdef HAVE_IPV6
			case AF_INET6:
				familyDesc = "IPv6";
				break;
#endif
			default:
				snprintf(familyDescBuf, sizeof(familyDescBuf),
						 "unrecognized address family %d",
						 addr->ai_family);
				familyDesc = familyDescBuf;
				break;
		}

		if ((fd = socket(addr->ai_family, SOCK_STREAM, 0)) < 0)
		{
			ereport(LOG,
					(EACCES,
			/* translator: %s is IPv4, IPv6, or Unix */
					 errmsg("could not create %s socket: %m",
							familyDesc)));
			continue;
		}

#ifndef WIN32

		/*
		 * Without the SO_REUSEADDR flag, a new postmaster can't be started
		 * right away after a stop or crash, giving "address already in use"
		 * error on TCP ports.
		 *
		 * On win32, however, this behavior only happens if the
		 * SO_EXLUSIVEADDRUSE is set. With SO_REUSEADDR, win32 allows multiple
		 * servers to listen on the same address, resulting in unpredictable
		 * behavior. With no flags at all, win32 behaves as Unix with
		 * SO_REUSEADDR.
		 */
		if (!IS_AF_UNIX(addr->ai_family))
		{
			if ((setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
							(char *) &one, sizeof(one))) == -1)
			{
				ereport(LOG,
						(EACCES,
						 errmsg("setsockopt(SO_REUSEADDR) failed: %m")));
				close(fd);
				continue;
			}
		}
#endif

#ifdef IPV6_V6ONLY
		if (addr->ai_family == AF_INET6)
		{
			if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY,
						   (char *) &one, sizeof(one)) == -1)
			{
				ereport(LOG,
						(EACCES,
						 errmsg("setsockopt(IPV6_V6ONLY) failed: %m")));
				close(fd);
				continue;
			}
		}
#endif

		/*
		 * Note: This might fail on some OS's, like Linux older than
		 * 2.4.21-pre3, that don't have the IPV6_V6ONLY socket option, and map
		 * ipv4 addresses to ipv6.	It will show ::ffff:ipv4 for all ipv4
		 * connections.
		 */
		err = bind(fd, addr->ai_addr, addr->ai_addrlen);
		if (err < 0)
		{
			ereport(LOG,
					(EACCES,
			/* translator: %s is IPv4, IPv6, or Unix */
					 errmsg("could not bind %s socket: %m",
							familyDesc),
					 (IS_AF_UNIX(addr->ai_family)) ?
				  errhint("Is another postmaster already running on port %d?"
						  " If not, remove socket file \"%s\" and retry.",
						  (int) portNumber, sock_path) :
				  errhint("Is another postmaster already running on port %d?"
						  " If not, wait a few seconds and retry.",
						  (int) portNumber)));
			close(fd);
			continue;
		}

#define GTM_MAX_CONNECTIONS		1024

		/*
		 * Select appropriate accept-queue length limit.  PG_SOMAXCONN is only
		 * intended to provide a clamp on the request on platforms where an
		 * overly large request provokes a kernel error (are there any?).
		 */
		maxconn = GTM_MAX_CONNECTIONS * 2;

		err = listen(fd, maxconn);
		if (err < 0)
		{
			ereport(LOG,
					(EACCES,
			/* translator: %s is IPv4, IPv6, or Unix */
					 errmsg("could not listen on %s socket: %m",
							familyDesc)));
			close(fd);
			continue;
		}
		ListenSocket[listen_index] = fd;
		added++;
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);

	if (!added)
		return STATUS_ERROR;

	return STATUS_OK;
}


/*
 * StreamConnection -- create a new connection with client using
 *		server port.  Set port->sock to the FD of the new connection.
 *
 * ASSUME: that this doesn't need to be non-blocking because
 *		the Postmaster uses select() to tell when the server master
 *		socket is ready for accept().
 *
 * RETURNS: STATUS_OK or STATUS_ERROR
 */
int
StreamConnection(int server_fd, Port *port)
{
	/* accept connection and fill in the client (remote) address */
	port->raddr.salen = sizeof(port->raddr.addr);
	if ((port->sock = accept(server_fd,
							 (struct sockaddr *) & port->raddr.addr,
							 (socklen_t *)&port->raddr.salen)) < 0)
	{
		ereport(LOG,
				(EACCES,
				 errmsg("could not accept new connection: %m")));

		/*
		 * If accept() fails then postmaster.c will still see the server
		 * socket as read-ready, and will immediately try again.  To avoid
		 * uselessly sucking lots of CPU, delay a bit before trying again.
		 * (The most likely reason for failure is being out of kernel file
		 * table slots; we can do little except hope some will get freed up.)
		 */
	/*	pg_usleep(100000L);	 */	/* wait 0.1 sec */
		return STATUS_ERROR;
	}

#ifdef SCO_ACCEPT_BUG

	/*
	 * UnixWare 7+ and OpenServer 5.0.4 are known to have this bug, but it
	 * shouldn't hurt to catch it for all versions of those platforms.
	 */
	if (port->raddr.addr.ss_family == 0)
		port->raddr.addr.ss_family = AF_UNIX;
#endif

	/* fill in the server (local) address */
	port->laddr.salen = sizeof(port->laddr.addr);
	if (getsockname(port->sock,
					(struct sockaddr *) & port->laddr.addr,
					(socklen_t *)&port->laddr.salen) < 0)
	{
		elog(LOG, "getsockname() failed: %m");
		return STATUS_ERROR;
	}

	/* select NODELAY and KEEPALIVE options if it's a TCP connection */
	if (!IS_AF_UNIX(port->laddr.addr.ss_family))
	{
		int			on;

#ifdef	TCP_NODELAY
		on = 1;
		if (setsockopt(port->sock, IPPROTO_TCP, TCP_NODELAY,
					   (char *) &on, sizeof(on)) < 0)
		{
			elog(LOG, "setsockopt(TCP_NODELAY) failed: %m");
			return STATUS_ERROR;
		}
#endif
		on = 1;
		if (setsockopt(port->sock, SOL_SOCKET, SO_KEEPALIVE,
					   (char *) &on, sizeof(on)) < 0)
		{
			elog(LOG, "setsockopt(SO_KEEPALIVE) failed: %m");
			return STATUS_ERROR;
		}

		/*
		 * Also apply the current keepalive parameters.  If we fail to set a
		 * parameter, don't error out, because these aren't universally
		 * supported.  (Note: you might think we need to reset the GUC
		 * variables to 0 in such a case, but it's not necessary because the
		 * show hooks for these variables report the truth anyway.)
		 */
		(void) pq_setkeepalivesidle(tcp_keepalives_idle, port);
		(void) pq_setkeepalivesinterval(tcp_keepalives_interval, port);
		(void) pq_setkeepalivescount(tcp_keepalives_count, port);
	}

	return STATUS_OK;
}

/*
 * StreamClose -- close a client/backend connection
 *
 * NOTE: this is NOT used to terminate a session; it is just used to release
 * the file descriptor in a process that should no longer have the socket
 * open.  (For example, the postmaster calls this after passing ownership
 * of the connection to a child process.)  It is expected that someone else
 * still has the socket open.  So, we only want to close the descriptor,
 * we do NOT want to send anything to the far end.
 */
void
StreamClose(int sock)
{
	close(sock);
}

/*
 * TouchSocketFile -- mark socket file as recently accessed
 *
 * This routine should be called every so often to ensure that the socket
 * file has a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves it from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
void
TouchSocketFile(void)
{
	/* Do nothing if we did not create a socket... */
	if (sock_path[0] != '\0')
	{
		/*
		 * utime() is POSIX standard, utimes() is a common alternative. If we
		 * have neither, there's no way to affect the mod or access time of
		 * the socket :-(
		 *
		 * In either path, we ignore errors; there's no point in complaining.
		 */
#ifdef HAVE_UTIME
		utime(sock_path, NULL);
#else							/* !HAVE_UTIME */
#ifdef HAVE_UTIMES
		utimes(sock_path, NULL);
#endif   /* HAVE_UTIMES */
#endif   /* HAVE_UTIME */
	}
}


/* --------------------------------
 * Low-level I/O routines begin here.
 *
 * These routines communicate with a frontend client across a connection
 * already established by the preceding routines.
 * --------------------------------
 */


/* --------------------------------
 *		pq_recvbuf - load some bytes into the input buffer
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pq_recvbuf(Port *myport)
{
	if (myport->PqRecvPointer > 0)
	{
		if (myport->PqRecvLength > myport->PqRecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(myport->PqRecvBuffer, myport->PqRecvBuffer + myport->PqRecvPointer,
					myport->PqRecvLength - myport->PqRecvPointer);
			myport->PqRecvLength -= myport->PqRecvPointer;
			myport->PqRecvPointer = 0;
		}
		else
			myport->PqRecvLength = myport->PqRecvPointer = 0;
	}

	/* Can fill buffer from myport->PqRecvLength and upwards */
	for (;;)
	{
		int			r;

		r = recv(myport->sock, myport->PqRecvBuffer + myport->PqRecvLength,
						PQ_BUFFER_SIZE - myport->PqRecvLength, 0);
		myport->last_call = GTM_LastCall_RECV;

		if (r < 0)
		{
			myport->last_errno = errno;
			if (errno == EINTR)
				continue;		/* Ok if interrupted */

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 */
			ereport(COMMERROR,
					(EACCES,
					 errmsg("could not receive data from client: %m")));
			return EOF;
		}
		else
			myport->last_errno = 0;
		if (r == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return EOF;
		}
		/* r contains number of bytes read, so just incr length */
		myport->PqRecvLength += r;
		return 0;
	}
}

/* --------------------------------
 *		pq_getbyte	- get a single byte from connection, or return EOF
 * --------------------------------
 */
int
pq_getbyte(Port *myport)
{
	while (myport->PqRecvPointer >= myport->PqRecvLength)
	{
		if (pq_recvbuf(myport))		/* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) myport->PqRecvBuffer[myport->PqRecvPointer++];
}

/* --------------------------------
 *		pq_peekbyte		- peek at next byte from connection
 *
 *	 Same as pq_getbyte() except we don't advance the pointer.
 * --------------------------------
 */
int
pq_peekbyte(Port *myport)
{
	while (myport->PqRecvPointer >= myport->PqRecvLength)
	{
		if (pq_recvbuf(myport))		/* If nothing in buffer, then recv some */
			return EOF;			/* Failed to recv data */
	}
	return (unsigned char) myport->PqRecvBuffer[myport->PqRecvPointer];
}

/* --------------------------------
 *		pq_getbytes		- get a known number of bytes from connection
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getbytes(Port *myport, char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (myport->PqRecvPointer >= myport->PqRecvLength)
		{
			if (pq_recvbuf(myport))	/* If nothing in buffer, then recv some */
				return EOF;		/* Failed to recv data */
		}
		amount = myport->PqRecvLength - myport->PqRecvPointer;
		if (amount > len)
			amount = len;
		memcpy(s, myport->PqRecvBuffer + myport->PqRecvPointer, amount);
		myport->PqRecvPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}

#ifdef NOT_USED
/* --------------------------------
 *		pq_discardbytes		- throw away a known number of bytes
 *
 *		same as pq_getbytes except we do not copy the data to anyplace.
 *		this is used for resynchronizing after read errors.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
static int
pq_discardbytes(Port *myport, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (myport->PqRecvPointer >= myport->PqRecvLength)
		{
			if (pq_recvbuf(myport))	/* If nothing in buffer, then recv some */
				return EOF;		/* Failed to recv data */
		}
		amount = myport->PqRecvLength - myport->PqRecvPointer;
		if (amount > len)
			amount = len;
		myport->PqRecvPointer += amount;
		len -= amount;
	}
	return 0;
}
#endif /* NOT_USED */

/* --------------------------------
 *		pq_getstring	- get a null terminated string from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *
 *		This is used only for dealing with old-protocol clients.  The idea
 *		is to produce a StringInfo that looks the same as we would get from
 *		pq_getmessage() with a newer client; we will then process it with
 *		pq_getmsgstring.  Therefore, no character set conversion is done here,
 *		even though this is presumably useful only for text.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getstring(Port *myport, StringInfo s)
{
	int			i;

	resetStringInfo(s);

	/* Read until we get the terminating '\0' */
	for (;;)
	{
		while (myport->PqRecvPointer >= myport->PqRecvLength)
		{
			if (pq_recvbuf(myport))	/* If nothing in buffer, then recv some */
				return EOF;		/* Failed to recv data */
		}

		for (i = myport->PqRecvPointer; i < myport->PqRecvLength; i++)
		{
			if (myport->PqRecvBuffer[i] == '\0')
			{
				/* include the '\0' in the copy */
				appendBinaryStringInfo(s, myport->PqRecvBuffer + myport->PqRecvPointer,
									   i - myport->PqRecvPointer + 1);
				myport->PqRecvPointer = i + 1;	/* advance past \0 */
				return 0;
			}
		}

		/* If we're here we haven't got the \0 in the buffer yet. */
		appendBinaryStringInfo(s, myport->PqRecvBuffer + myport->PqRecvPointer,
							   myport->PqRecvLength - myport->PqRecvPointer);
		myport->PqRecvPointer = myport->PqRecvLength;
	}
}


/* --------------------------------
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		If maxlen is not zero, it is an upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_getmessage(Port *myport, StringInfo s, int maxlen)
{
	int32		len;

	resetStringInfo(s);

	/* Read message length word */
	if (pq_getbytes(myport, (char *) &len, 4) == EOF)
	{
		ereport(COMMERROR,
				(EPROTO,
				 errmsg("unexpected EOF within message length word")));
		return EOF;
	}

	len = ntohl(len);

	if (len < 4 ||
		(maxlen > 0 && len > maxlen))
	{
		ereport(COMMERROR,
				(EPROTO,
				 errmsg("invalid message length")));
		return EOF;
	}

	len -= 4;					/* discount length itself */

	if (len > 0)
	{
		/*
		 * Allocate space for message.	If we run out of room (ridiculously
		 * large message), we will elog(ERROR), but we want to discard the
		 * message body so as not to lose communication sync.
		 */
		enlargeStringInfo(s, len);

		/* And grab the message */
		if (pq_getbytes(myport, s->data, len) == EOF)
		{
			ereport(COMMERROR,
					(EPROTO,
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
 *		pq_putbytes		- send bytes to connection (not flushed until pq_flush)
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_putbytes(Port *myport, const char *s, size_t len)
{
	int			res;

	res = internal_putbytes(myport, s, len);
	return res;
}

static int
internal_putbytes(Port *myport, const char *s, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		/* If buffer is full, then flush it out */
		if (myport->PqSendPointer >= PQ_BUFFER_SIZE)
			if (internal_flush(myport))
				return EOF;
		amount = PQ_BUFFER_SIZE - myport->PqSendPointer;
		if (amount > len)
			amount = len;
		memcpy(myport->PqSendBuffer + myport->PqSendPointer, s, amount);
		myport->PqSendPointer += amount;
		s += amount;
		len -= amount;
	}
	return 0;
}

/* --------------------------------
 *		pq_flush		- flush pending output
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_flush(Port *myport)
{
	int			res;

	/* No-op if reentrant call */
	res = internal_flush(myport);
	return res;
}

static int
internal_flush(Port *myport)
{
	static int	last_reported_send_errno = 0;

	char	   *bufptr = myport->PqSendBuffer;
	char	   *bufend = myport->PqSendBuffer + myport->PqSendPointer;

	while (bufptr < bufend)
	{
		int			r;

		r = send(myport->sock, bufptr, bufend - bufptr, 0);
		myport->last_call = GTM_LastCall_SEND;

		if (r <= 0)
		{
			myport->last_errno = errno;
			if (errno == EINTR)
				continue;		/* Ok if we were interrupted */

			/*
			 * Careful: an ereport() that tries to write to the client would
			 * cause recursion to here, leading to stack overflow and core
			 * dump!  This message must go *only* to the postmaster log.
			 *
			 * If a client disconnects while we're in the midst of output, we
			 * might write quite a bit of data before we get to a safe query
			 * abort point.  So, suppress duplicate log messages.
			 */
			if (errno != last_reported_send_errno)
			{
				last_reported_send_errno = errno;
				ereport(COMMERROR,
						(EACCES,
						 errmsg("could not send data to client: %m")));
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon.
			 */
			myport->PqSendPointer = 0;
			return EOF;
		}
		else
			myport->last_errno = 0;

		last_reported_send_errno = 0;	/* reset after any successful send */
		bufptr += r;
	}

	myport->PqSendPointer = 0;
	return 0;
}


/* --------------------------------
 * Message-level I/O routines begin here.
 *
 * These routines understand about the old-style COPY OUT protocol.
 * --------------------------------
 */


/* --------------------------------
 *		pq_putmessage	- send a normal message (suppressed in COPY OUT mode)
 *
 *		If msgtype is not '\0', it is a message type code to place before
 *		the message body.  If msgtype is '\0', then the message has no type
 *		code (this is only valid in pre-3.0 protocols).
 *
 *		len is the length of the message body data at *s.  In protocol 3.0
 *		and later, a message length word (equal to len+4 because it counts
 *		itself too) is inserted by this routine.
 *
 *		All normal messages are suppressed while old-style COPY OUT is in
 *		progress.  (In practice only a few notice messages might get emitted
 *		then; dropping them is annoying, but at least they will still appear
 *		in the postmaster log.)
 *
 *		We also suppress messages generated while pqcomm.c is busy.  This
 *		avoids any possibility of messages being inserted within other
 *		messages.  The only known trouble case arises if SIGQUIT occurs
 *		during a pqcomm.c routine --- quickdie() will try to send a warning
 *		message, and the most reasonable approach seems to be to drop it.
 *
 *		returns 0 if OK, EOF if trouble
 * --------------------------------
 */
int
pq_putmessage(Port *myport, char msgtype, const char *s, size_t len)
{
	uint32		n32;
	if (msgtype)
		if (internal_putbytes(myport, &msgtype, 1))
			goto fail;

	n32 = htonl((uint32) (len + 4));
	if (internal_putbytes(myport, (char *) &n32, 4))
		goto fail;

	if (internal_putbytes(myport, s, len))
		goto fail;
	return 0;

fail:
	return EOF;
}


/*
 * Support for TCP Keepalive parameters
 */

int
pq_getkeepalivesidle(Port *port)
{
#ifdef TCP_KEEPIDLE
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return 0;

	if (port->keepalives_idle != 0)
		return port->keepalives_idle;

	if (port->default_keepalives_idle == 0)
	{
		ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_idle);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE,
					   (char *) &port->default_keepalives_idle,
					   &size) < 0)
		{
			elog(LOG, "getsockopt(TCP_KEEPIDLE) failed: %m");
			port->default_keepalives_idle = -1; /* don't know */
		}
	}

	return port->default_keepalives_idle;
#else
	return 0;
#endif
}

int
pq_setkeepalivesidle(int idle, Port *port)
{
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return STATUS_OK;

#ifdef TCP_KEEPIDLE
	if (idle == port->keepalives_idle)
		return STATUS_OK;

	if (port->default_keepalives_idle <= 0)
	{
		if (pq_getkeepalivesidle(port) < 0)
		{
			if (idle == 0)
				return STATUS_OK;		/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (idle == 0)
		idle = port->default_keepalives_idle;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPIDLE,
				   (char *) &idle, sizeof(idle)) < 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPIDLE) failed: %m");
		return STATUS_ERROR;
	}

	port->keepalives_idle = idle;
#else
	if (idle != 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPIDLE) not supported");
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivesinterval(Port *port)
{
#ifdef TCP_KEEPINTVL
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return 0;

	if (port->keepalives_interval != 0)
		return port->keepalives_interval;

	if (port->default_keepalives_interval == 0)
	{
		ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_interval);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
					   (char *) &port->default_keepalives_interval,
					   &size) < 0)
		{
			elog(LOG, "getsockopt(TCP_KEEPINTVL) failed: %m");
			port->default_keepalives_interval = -1;		/* don't know */
		}
	}

	return port->default_keepalives_interval;
#else
	return 0;
#endif
}

int
pq_setkeepalivesinterval(int interval, Port *port)
{
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return STATUS_OK;

#ifdef TCP_KEEPINTVL
	if (interval == port->keepalives_interval)
		return STATUS_OK;

	if (port->default_keepalives_interval <= 0)
	{
		if (pq_getkeepalivesinterval(port) < 0)
		{
			if (interval == 0)
				return STATUS_OK;		/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (interval == 0)
		interval = port->default_keepalives_interval;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPINTVL,
				   (char *) &interval, sizeof(interval)) < 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPINTVL) failed: %m");
		return STATUS_ERROR;
	}

	port->keepalives_interval = interval;
#else
	if (interval != 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPINTVL) not supported");
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}

int
pq_getkeepalivescount(Port *port)
{
#ifdef TCP_KEEPCNT
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return 0;

	if (port->keepalives_count != 0)
		return port->keepalives_count;

	if (port->default_keepalives_count == 0)
	{
		ACCEPT_TYPE_ARG3 size = sizeof(port->default_keepalives_count);

		if (getsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
					   (char *) &port->default_keepalives_count,
					   &size) < 0)
		{
			elog(LOG, "getsockopt(TCP_KEEPCNT) failed: %m");
			port->default_keepalives_count = -1;		/* don't know */
		}
	}

	return port->default_keepalives_count;
#else
	return 0;
#endif
}

int
pq_setkeepalivescount(int count, Port *port)
{
	if (port == NULL || IS_AF_UNIX(port->laddr.addr.ss_family))
		return STATUS_OK;

#ifdef TCP_KEEPCNT
	if (count == port->keepalives_count)
		return STATUS_OK;

	if (port->default_keepalives_count <= 0)
	{
		if (pq_getkeepalivescount(port) < 0)
		{
			if (count == 0)
				return STATUS_OK;		/* default is set but unknown */
			else
				return STATUS_ERROR;
		}
	}

	if (count == 0)
		count = port->default_keepalives_count;

	if (setsockopt(port->sock, IPPROTO_TCP, TCP_KEEPCNT,
				   (char *) &count, sizeof(count)) < 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPCNT) failed: %m");
		return STATUS_ERROR;
	}

	port->keepalives_count = count;
#else
	if (count != 0)
	{
		elog(LOG, "setsockopt(TCP_KEEPCNT) not supported");
		return STATUS_ERROR;
	}
#endif

	return STATUS_OK;
}
