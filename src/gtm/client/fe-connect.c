/*-------------------------------------------------------------------------
 *
 * fe-connect.c
 *	  functions related to setting up a connection to the backend
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL: pgsql/src/interfaces/libpq/fe-connect.c,v 1.371 2008/12/15 10:28:21 mha Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <unistd.h>

#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

#include <sys/socket.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#include "gtm/gtm_ip.h"
#include "gtm/gtm_msg.h"

/* fall back options if they are not specified by arguments or defined
   by environment variables */
#define DefaultHost		"localhost"

/* ----------
 * Definition of the conninfo parameters and their fallback resources.
 *
 * GTMPQconninfoOptions[] is a constant static array that we use to initialize
 * a dynamically allocated working copy.  All the "val" fields in
 * GTMPQconninfoOptions[] *must* be NULL.	In a working copy, non-null "val"
 * fields point to malloc'd strings that should be freed when the working
 * array is freed (see GTMPQconninfoFree).
 * ----------
 */
static const GTMPQconninfoOption GTMPQconninfoOptions[] = {
	{"connect_timeout", NULL},
	{"host", NULL},
	{"hostaddr", NULL},
	{"port", NULL},
	{"node_name", NULL},
	{"remote_type", NULL},
	{"postmaster", NULL},
	/* Terminating entry --- MUST BE LAST */
	{NULL, NULL}
};

static bool connectOptions1(GTM_Conn *conn, const char *conninfo);
static int	connectGTMStart(GTM_Conn *conn);
static int	connectGTMComplete(GTM_Conn *conn);
static GTM_Conn *makeEmptyGTM_Conn(void);
static void freeGTM_Conn(GTM_Conn *conn);
static void closeGTM_Conn(GTM_Conn *conn);
static GTMPQconninfoOption *conninfo_parse(const char *conninfo,
			   PQExpBuffer errorMessage, bool use_defaults);
static char *conninfo_getval(GTMPQconninfoOption *connOptions,
				const char *keyword);

static int pqPacketSend(GTM_Conn *conn, char packet_type,
			 const void *buf, size_t buf_len);

GTM_Conn *
PQconnectGTM(const char *conninfo)
{
	GTM_Conn	   *conn = PQconnectGTMStart(conninfo);

	if (conn && conn->status != CONNECTION_BAD)
		(void) connectGTMComplete(conn);
	else if (conn != NULL)
	{
		freeGTM_Conn(conn);
		conn = NULL;
	}

	return conn;
}

/*
 *		PQconnectGTMStart
 *
 * Returns a GTM_Conn*.  If NULL is returned, a malloc error has occurred, and
 * you should not attempt to proceed with this connection.	If the status
 * field of the connection returned is CONNECTION_BAD, an error has
 * occurred. In this case you should call GTMPQfinish on the result, (perhaps
 * inspecting the error message first).  Other fields of the structure may not
 * be valid if that occurs.  If the status field is not CONNECTION_BAD, then
 * this stage has succeeded - call GTMPQconnectPoll, using select(2) to see when
 * this is necessary.
 *
 * See GTMPQconnectPoll for more info.
 */
GTM_Conn *
PQconnectGTMStart(const char *conninfo)
{
	GTM_Conn	   *conn;

	/*
	 * Allocate memory for the conn structure
	 */
	conn = makeEmptyGTM_Conn();
	if (conn == NULL)
		return NULL;

	/*
	 * Parse the conninfo string
	 */
	if (!connectOptions1(conn, conninfo))
		return conn;

	/*
	 * Connect to the database
	 */
	if (!connectGTMStart(conn))
	{
		/* Just in case we failed to set it in connectGTMStart */
		conn->status = CONNECTION_BAD;
	}

	return conn;
}

/*
 *		connectOptions1
 *
 * Internal subroutine to set up connection parameters given an already-
 * created GTM_Conn and a conninfo string.
 *
 * Returns true if OK, false if trouble (in which case errorMessage is set
 * and so is conn->status).
 */
static bool
connectOptions1(GTM_Conn *conn, const char *conninfo)
{
	GTMPQconninfoOption *connOptions;
	char	   *tmp;

	/*
	 * Parse the conninfo string
	 */
	connOptions = conninfo_parse(conninfo, &conn->errorMessage, true);
	if (connOptions == NULL)
	{
		conn->status = CONNECTION_BAD;
		/* errorMessage is already set */
		return false;
	}

	/*
	 * Move option values into conn structure
	 *
	 * XXX: probably worth checking strdup() return value here...
	 */
	tmp = conninfo_getval(connOptions, "hostaddr");
	conn->pghostaddr = tmp ? strdup(tmp) : NULL;
	tmp = conninfo_getval(connOptions, "host");
	conn->pghost = tmp ? strdup(tmp) : NULL;
	tmp = conninfo_getval(connOptions, "port");
	conn->pgport = tmp ? strdup(tmp) : NULL;
	tmp = conninfo_getval(connOptions, "connect_timeout");
	conn->connect_timeout = tmp ? strdup(tmp) : NULL;
	tmp = conninfo_getval(connOptions, "node_name");
	conn->gc_node_name = tmp ? strdup(tmp) : NULL;
	tmp = conninfo_getval(connOptions, "postmaster");
	conn->is_postmaster = tmp ? atoi(tmp) : 0;
	tmp = conninfo_getval(connOptions, "remote_type");
	conn->remote_type = tmp ? atoi(tmp) : GTM_NODE_DEFAULT;

	/*
	 * Free the option info - all is in conn now
	 */
	GTMPQconninfoFree(connOptions);

	return true;
}


/* ----------
 * connectNoDelay -
 * Sets the TCP_NODELAY socket option.
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int
connectNoDelay(GTM_Conn *conn)
{
#ifdef	TCP_NODELAY
	int			on = 1;

	if (setsockopt(conn->sock, IPPROTO_TCP, TCP_NODELAY,
				   (char *) &on,
				   sizeof(on)) < 0)
	{
		appendGTMPQExpBuffer(&conn->errorMessage,
			"could not set socket to TCP no delay mode: \n");
		return 0;
	}
#endif

	return 1;
}


/* ----------
 * connectFailureMessage -
 * create a friendly error message on connection failure.
 * ----------
 */
static void
connectFailureMessage(GTM_Conn *conn, int errorno)
{
	{
		appendGTMPQExpBuffer(&conn->errorMessage,
						  "could not connect to server: \n"
					 "\tIs the server running on host \"%s\" and accepting\n"
										"\tTCP/IP connections on port %s?\n",
						  conn->pghostaddr
						  ? conn->pghostaddr
						  : (conn->pghost
							 ? conn->pghost
							 : "???"),
						  conn->pgport);
	}
}


/* ----------
 * connectGTMStart -
 *		Begin the process of making a connection to the backend.
 *
 * Returns 1 if successful, 0 if not.
 * ----------
 */
static int
connectGTMStart(GTM_Conn *conn)
{
	int			portnum = 0;
	char		portstr[128];
	struct addrinfo *addrs = NULL;
	struct addrinfo hint;
	const char *node;
	int			ret;

	if (!conn)
		return 0;

	/* Ensure our buffers are empty */
	conn->inStart = conn->inCursor = conn->inEnd = 0;
	conn->outCount = 0;

	/*
	 * Determine the parameters to pass to gtm_getaddrinfo_all.
	 */

	/* Initialize hint structure */
	MemSet(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = AF_UNSPEC;

	/* Set up port number as a string */
	if (conn->pgport != NULL && conn->pgport[0] != '\0')
		portnum = atoi(conn->pgport);
	snprintf(portstr, sizeof(portstr), "%d", portnum);

	if (conn->pghostaddr != NULL && conn->pghostaddr[0] != '\0')
	{
		/* Using pghostaddr avoids a hostname lookup */
		node = conn->pghostaddr;
		hint.ai_family = AF_UNSPEC;
		hint.ai_flags = AI_NUMERICHOST;
	}
	else if (conn->pghost != NULL && conn->pghost[0] != '\0')
	{
		/* Using pghost, so we have to look-up the hostname */
		node = conn->pghost;
		hint.ai_family = AF_UNSPEC;
	}
	else
	{
		/* Without Unix sockets, default to localhost instead */
		node = "localhost";
		hint.ai_family = AF_UNSPEC;
	}

	/* Use gtm_getaddrinfo_all() to resolve the address */
	ret = gtm_getaddrinfo_all(node, portstr, &hint, &addrs);
	if (ret || !addrs)
	{
		if (node)
			appendGTMPQExpBuffer(&conn->errorMessage,
							  "could not translate host name \"%s\" to address: %s\n",
							  node, gai_strerror(ret));
		else
			appendGTMPQExpBuffer(&conn->errorMessage,
							  "could not translate Unix-domain socket path \"%s\" to address: %s\n",
							  portstr, gai_strerror(ret));
		if (addrs)
			gtm_freeaddrinfo_all(hint.ai_family, addrs);
		goto connect_errReturn;
	}

	/*
	 * Set up to try to connect, with protocol 3.0 as the first attempt.
	 */
	conn->addrlist = addrs;
	conn->addr_cur = addrs;
	conn->addrlist_family = hint.ai_family;
	conn->status = CONNECTION_NEEDED;

	/*
	 * The code for processing CONNECTION_NEEDED state is in GTMPQconnectPoll(),
	 * so that it can easily be re-executed if needed again during the
	 * asynchronous startup process.  However, we must run it once here,
	 * because callers expect a success return from this routine to mean that
	 * we are in PGRES_POLLING_WRITING connection state.
	 */
	if (GTMPQconnectPoll(conn) == PGRES_POLLING_WRITING)
		return 1;

connect_errReturn:
	if (conn->sock >= 0)
	{
		close(conn->sock);
		conn->sock = -1;
	}
	conn->status = CONNECTION_BAD;
	return 0;
}


/*
 *		connectGTMComplete
 *
 * Block and complete a connection.
 *
 * Returns 1 on success, 0 on failure.
 */
static int
connectGTMComplete(GTM_Conn *conn)
{
	GTMClientPollingStatusType flag = PGRES_POLLING_WRITING;
	time_t		finish_time = ((time_t) -1);

	if (conn == NULL || conn->status == CONNECTION_BAD)
		return 0;

	/*
	 * Set up a time limit, if connect_timeout isn't zero.
	 */
	if (conn->connect_timeout != NULL)
	{
		int			timeout = atoi(conn->connect_timeout);

		if (timeout > 0)
		{
			/*
			 * Rounding could cause connection to fail; need at least 2 secs
			 */
			if (timeout < 2)
				timeout = 2;
			/* calculate the finish time based on start + timeout */
			finish_time = time(NULL) + timeout;
		}
	}

	for (;;)
	{
		/*
		 * Wait, if necessary.	Note that the initial state (just after
		 * PQconnectGTMStart) is to wait for the socket to select for writing.
		 */
		switch (flag)
		{
			case PGRES_POLLING_OK:
				/* Reset stored error messages since we now have a working connection */
				resetGTMPQExpBuffer(&conn->errorMessage);
				return 1;		/* success! */

			case PGRES_POLLING_READING:
				if (gtmpqWaitTimed(1, 0, conn, finish_time))
				{
					conn->status = CONNECTION_BAD;
					return 0;
				}
				break;

			case PGRES_POLLING_WRITING:
				if (gtmpqWaitTimed(0, 1, conn, finish_time))
				{
					conn->status = CONNECTION_BAD;
					return 0;
				}
				break;

			default:
				/* Just in case we failed to set it in GTMPQconnectPoll */
				conn->status = CONNECTION_BAD;
				return 0;
		}

		/*
		 * Now try to advance the state machine.
		 */
		flag = GTMPQconnectPoll(conn);
	}
}

/* ----------------
 *		GTMPQconnectPoll
 *
 * Poll an asynchronous connection.
 *
 * Returns a GTMClientPollingStatusType.
 * Before calling this function, use select(2) to determine when data
 * has arrived..
 *
 * You must call GTMPQfinish whether or not this fails.
 */
GTMClientPollingStatusType
GTMPQconnectPoll(GTM_Conn *conn)
{
	if (conn == NULL)
		return PGRES_POLLING_FAILED;

	/* Get the new data */
	switch (conn->status)
	{
			/*
			 * We really shouldn't have been polled in these two cases, but we
			 * can handle it.
			 */
		case CONNECTION_BAD:
			return PGRES_POLLING_FAILED;
		case CONNECTION_OK:
			return PGRES_POLLING_OK;

			/* These are reading states */
		case CONNECTION_AWAITING_RESPONSE:
		case CONNECTION_AUTH_OK:
			{
				/* Load waiting data */
				int			n = gtmpqReadData(conn);

				if (n < 0)
					goto error_return;
				if (n == 0)
					return PGRES_POLLING_READING;

				break;
			}

			/* These are writing states, so we just proceed. */
		case CONNECTION_STARTED:
		case CONNECTION_MADE:
			break;

		case CONNECTION_NEEDED:
			break;

		default:
			appendGTMPQExpBuffer(&conn->errorMessage,
											"invalid connection state, "
								 "probably indicative of memory corruption\n"
											);
			goto error_return;
	}


keep_going:						/* We will come back to here until there is
								 * nothing left to do. */
	switch (conn->status)
	{
		case CONNECTION_NEEDED:
			{
				/*
				 * Try to initiate a connection to one of the addresses
				 * returned by gtm_getaddrinfo_all().  conn->addr_cur is the
				 * next one to try. We fail when we run out of addresses
				 * (reporting the error returned for the *last* alternative,
				 * which may not be what users expect :-().
				 */
				while (conn->addr_cur != NULL)
				{
					struct addrinfo *addr_cur = conn->addr_cur;

					/* Remember current address for possible error msg */
					memcpy(&conn->raddr.addr, addr_cur->ai_addr,
						   addr_cur->ai_addrlen);
					conn->raddr.salen = addr_cur->ai_addrlen;

					/* Open a socket */
					conn->sock = socket(addr_cur->ai_family, SOCK_STREAM, 0);
					if (conn->sock < 0)
					{
						/*
						 * ignore socket() failure if we have more addresses
						 * to try
						 */
						if (addr_cur->ai_next != NULL)
						{
							conn->addr_cur = addr_cur->ai_next;
							continue;
						}
						appendGTMPQExpBuffer(&conn->errorMessage,
							  "could not create socket: \n");
						break;
					}

					/*
					 * Select socket options: no delay of outgoing data for
					 * TCP sockets, nonblock mode, close-on-exec. Fail if any
					 * of this fails.
					 */
					if (!IS_AF_UNIX(addr_cur->ai_family))
					{
						if (!connectNoDelay(conn))
						{
							close(conn->sock);
							conn->sock = -1;
							conn->addr_cur = addr_cur->ai_next;
							continue;
						}
					}

					/*
					 * Start/make connection.  This should not block, since we
					 * are in nonblock mode.  If it does, well, too bad.
					 */
					if (connect(conn->sock, addr_cur->ai_addr,
								addr_cur->ai_addrlen) < 0)
					{
						if (SOCK_ERRNO == EINPROGRESS ||
							SOCK_ERRNO == EWOULDBLOCK ||
							SOCK_ERRNO == EINTR ||
							SOCK_ERRNO == 0)
						{
							/*
							 * This is fine - we're in non-blocking mode, and
							 * the connection is in progress.  Tell caller to
							 * wait for write-ready on socket.
							 */
							conn->status = CONNECTION_STARTED;
							return PGRES_POLLING_WRITING;
						}
						/* otherwise, trouble */
					}
					else
					{
						/*
						 * Hm, we're connected already --- seems the "nonblock
						 * connection" wasn't.  Advance the state machine and
						 * go do the next stuff.
						 */
						conn->status = CONNECTION_STARTED;
						goto keep_going;
					}

					/*
					 * This connection failed --- set up error report, then
					 * close socket (do it this way in case close() affects
					 * the value of errno...).	We will ignore the connect()
					 * failure and keep going if there are more addresses.
					 */
					connectFailureMessage(conn, SOCK_ERRNO);
					if (conn->sock >= 0)
					{
						close(conn->sock);
						conn->sock = -1;
					}

					/*
					 * Try the next address, if any.
					 */
					conn->addr_cur = addr_cur->ai_next;
				}				/* loop over addresses */

				/*
				 * Ooops, no more addresses.  An appropriate error message is
				 * already set up, so just set the right status.
				 */
				goto error_return;
			}

		case CONNECTION_STARTED:
			{
				int			optval;
				size_t optlen = sizeof(optval);

				/*
				 * Write ready, since we've made it here, so the connection
				 * has been made ... or has failed.
				 */

				/*
				 * Now check (using getsockopt) that there is not an error
				 * state waiting for us on the socket.
				 */

				if (getsockopt(conn->sock, SOL_SOCKET, SO_ERROR,
						       (char *) &optval, (socklen_t *)&optlen) == -1)
				{
					appendGTMPQExpBuffer(&conn->errorMessage,
					libpq_gettext("could not get socket error status: \n"));
					goto error_return;
				}
				else if (optval != 0)
				{
					/*
					 * When using a nonblocking connect, we will typically see
					 * connect failures at this point, so provide a friendly
					 * error message.
					 */
					connectFailureMessage(conn, optval);

					/*
					 * If more addresses remain, keep trying, just as in the
					 * case where connect() returned failure immediately.
					 */
					if (conn->addr_cur->ai_next != NULL)
					{
						if (conn->sock >= 0)
						{
							close(conn->sock);
							conn->sock = -1;
						}
						conn->addr_cur = conn->addr_cur->ai_next;
						conn->status = CONNECTION_NEEDED;
						goto keep_going;
					}
					goto error_return;
				}

				/* Fill in the client address */
				conn->laddr.salen = sizeof(conn->laddr.addr);
				if (getsockname(conn->sock,
								(struct sockaddr *) & conn->laddr.addr,
								(socklen_t *)&conn->laddr.salen) < 0)
				{
					appendGTMPQExpBuffer(&conn->errorMessage,
									  "could not get client address from socket:\n");
					goto error_return;
				}

				/*
				 * Make sure we can write before advancing to next step.
				 */
				conn->status = CONNECTION_MADE;
				return PGRES_POLLING_WRITING;
			}

		case CONNECTION_MADE:
			{
				GTM_StartupPacket *sp = (GTM_StartupPacket *)
					malloc(sizeof(GTM_StartupPacket));
				int packetlen = sizeof(GTM_StartupPacket);

				MemSet(sp, 0, sizeof(GTM_StartupPacket));

				/*
				 * Build a startup packet. We tell the GTM server/proxy our
				 * PGXC Node name and whether we are a proxy or not.
				 *
				 * When the connection is made from the proxy, we let the GTM
				 * server know about it so that some special headers are
				 * handled correctly by the server.
				 */
				strncpy(sp->sp_node_name, conn->gc_node_name, SP_NODE_NAME);
				sp->sp_remotetype = conn->remote_type;
				sp->sp_ispostmaster = conn->is_postmaster;

				/*
				 * Send the startup packet.
				 *
				 * Theoretically, this could block, but it really shouldn't
				 * since we only got here if the socket is write-ready.
				 */
				if (pqPacketSend(conn, 'A', (char *)sp, packetlen) != STATUS_OK)
				{
					appendGTMPQExpBuffer(&conn->errorMessage,
						"could not send startup packet: \n");
					goto error_return;
				}

				conn->status = CONNECTION_AWAITING_RESPONSE;

				/* Clean up startup packet */
				free(sp);

				return PGRES_POLLING_READING;
			}

			/*
			 * Handle authentication exchange: wait for postmaster messages
			 * and respond as necessary.
			 */
		case CONNECTION_AWAITING_RESPONSE:
			{
				char		beresp;

				/*
				 * Scan the message from current point (note that if we find
				 * the message is incomplete, we will return without advancing
				 * inStart, and resume here next time).
				 */
				conn->inCursor = conn->inStart;

				/* Read type byte */
				if (gtmpqGetc(&beresp, conn))
				{
					/* We'll come back when there is more data */
					return PGRES_POLLING_READING;
				}

				/*
				 * Validate message type: we expect only an authentication
				 * request or an error here.  Anything else probably means
				 * it's not GTM on the other end at all.
				 */
				if (!(beresp == 'R' || beresp == 'E'))
				{
					appendGTMPQExpBuffer(&conn->errorMessage,
									  "expected authentication request from "
												"server, but received %c\n",
									  beresp);
					goto error_return;
				}


				/* Handle errors. */
				if (beresp == 'E')
				{
					if (gtmpqGets_append(&conn->errorMessage, conn))
					{
						/* We'll come back when there is more data */
						return PGRES_POLLING_READING;
					}
					/* OK, we read the message; mark data consumed */
					conn->inStart = conn->inCursor;
					goto error_return;
				}

				{
					/*
					 * Server sends a dummy message body of size 4 bytes
					 */
					int tmp_int;
					gtmpqGetInt(&tmp_int, 4, conn);
				}

				/*
				 * OK, we successfully read the message; mark data consumed
				 */
				conn->inStart = conn->inCursor;

				/* We are done with authentication exchange */
				conn->status = CONNECTION_AUTH_OK;

				/* Look to see if we have more data yet. */
				goto keep_going;
			}

		case CONNECTION_AUTH_OK:
			{
				/* We can release the address list now. */
				gtm_freeaddrinfo_all(conn->addrlist_family, conn->addrlist);
				conn->addrlist = NULL;
				conn->addr_cur = NULL;

				/* Otherwise, we are open for business! */
				conn->status = CONNECTION_OK;
				return PGRES_POLLING_OK;
			}


		default:
			appendGTMPQExpBuffer(&conn->errorMessage,
											"invalid connection state %c, "
								 "probably indicative of memory corruption\n"
											,
							  conn->status);
			goto error_return;
	}

	/* Unreachable */

error_return:

	/*
	 * We used to close the socket at this point, but that makes it awkward
	 * for those above us if they wish to remove this socket from their own
	 * records (an fd_set for example).  We'll just have this socket closed
	 * when GTMPQfinish is called (which is compulsory even after an error, since
	 * the connection structure must be freed).
	 */
	conn->status = CONNECTION_BAD;
	return PGRES_POLLING_FAILED;
}


/*
 * makeEmptyGTM_Conn
 *	 - create a GTM_Conn data structure with (as yet) no interesting data
 */
static GTM_Conn *
makeEmptyGTM_Conn(void)
{
	GTM_Conn	   *conn;

	conn = (GTM_Conn *) malloc(sizeof(GTM_Conn));
	if (conn == NULL)
		return conn;

	/* Zero all pointers and booleans */
	MemSet(conn, 0, sizeof(GTM_Conn));

	conn->status = CONNECTION_BAD;

	/*
	 * We try to send at least 8K at a time, which is the usual size of pipe
	 * buffers on Unix systems.  That way, when we are sending a large amount
	 * of data, we avoid incurring extra kernel context swaps for partial
	 * bufferloads.  The output buffer is initially made 16K in size, and we
	 * try to dump it after accumulating 8K.
	 *
	 * With the same goal of minimizing context swaps, the input buffer will
	 * be enlarged anytime it has less than 8K free, so we initially allocate
	 * twice that.
	 */
	conn->inBufSize = 16 * 1024;
	conn->inBuffer = (char *) malloc(conn->inBufSize);
	conn->outBufSize = 16 * 1024;
	conn->outBuffer = (char *) malloc(conn->outBufSize);
	initGTMPQExpBuffer(&conn->errorMessage);
	initGTMPQExpBuffer(&conn->workBuffer);

	if (conn->inBuffer == NULL ||
		conn->outBuffer == NULL ||
		PQExpBufferBroken(&conn->errorMessage) ||
		PQExpBufferBroken(&conn->workBuffer))
	{
		/* out of memory already :-( */
		freeGTM_Conn(conn);
		conn = NULL;
	}

	return conn;
}

/*
 * freeGTM_Conn
 *	 - free an idle (closed) GTM_Conn data structure
 *
 * NOTE: this should not overlap any functionality with closeGTM_Conn().
 * Clearing/resetting of transient state belongs there; what we do here is
 * release data that is to be held for the life of the GTM_Conn structure.
 * If a value ought to be cleared/freed during PQreset(), do it there not here.
 */
static void
freeGTM_Conn(GTM_Conn *conn)
{
	if (conn->pghost)
		free(conn->pghost);
	if (conn->pghostaddr)
		free(conn->pghostaddr);
	if (conn->pgport)
		free(conn->pgport);
	if (conn->connect_timeout)
		free(conn->connect_timeout);
	if (conn->gc_node_name)
		free(conn->gc_node_name);
	if (conn->inBuffer)
		free(conn->inBuffer);
	if (conn->outBuffer)
		free(conn->outBuffer);
	termGTMPQExpBuffer(&conn->errorMessage);
	termGTMPQExpBuffer(&conn->workBuffer);

	free(conn);
}

/*
 * closeGTM_Conn
 *	 - properly close a connection to the backend
 *
 * This should reset or release all transient state, but NOT the connection
 * parameters.  On exit, the GTM_Conn should be in condition to start a fresh
 * connection with the same parameters (see PQreset()).
 */
static void
closeGTM_Conn(GTM_Conn *conn)
{
	/*
	 * Note that the protocol doesn't allow us to send Terminate messages
	 * during the startup phase.
	 */
	if (conn->sock >= 0 && conn->status == CONNECTION_OK)
	{
		/*
		 * Try to send "close connection" message to backend. Ignore any
		 * error.
		 *
		 * Force length word for backends may try to read that in a generic
		 * code
		 */
		gtmpqPutMsgStart('X', true, conn);
		gtmpqPutMsgEnd(conn);
		gtmpqFlush(conn);
	}

	/*
	 * Close the connection, reset all transient state, flush I/O buffers.
	 */
	if (conn->sock >= 0)
		close(conn->sock);
	conn->sock = -1;
	conn->status = CONNECTION_BAD;		/* Well, not really _bad_ - just
										 * absent */
	gtm_freeaddrinfo_all(conn->addrlist_family, conn->addrlist);
	conn->addrlist = NULL;
	conn->addr_cur = NULL;
	conn->inStart = conn->inCursor = conn->inEnd = 0;
	conn->outCount = 0;
}

/*
 * GTMPQfinish: properly close a connection to the backend. Also frees
 * the GTM_Conn data structure so it shouldn't be re-used after this.
 */
void
GTMPQfinish(GTM_Conn *conn)
{
	if (conn)
	{
		closeGTM_Conn(conn);
		freeGTM_Conn(conn);
	}
}

/*
 * pqPacketSend() -- convenience routine to send a message to server.
 *
 * pack_type: the single-byte message type code.  (Pass zero for startup
 * packets, which have no message type code.)
 *
 * buf, buf_len: contents of message.  The given length includes only what
 * is in buf; the message type and message length fields are added here.
 *
 * RETURNS: STATUS_ERROR if the write fails, STATUS_OK otherwise.
 * SIDE_EFFECTS: may block.
 *
 * Note: all messages sent with this routine have a length word, whether
 * it's protocol 2.0 or 3.0.
 */
static int
pqPacketSend(GTM_Conn *conn, char pack_type,
			 const void *buf, size_t buf_len)
{
	/* Start the message. */
	if (gtmpqPutMsgStart(pack_type, true, conn))
		return STATUS_ERROR;

	/* Send the message body. */
	if (gtmpqPutnchar(buf, buf_len, conn))
		return STATUS_ERROR;

	/* Finish the message. */
	if (gtmpqPutMsgEnd(conn))
		return STATUS_ERROR;

	/* Flush to ensure backend gets it. */
	if (gtmpqFlush(conn))
		return STATUS_ERROR;

	return STATUS_OK;
}


/*
 *		GTMPQconninfoParse
 *
 * Parse a string like PQconnectGTM() would do and return the
 * resulting connection options array.  NULL is returned on failure.
 * The result contains only options specified directly in the string,
 * not any possible default values.
 *
 * If errmsg isn't NULL, *errmsg is set to NULL on success, or a malloc'd
 * string on failure (use PQfreemem to free it).  In out-of-memory conditions
 * both *errmsg and the result could be NULL.
 *
 * NOTE: the returned array is dynamically allocated and should
 * be freed when no longer needed via GTMPQconninfoFree().
 */
GTMPQconninfoOption *
GTMPQconninfoParse(const char *conninfo, char **errmsg)
{
	PQExpBufferData errorBuf;
	GTMPQconninfoOption *connOptions;

	if (errmsg)
		*errmsg = NULL;			/* default */
	initGTMPQExpBuffer(&errorBuf);
	if (PQExpBufferDataBroken(errorBuf))
		return NULL;			/* out of memory already :-( */
	connOptions = conninfo_parse(conninfo, &errorBuf, false);
	if (connOptions == NULL && errmsg)
		*errmsg = errorBuf.data;
	else
		termGTMPQExpBuffer(&errorBuf);
	return connOptions;
}

/*
 * Conninfo parser routine
 *
 * If successful, a malloc'd GTMPQconninfoOption array is returned.
 * If not successful, NULL is returned and an error message is
 * left in errorMessage.
 * Defaults are supplied (from a service file, environment variables, etc)
 * for unspecified options, but only if use_defaults is TRUE.
 */
static GTMPQconninfoOption *
conninfo_parse(const char *conninfo, PQExpBuffer errorMessage,
			   bool use_defaults)
{
	char	   *pname;
	char	   *pval;
	char	   *buf;
	char	   *cp;
	char	   *cp2;
	GTMPQconninfoOption *options;
	GTMPQconninfoOption *option;

	/* Make a working copy of GTMPQconninfoOptions */
	options = malloc(sizeof(GTMPQconninfoOptions));
	if (options == NULL)
	{
		printfGTMPQExpBuffer(errorMessage,
						  libpq_gettext("out of memory\n"));
		return NULL;
	}
	memcpy(options, GTMPQconninfoOptions, sizeof(GTMPQconninfoOptions));

	/* Need a modifiable copy of the input string */
	if ((buf = strdup(conninfo)) == NULL)
	{
		printfGTMPQExpBuffer(errorMessage,
						  libpq_gettext("out of memory\n"));
		GTMPQconninfoFree(options);
		return NULL;
	}
	cp = buf;

	while (*cp)
	{
		/* Skip blanks before the parameter name */
		if (isspace((unsigned char) *cp))
		{
			cp++;
			continue;
		}

		/* Get the parameter name */
		pname = cp;
		while (*cp)
		{
			if (*cp == '=')
				break;
			if (isspace((unsigned char) *cp))
			{
				*cp++ = '\0';
				while (*cp)
				{
					if (!isspace((unsigned char) *cp))
						break;
					cp++;
				}
				break;
			}
			cp++;
		}

		/* Check that there is a following '=' */
		if (*cp != '=')
		{
			printfGTMPQExpBuffer(errorMessage,
							  libpq_gettext("missing \"=\" after \"%s\" in connection info string\n"),
							  pname);
			GTMPQconninfoFree(options);
			free(buf);
			return NULL;
		}
		*cp++ = '\0';

		/* Skip blanks after the '=' */
		while (*cp)
		{
			if (!isspace((unsigned char) *cp))
				break;
			cp++;
		}

		/* Get the parameter value */
		pval = cp;

		if (*cp != '\'')
		{
			cp2 = pval;
			while (*cp)
			{
				if (isspace((unsigned char) *cp))
				{
					*cp++ = '\0';
					break;
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
				}
				else
					*cp2++ = *cp++;
			}
			*cp2 = '\0';
		}
		else
		{
			cp2 = pval;
			cp++;
			for (;;)
			{
				if (*cp == '\0')
				{
					printfGTMPQExpBuffer(errorMessage,
									  libpq_gettext("unterminated quoted string in connection info string\n"));
					GTMPQconninfoFree(options);
					free(buf);
					return NULL;
				}
				if (*cp == '\\')
				{
					cp++;
					if (*cp != '\0')
						*cp2++ = *cp++;
					continue;
				}
				if (*cp == '\'')
				{
					*cp2 = '\0';
					cp++;
					break;
				}
				*cp2++ = *cp++;
			}
		}

		/*
		 * Now we have the name and the value. Search for the param record.
		 */
		for (option = options; option->keyword != NULL; option++)
		{
			if (strcmp(option->keyword, pname) == 0)
				break;
		}
		if (option->keyword == NULL)
		{
			printfGTMPQExpBuffer(errorMessage,
						 libpq_gettext("invalid connection option \"%s\"\n"),
							  pname);
			GTMPQconninfoFree(options);
			free(buf);
			return NULL;
		}

		/*
		 * Store the value
		 */
		if (option->val)
			free(option->val);
		option->val = strdup(pval);
		if (!option->val)
		{
			printfGTMPQExpBuffer(errorMessage,
							  libpq_gettext("out of memory\n"));
			GTMPQconninfoFree(options);
			free(buf);
			return NULL;
		}
	}

	/* Done with the modifiable input string */
	free(buf);

	return options;
}


static char *
conninfo_getval(GTMPQconninfoOption *connOptions,
				const char *keyword)
{
	GTMPQconninfoOption *option;

	for (option = connOptions; option->keyword != NULL; option++)
	{
		if (strcmp(option->keyword, keyword) == 0)
			return option->val;
	}

	return NULL;
}


void
GTMPQconninfoFree(GTMPQconninfoOption *connOptions)
{
	GTMPQconninfoOption *option;

	if (connOptions == NULL)
		return;

	for (option = connOptions; option->keyword != NULL; option++)
	{
		if (option->val != NULL)
			free(option->val);
	}
	free(connOptions);
}

char *
GTMPQhost(const GTM_Conn *conn)
{
	if (!conn)
		return NULL;
	return conn->pghost;
}

char *
GTMPQport(const GTM_Conn *conn)
{
	if (!conn)
		return NULL;
	return conn->pgport;
}

ConnStatusType
GTMPQstatus(const GTM_Conn *conn)
{
	if (!conn)
		return CONNECTION_BAD;
	return conn->status;
}

int
GTMPQispostmaster(const GTM_Conn *conn)
{
	if (!conn)
		return 0;
	return conn->is_postmaster;
}

char *
GTMPQerrorMessage(const GTM_Conn *conn)
{
	if (!conn)
		return libpq_gettext("connection pointer is NULL\n");

	return conn->errorMessage.data;
}

int
GTMPQsocket(const GTM_Conn *conn)
{
	if (!conn)
		return -1;
	return conn->sock;
}

void
GTMPQtrace(GTM_Conn *conn, FILE *debug_port)
{
	if (conn == NULL)
		return;
	GTMPQuntrace(conn);
	conn->Pfdebug = debug_port;
}

void
GTMPQuntrace(GTM_Conn *conn)
{
	if (conn == NULL)
		return;
	if (conn->Pfdebug)
	{
		fflush(conn->Pfdebug);
		conn->Pfdebug = NULL;
	}
}
