/*-------------------------------------------------------------------------
 *
 * libpq-int.h
 *	  This file contains internal definitions meant to be used only by
 *	  the frontend libpq library, not by applications that call it.
 *
 *	  An application can include this file if it wants to bypass the
 *	  official API defined by libpq-fe.h, but code that does so is much
 *	  more likely to break across PostgreSQL releases than code that uses
 *	  only the official API.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/interfaces/libpq/libpq-int.h,v 1.139 2009/01/01 17:24:03 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#ifndef LIBPQ_INT_H
#define LIBPQ_INT_H

#include <time.h>
#include <sys/types.h>
#include <sys/time.h>
#include "gtm/pqcomm.h"
#include "gtm/pqexpbuffer.h"
#include "gtm/gtm_client.h"
#include "gtm/gtm_c.h"

/*
 * GTM_Conn stores all the state data associated with a single connection
 * to a backend.
 */
struct gtm_conn
{
	/* Saved values of connection options */
	char		*pghost;			/* the machine on which the server is running */
	char		*pghostaddr;		/* the IPv4 address of the machine on which
									 * the server is running, in IPv4
									 * numbers-and-dots notation. Takes precedence
									 * over above. */
	char		*pgport;			/* the server's communication port */
	char		*connect_timeout;	/* connection timeout (numeric string) */
	char		*gc_node_name;		/* PGXC Node Name */
	int			remote_type;		/* is this a connection to/from a proxy ? */
	int			is_postmaster;		/* is this connection to/from a postmaster instance */

	/* Optional file to write trace info to */
	FILE		*Pfdebug;

	/* Status indicators */
	ConnStatusType	status;

	/* Connection data */
	int			sock;			/* Unix FD for socket, -1 if not connected */
	SockAddr	laddr;			/* Local address */
	SockAddr	raddr;			/* Remote address */

	/* Error info for GTM communication */
	GTM_PortLastCall	last_call;	/* Last syscall to this sock. */
	int					last_errno;	/* Last errno.  zero if the last call succeeds. */

	/* Transient state needed while establishing connection */
	struct addrinfo	*addrlist;	/* list of possible backend addresses */
	struct addrinfo	*addr_cur;	/* the one currently being tried */
	int		addrlist_family;	/* needed to know how to free addrlist */

	/* Buffer for data received from backend and not yet processed */
	char	*inBuffer;		/* currently allocated buffer */
	int		inBufSize;		/* allocated size of buffer */
	int		inStart;		/* offset to first unconsumed data in buffer */
	int		inCursor;		/* next byte to tentatively consume */
	int		inEnd;			/* offset to first position after avail data */

	/* Buffer for data not yet sent to backend */
	char	*outBuffer;		/* currently allocated buffer */
	int		outBufSize;		/* allocated size of buffer */
	int		outCount;		/* number of chars waiting in buffer */

	/* State for constructing messages in outBuffer */
	int		outMsgStart;	/* offset to msg start (length word); if -1,
							 * msg has no length word */
	int		outMsgEnd;		/* offset to msg end (so far) */

	/* Buffer for current error message */
	PQExpBufferData	errorMessage;		/* expansible string */

	/* Buffer for receiving various parts of messages */
	PQExpBufferData	workBuffer; /* expansible string */

	/* Pointer to the result of last operation */
	GTM_Result	*result;
};

/* === in fe-misc.c === */

 /*
  * "Get" and "Put" routines return 0 if successful, EOF if not. Note that for
  * Get, EOF merely means the buffer is exhausted, not that there is
  * necessarily any error.
  */
extern int	gtmpqCheckOutBufferSpace(size_t bytes_needed, GTM_Conn *conn);
extern int	gtmpqCheckInBufferSpace(size_t bytes_needed, GTM_Conn *conn);
extern int	gtmpqGetc(char *result, GTM_Conn *conn);
extern int	gtmpqPutc(char c, GTM_Conn *conn);
extern int	gtmpqGets(PQExpBuffer buf, GTM_Conn *conn);
extern int	gtmpqGets_append(PQExpBuffer buf, GTM_Conn *conn);
extern int	gtmpqPuts(const char *s, GTM_Conn *conn);
extern int	gtmpqGetnchar(char *s, size_t len, GTM_Conn *conn);
extern int	gtmpqPutnchar(const char *s, size_t len, GTM_Conn *conn);
extern int	gtmpqGetInt(int *result, size_t bytes, GTM_Conn *conn);
extern int	gtmpqPutInt(int value, size_t bytes, GTM_Conn *conn);
extern int	gtmpqPutMsgStart(char msg_type, bool force_len, GTM_Conn *conn);
extern int	gtmpqPutMsgEnd(GTM_Conn *conn);
extern int	gtmpqReadData(GTM_Conn *conn);
extern int	gtmpqFlush(GTM_Conn *conn);
extern int	gtmpqWait(int forRead, int forWrite, GTM_Conn *conn);
extern int	gtmpqWaitTimed(int forRead, int forWrite, GTM_Conn *conn,
			time_t finish_time);
extern int	gtmpqReadReady(GTM_Conn *conn);
extern int	gtmpqWriteReady(GTM_Conn *conn);

/*
 * In fe-protocol.c
 */
GTM_Result * GTMPQgetResult(GTM_Conn *conn);
extern int gtmpqGetError(GTM_Conn *conn, GTM_Result *result);
void gtmpqFreeResultData(GTM_Result *result, GTM_PGXCNodeType remote_type);

#define SOCK_ERRNO errno
#define SOCK_ERRNO_SET(e) (errno = (e))

#endif   /* LIBPQ_INT_H */
