/*-------------------------------------------------------------------------
 *
 * libpq_be.h
 *	  This file contains definitions for structures and externs used
 *	  by the postmaster during client authentication.
 *
 *	  Note that this is backend-internal and is NOT exported to clients.
 *	  Structs that need to be client-visible are in pqcomm.h.
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/libpq/libpq-be.h,v 1.69 2009/01/01 17:23:59 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef LIBPQ_BE_H
#define LIBPQ_BE_H

#include "gtm/pqcomm.h"
#include "gtm/gtm_c.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif

/*
 * This is used by the postmaster in its communication with frontends.	It
 * contains all state information needed during this communication before the
 * backend is run.	The Port structure is kept in malloc'd memory and is
 * still available when a backend is running (see MyProcPort).	The data
 * it points to must also be malloc'd, or else palloc'd in TopMostMemoryContext,
 * so that it survives into GTM_ThreadMain execution!
 */

typedef struct Port
{
	int			sock;				/* File descriptor */
	SockAddr	laddr;				/* local addr (postmaster) */
	SockAddr	raddr;				/* remote addr (client) */
	char		*remote_host;		/* name (or ip addr) of remote host */
	char		*remote_port;		/* text rep of remote port */
	GTM_PortLastCall last_call;		/* Last syscall to this port */
	int			last_errno;			/* Last errno. zero if the last call succeeds */

	GTMProxy_ConnID	conn_id;		/* RequestID of this command */

	GTM_PGXCNodeType	remote_type;	/* Type of remote connection */
	char		*node_name;
	bool		is_postmaster;		/* Is remote a node postmaster? */
#define PQ_BUFFER_SIZE 8192

	char		PqSendBuffer[PQ_BUFFER_SIZE];
	int			PqSendPointer;		/* Next index to store a byte in PqSendBuffer */

	char 		PqRecvBuffer[PQ_BUFFER_SIZE];
	int			PqRecvPointer;		/* Next index to read a byte from PqRecvBuffer */
	int			PqRecvLength;		/* End of data available in PqRecvBuffer */

	/*
	 * TCP keepalive settings.
	 *
	 * default values are 0 if AF_UNIX or not yet known; current values are 0
	 * if AF_UNIX or using the default. Also, -1 in a default value means we
	 * were unable to find out the default (getsockopt failed).
	 */
	int			default_keepalives_idle;
	int			default_keepalives_interval;
	int			default_keepalives_count;
	int			keepalives_idle;
	int			keepalives_interval;
	int			keepalives_count;

	/*
	 * GTM communication error handling.  See libpq-int.h for details.
	 */
	int			connErr_WaitOpt;
	int			connErr_WaitInterval;
	int			connErr_WaitCount;
} Port;

/* TCP keepalives configuration. These are no-ops on an AF_UNIX socket. */

extern int	pq_getkeepalivesidle(Port *port);
extern int	pq_getkeepalivesinterval(Port *port);
extern int	pq_getkeepalivescount(Port *port);

extern int	pq_setkeepalivesidle(int idle, Port *port);
extern int	pq_setkeepalivesinterval(int interval, Port *port);
extern int	pq_setkeepalivescount(int count, Port *port);

#endif   /* LIBPQ_BE_H */
