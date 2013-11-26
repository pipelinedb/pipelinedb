/*-------------------------------------------------------------------------
 *
 * gtm_conn.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_CONN_H
#define GTM_CONN_H

#include "gtm/libpq-be.h"
#include "gtm/libpq-fe.h"
#include "gtm/libpq-int.h"

struct GTM_ThreadInfo;

typedef struct GTM_ConnectionInfo
{
	/* Port contains all the vital information about this connection */
	Port					*con_port;
	struct GTM_ThreadInfo	*con_thrinfo;
	bool					con_authenticated;

	/* a connection object to the standby */
	GTM_Conn				*standby;
} GTM_ConnectionInfo;

typedef struct GTM_Connections
{
	uint32				gc_conn_count;
	uint32				gc_array_size;
	GTM_ConnectionInfo	*gc_connections;
	GTM_RWLock			gc_lock;
} GTM_Connections;


#endif
