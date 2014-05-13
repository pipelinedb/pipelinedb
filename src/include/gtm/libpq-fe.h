/*-------------------------------------------------------------------------
 *
 * libpq-fe.h
 *	  This file contains definitions for structures and
 *	  externs for functions used by frontend postgres applications.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/interfaces/libpq/libpq-fe.h,v 1.145 2009/01/01 17:24:03 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */

#ifndef LIBPQ_FE_H
#define LIBPQ_FE_H

#ifdef __cplusplus
extern		"C"
{
#endif

#include <stdio.h>

/*
 * postgres_ext.h defines the backend's externally visible types,
 * such as Oid.
 */
#include "gtm/gtm_ext.h"

/*
 * Option flags for PQcopyResult
 */
#define PG_COPYRES_ATTRS          0x01
#define PG_COPYRES_TUPLES         0x02		/* Implies PG_COPYRES_ATTRS */
#define PG_COPYRES_EVENTS         0x04
#define PG_COPYRES_NOTICEHOOKS    0x08

/* Application-visible enum types */

typedef enum
{
	/*
	 * Although it is okay to add to this list, values which become unused
	 * should never be removed, nor should constants be redefined - that would
	 * break compatibility with existing code.
	 */
	CONNECTION_OK,
	CONNECTION_BAD,
	/* Non-blocking mode only below here */

	/*
	 * The existence of these should never be relied upon - they should only
	 * be used for user feedback or similar purposes.
	 */
	CONNECTION_STARTED,				/* Waiting for connection to be made.  */
	CONNECTION_MADE,				/* Connection OK; waiting to send.	   */
	CONNECTION_AWAITING_RESPONSE,	/* Waiting for a response from the
									 * postmaster.		  */
	CONNECTION_AUTH_OK,				/* Received authentication; waiting for
									 * backend startup. */
	CONNECTION_SETENV,				/* Negotiating environment. */
	CONNECTION_SSL_STARTUP,			/* Negotiating SSL. */
	CONNECTION_NEEDED				/* Internal state: connect() needed */
} ConnStatusType;

typedef enum
{
	PGRES_POLLING_FAILED = 0,
	PGRES_POLLING_READING,		/* These two indicate that one may	  */
	PGRES_POLLING_WRITING,		/* use select before polling again.   */
	PGRES_POLLING_OK,
	PGRES_POLLING_ACTIVE		/* unused; keep for awhile for backwards
								 * compatibility */
} GTMClientPollingStatusType;

/* ----------------
 * Structure for the conninfo parameter definitions returned by PQconndefaults
 * or GTMPQconninfoParse.
 *
 * All fields except "val" point at static strings which must not be altered.
 * "val" is either NULL or a malloc'd current-value string.  GTMPQconninfoFree()
 * will release both the val strings and the GTMPQconninfoOption array itself.
 * ----------------
 */
typedef struct _GTMPQconninfoOption
{
	char	   *keyword;		/* The keyword of the option			*/
	char	   *val;			/* Option's current value, or NULL		 */
} GTMPQconninfoOption;

typedef struct gtm_conn GTM_Conn;

/* ----------------
 * Exported functions of libpq
 * ----------------
 */

/* ===	in fe-connect.c === */

/* make a new client connection to the backend */
/* Asynchronous (non-blocking) */
extern GTM_Conn *PQconnectGTMStart(const char *conninfo);
extern GTMClientPollingStatusType GTMPQconnectPoll(GTM_Conn *conn);

/* Synchronous (blocking) */
extern GTM_Conn *PQconnectGTM(const char *conninfo);

/* close the current connection and free the GTM_Conn data structure */
extern void GTMPQfinish(GTM_Conn *conn);

/* parse connection options in same way as PQconnectGTM */
extern GTMPQconninfoOption *GTMPQconninfoParse(const char *conninfo, char **errmsg);

/* free the data structure returned by PQconndefaults() or GTMPQconninfoParse() */
extern void GTMPQconninfoFree(GTMPQconninfoOption *connOptions);

extern char *GTMPQhost(const GTM_Conn *conn);
extern char *GTMPQport(const GTM_Conn *conn);
extern ConnStatusType GTMPQstatus(const GTM_Conn *conn);
extern int GTMPQispostmaster(const GTM_Conn *conn);
extern char *GTMPQerrorMessage(const GTM_Conn *conn);
extern int	GTMPQsocket(const GTM_Conn *conn);

/* Enable/disable tracing */
extern void GTMPQtrace(GTM_Conn *conn, FILE *debug_port);
extern void GTMPQuntrace(GTM_Conn *conn);

/* Force the write buffer to be written (or at least try) */
extern int	PQflush(GTM_Conn *conn);

#define libpq_gettext(x)	x

#ifdef __cplusplus
}
#endif

#endif   /* LIBPQ_FE_H */
