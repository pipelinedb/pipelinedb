/*-------------------------------------------------------------------------
 *
 * gtm_c.h
 *	  Fundamental C definitions.  This is included by every .c file in
 *	  PostgreSQL (via either postgres.h or postgres_fe.h, as appropriate).
 *
 *	  Note that the definitions here are not intended to be exposed to clients
 *	  of the frontend interface libraries --- so we don't worry much about
 *	  polluting the namespace with lots of stuff...
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/include/c.h,v 1.234 2009/01/01 17:23:55 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef GTM_C_H
#define GTM_C_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#include <sys/types.h>

#include <errno.h>
#include <pthread.h>
#include "c.h"

typedef uint32	GlobalTransactionId;		/* 32-bit global transaction ids */
typedef int16	GTMProxy_ConnID;
typedef uint32	GTM_StrLen;

#define InvalidGTMProxyConnID	-1

typedef pthread_t	GTM_ThreadID;

typedef uint32		GTM_PGXCNodeId;
typedef uint32		GTM_PGXCNodePort;

/* Possible type of nodes for registration */
typedef enum GTM_PGXCNodeType
{
	GTM_NODE_GTM_PROXY = 1,
	GTM_NODE_GTM_PROXY_POSTMASTER = 2,
				/* Used by Proxy to communicate with GTM and not use Proxy headers */
	GTM_NODE_COORDINATOR = 3,
	GTM_NODE_DATANODE = 4,
	GTM_NODE_GTM = 5,
	GTM_NODE_DEFAULT = 6	/* In case nothing is associated to connection */
} GTM_PGXCNodeType;

/*
 * A unique handle to identify transaction at the GTM. It could just be
 * an index in an array or a pointer to the structure
 *
 * Note: If we get rid of BEGIN transaction at the GTM, we can use GXID
 * as a handle because we would never have a transaction state at the
 * GTM without assigned GXID.
 */
typedef int32	GTM_TransactionHandle;

#define InvalidTransactionHandle	-1

/*
 * As GTM and Postgres-XC packages are separated, GTM and XC's API
 * use different type names for timestamps and sequences, but they have to be the same!
 */
typedef int64	GTM_Timestamp;	/* timestamp data is 64-bit based */

typedef int64	GTM_Sequence;	/* a 64-bit sequence */

/* Type of sequence name used when dropping it */
typedef enum GTM_SequenceKeyType
{
	GTM_SEQ_FULL_NAME,	/* Full sequence key */
	GTM_SEQ_DB_NAME		/* DB name part of sequence key */
} GTM_SequenceKeyType;

typedef struct GTM_SequenceKeyData
{
	uint32		gsk_keylen;
	char		*gsk_key;
	GTM_SequenceKeyType	gsk_type; /* see constants below */
} GTM_SequenceKeyData;	/* Counter key, set by the client */

typedef GTM_SequenceKeyData *GTM_SequenceKey;

#define GTM_MAX_SEQKEY_LENGTH		1024

#define InvalidSequenceValue		0x7fffffffffffffffLL
#define SEQVAL_IS_VALID(v)		((v) != InvalidSequenceValue)

#define GTM_MAX_GLOBAL_TRANSACTIONS	4096

typedef enum GTM_IsolationLevel
{
	GTM_ISOLATION_SERIALIZABLE, /* serializable txn */
	GTM_ISOLATION_RC		/* read-committed txn */
} GTM_IsolationLevel;

typedef struct GTM_SnapshotData
{
	GlobalTransactionId		sn_xmin;
	GlobalTransactionId		sn_xmax;
	GlobalTransactionId		sn_recent_global_xmin;
	uint32				sn_xcnt;
	GlobalTransactionId		*sn_xip;
} GTM_SnapshotData;

typedef GTM_SnapshotData *GTM_Snapshot;

/* Define max size of node name in start up packet */
#define SP_NODE_NAME		64

typedef struct GTM_StartupPacket {
	char					sp_node_name[SP_NODE_NAME];
	GTM_PGXCNodeType		sp_remotetype;
	bool					sp_ispostmaster;
} GTM_StartupPacket;

typedef enum GTM_PortLastCall
{
	GTM_LastCall_NONE = 0,
	GTM_LastCall_SEND,
	GTM_LastCall_RECV,
	GTM_LastCall_READ,
	GTM_LastCall_WRITE
} GTM_PortLastCall;

#define InvalidGlobalTransactionId		((GlobalTransactionId) 0)

/*
 * Initial GXID value to start with, when -x option is not specified at the first run.
 *
 * This value is supposed to be safe enough.   If initdb involves huge amount of initial
 * statements/transactions, users should consider to tweak this value with explicit
 * -x option.
 */
#define InitialGXIDValue_Default		((GlobalTransactionId) 10000)

#define GlobalTransactionIdIsValid(gxid) ((GlobalTransactionId) (gxid)) != InvalidGlobalTransactionId

#define _(x) gettext(x)

#endif   /* GTM_C_H */
