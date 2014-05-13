/*-------------------------------------------------------------------------
 *
 * gtm_stats.c
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */
typedef struct GTM_Stats
{
	int 	GTM_RecvMessages[GTM_MAX_MESSAGE_TYPE];
	int 	GTM_SentMessages[GTM_MAX_MESSAGE_TYPE];
	float	GTM_RecvBytes;
	float	GTM_SentBytes;
} GTM_Stats;
