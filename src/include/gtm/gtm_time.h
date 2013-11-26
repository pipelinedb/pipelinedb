/*-------------------------------------------------------------------------
 *
 * gtm_time.h
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

#ifndef GTM_TIME_H
#define GTM_TIME_H

/* Julian-date equivalents of Day 0 in Unix and GTM reckoning */
#define UNIX_EPOCH_JDATE	2440588 /* == date2j(1970, 1, 1) */
#define GTM_EPOCH_JDATE		2451545 /* == date2j(2000, 1, 1) */

#define SECS_PER_YEAR	(36525 * 864)	/* avoid floating-point computation */
#define SECS_PER_DAY	86400
#define SECS_PER_HOUR	3600
#define SECS_PER_MINUTE	60
#define MINS_PER_HOUR	60

#ifdef HAVE_INT64_TIMESTAMP
#define USECS_PER_DAY		INT64CONST(86400000000)
#define USECS_PER_HOUR		INT64CONST(3600000000)
#define USECS_PER_MINUTE	INT64CONST(60000000)
#define USECS_PER_SEC		INT64CONST(1000000)
#endif

GTM_Timestamp GTM_TimestampGetCurrent(void);

#endif
