/*-------------------------------------------------------------------------
 *
 * gtm_time.c
 *			Timestamp handling on GTM
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *			$PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm.h"
#include "gtm/gtm_c.h"
#include "gtm/gtm_time.h"
#include <time.h>
#include <sys/time.h>

GTM_Timestamp
GTM_TimestampGetCurrent(void)
{
	struct timeval	tp;
	GTM_Timestamp	result;

	gettimeofday(&tp, NULL);

	result = (GTM_Timestamp) tp.tv_sec -
	((GTM_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY);

#ifdef HAVE_INT64_TIMESTAMP
	result = (result * USECS_PER_SEC) + tp.tv_usec;
#else
	result = result + (tp.tv_usec / 1000000.0);
#endif

	return result;
}
