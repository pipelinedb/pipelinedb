/*-------------------------------------------------------------------------
 *
 * standby_utils.c
 *	Utilities for GTM standby global values
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	src/gtm/recovery/standby_utils.c
 *
 *-------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/standby_utils.h"
#include "gtm/gtm_lock.h"

/*
 * Variables to interact with GTM active under GTM standby mode.
 */
bool GTM_StandbyMode = false;
char *GTM_ActiveAddress;
int  GTM_ActivePort;

/* For thread safety, values above are protected by a lock */
static GTM_RWLock StandbyLock;

bool
Recovery_IsStandby(void)
{
	bool res;
	GTM_RWLockAcquire(&StandbyLock, GTM_LOCKMODE_READ);
	res = GTM_StandbyMode;
	GTM_RWLockRelease(&StandbyLock);
	return res;
}

void
Recovery_StandbySetStandby(bool standby)
{
	GTM_RWLockAcquire(&StandbyLock, GTM_LOCKMODE_WRITE);
	GTM_StandbyMode = standby;
	GTM_RWLockRelease(&StandbyLock);
}

void
Recovery_StandbySetConnInfo(const char *addr, int port)
{
	GTM_RWLockAcquire(&StandbyLock, GTM_LOCKMODE_WRITE);
	GTM_ActiveAddress = strdup(addr);
	GTM_ActivePort = port;
	GTM_RWLockRelease(&StandbyLock);
}

int
Recovery_StandbyGetActivePort(void)
{
	int res;

	GTM_RWLockAcquire(&StandbyLock, GTM_LOCKMODE_READ);
	res = GTM_ActivePort;
	GTM_RWLockRelease(&StandbyLock);

	return res;
}

char *
Recovery_StandbyGetActiveAddress(void)
{
	char *res;

	GTM_RWLockAcquire(&StandbyLock, GTM_LOCKMODE_READ);
	res = GTM_ActiveAddress;
	GTM_RWLockRelease(&StandbyLock);

	return res;
}

void
Recovery_InitStandbyLock(void)
{
	GTM_RWLockInit(&StandbyLock);
}
