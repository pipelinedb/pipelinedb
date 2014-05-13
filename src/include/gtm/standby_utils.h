/*-------------------------------------------------------------------------
 *
 * standby_utils.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/standby_utils.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STANDBY_UTILS_H
#define STANDBY_UTILS_H

#include "gtm/gtm_lock.h"

bool Recovery_IsStandby(void);
void Recovery_StandbySetStandby(bool standby);
void Recovery_StandbySetConnInfo(const char *addr, int port);
int Recovery_StandbyGetActivePort(void);
char* Recovery_StandbyGetActiveAddress(void);
void Recovery_InitStandbyLock(void);

#endif
