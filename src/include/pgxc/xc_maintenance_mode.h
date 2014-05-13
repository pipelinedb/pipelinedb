/*-------------------------------------------------------------------------
 *
 * pgxc.h
 *		XC maintenance mode stuff
 *
 *
 * Portions Copyright (c) 1996-2011 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/xc_maintenance_mode.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XC_MAINTENANCE_MODE_H
#define XC_MAINTENANCE_MODE_H

#include <unistd.h>
#include "c.h"
#include "postgres.h"
#include "utils/guc.h"

extern bool xc_maintenance_mode;
extern GucContext	currentGucContext;

#endif /* XC_MAINTENANCE_MODE_H */
