/*-------------------------------------------------------------------------
 *
 * xc_maintenance_mode.c
 *		XC maintenance mode parameters
 *
 *
 * Portions Copyright (c) 1996-2011 PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/backend/pgxc/xc_maintenance_mode/xc_maintenance_mode.c
 *
 *-------------------------------------------------------------------------
 */
#include "pgxc/xc_maintenance_mode.h"

bool xc_maintenance_mode;
GucContext currentGucContext;
