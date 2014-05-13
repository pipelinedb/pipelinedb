/*-------------------------------------------------------------------------
 *
 * pgxcdesc.c
 *	  rmgr descriptor routines for XC special
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2014 Postgres-XC Development Group
 *
 * IDENTIFICATION
 *	  $$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pgxc/barrier.h"
void
barrier_redo(XLogRecPtr lsn, XLogRecord *record)
{
	/* Nothing to do */
	return;
}

void
barrier_desc(StringInfo buf, uint8 xl_info, char *rec)
{
	Assert(xl_info == XLOG_BARRIER_CREATE);
	appendStringInfo(buf, "BARRIER %s", rec);
}
