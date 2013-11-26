/*-------------------------------------------------------------------------
 *
 * barrier.h
 *
 *	  Definitions for the PITR barrier handling
 *
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/barrier.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BARRIER_H
#define BARRIER_H

#include "access/xlog.h"
#include "access/xlogdefs.h"

#define CREATE_BARRIER_PREPARE	'P'
#define CREATE_BARRIER_EXECUTE	'X'
#define CREATE_BARRIER_END		'E'

#define CREATE_BARRIER_PREPARE_DONE	'p'
#define CREATE_BARRIER_EXECUTE_DONE	'x'

typedef struct xl_barrier
{
	char	barrier_id[1];	/* variable length data follows */
} xl_barrier;

#define XLOG_BARRIER_CREATE	0x00

extern void ProcessCreateBarrierPrepare(const char *id);
extern void ProcessCreateBarrierEnd(const char *id);
extern void ProcessCreateBarrierExecute(const char *id);

extern void RequestBarrier(const char *id, char *completionTag);
extern void barrier_redo(XLogRecPtr lsn, XLogRecord *record);
extern void barrier_desc(StringInfo buf, uint8 xl_info, char *rec);
#endif
