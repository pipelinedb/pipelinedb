/*-------------------------------------------------------------------------
 *
 * replication.h
 *
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/gtm/replication.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef REPLICATION_H
#define REPLICATION_H

#include "c.h"

#include "gtm/libpq.h"
#include "gtm/stringinfo.h"

void ProcessBeginReplicationInitialSyncRequest(Port *, StringInfo);
void ProcessEndReplicationInitialSyncRequest(Port *, StringInfo);

#endif /* REPLICATION_H */
