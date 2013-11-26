/*-------------------------------------------------------------------------
 *
 * groupmgr.h
 *	  Routines for PGXC node group management
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/groupmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GROUPMGR_H
#define GROUPMGR_H

#include "nodes/parsenodes.h"

extern void PgxcGroupCreate(CreateGroupStmt *stmt);
extern void PgxcGroupRemove(DropGroupStmt *stmt);

#endif   /* GROUPMGR_H */
