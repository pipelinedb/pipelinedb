/*-------------------------------------------------------------------------
 *
 * schemacmds.h
 *	  prototypes for schemacmds.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/schemacmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SCHEMACMDS_H
#define SCHEMACMDS_H

#include "nodes/parsenodes.h"

#ifdef PGXC
extern void CreateSchemaCommand(CreateSchemaStmt *parsetree,
					const char *queryString, bool is_top_level);
#else
extern void CreateSchemaCommand(CreateSchemaStmt *parsetree,
					const char *queryString);
#endif
extern void RemoveSchemaById(Oid schemaOid);

extern void RenameSchema(const char *oldname, const char *newname);
extern void AlterSchemaOwner(const char *name, Oid newOwnerId);
extern void AlterSchemaOwner_oid(Oid schemaOid, Oid newOwnerId);

#endif   /* SCHEMACMDS_H */
