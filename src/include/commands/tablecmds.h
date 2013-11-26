/*-------------------------------------------------------------------------
 *
 * tablecmds.h
 *	  prototypes for tablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/tablecmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLECMDS_H
#define TABLECMDS_H

#include "access/htup.h"
#include "nodes/parsenodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"


extern Oid	DefineRelation(CreateStmt *stmt, char relkind, Oid ownerId);

extern void RemoveRelations(DropStmt *drop);

extern Oid	AlterTableLookupRelation(AlterTableStmt *stmt, LOCKMODE lockmode);

extern void AlterTable(Oid relid, LOCKMODE lockmode, AlterTableStmt *stmt);

extern LOCKMODE AlterTableGetLockLevel(List *cmds);

extern void ATExecChangeOwner(Oid relationOid, Oid newOwnerId, bool recursing, LOCKMODE lockmode);

extern void AlterTableInternal(Oid relid, List *cmds, bool recurse);

extern void AlterTableNamespace(AlterObjectSchemaStmt *stmt);

extern void AlterRelationNamespaceInternal(Relation classRel, Oid relOid,
							   Oid oldNspOid, Oid newNspOid,
							   bool hasDependEntry);

extern void CheckTableNotInUse(Relation rel, const char *stmt);

#ifdef PGXC
extern void ExecuteTruncate(TruncateStmt *stmt, const char *sql_statement);
#else
extern void ExecuteTruncate(TruncateStmt *stmt);
#endif

extern void SetRelationHasSubclass(Oid relationId, bool relhassubclass);

extern void renameatt(RenameStmt *stmt);

extern void RenameConstraint(RenameStmt *stmt);

extern void RenameRelation(RenameStmt *stmt);

extern void RenameRelationInternal(Oid myrelid,
					   const char *newrelname);

extern void find_composite_type_dependencies(Oid typeOid,
								 Relation origRelation,
								 const char *origTypeName);

extern void check_of_type(HeapTuple typetuple);

extern AttrNumber *varattnos_map(TupleDesc olddesc, TupleDesc newdesc);
extern AttrNumber *varattnos_map_schema(TupleDesc old, List *schema);
extern void change_varattnos_of_a_node(Node *node, const AttrNumber *newattno);

extern void register_on_commit_action(Oid relid, OnCommitAction action);
extern void remove_on_commit_action(Oid relid);

extern void PreCommit_on_commit_actions(void);
extern void AtEOXact_on_commit_actions(bool isCommit);
extern void AtEOSubXact_on_commit_actions(bool isCommit,
							  SubTransactionId mySubid,
							  SubTransactionId parentSubid);
#ifdef PGXC
extern bool IsTempTable(Oid relid);
extern bool IsIndexUsingTempTable(Oid relid);
extern bool IsOnCommitActions(void);
extern void DropTableThrowErrorExternal(RangeVar *relation,
										ObjectType removeType,
										bool missing_ok);
#endif

extern void RangeVarCallbackOwnsTable(const RangeVar *relation,
						  Oid relId, Oid oldRelId, void *arg);

#endif   /* TABLECMDS_H */
