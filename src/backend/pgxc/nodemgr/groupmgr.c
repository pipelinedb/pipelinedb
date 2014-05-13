/*-------------------------------------------------------------------------
 *
 * groupmgr.c
 *	  Routines to support manipulation of the pgxc_group catalog
 *	  This includes support for DDL on objects NODE GROUP
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "catalog/pgxc_group.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "pgxc/groupmgr.h"

/*
 * PgxcGroupCreate
 *
 * Create a PGXC node group
 */
void
PgxcGroupCreate(CreateGroupStmt *stmt)
{
	const char *group_name = stmt->group_name;
	List	   *nodes = stmt->nodes;
	oidvector  *nodes_array;
	Oid		   *inTypes;
	Relation	rel;
	HeapTuple	tup;
	bool		nulls[Natts_pgxc_group];
	Datum		values[Natts_pgxc_group];
	int			member_count = list_length(stmt->nodes);
	ListCell   *lc;
	int			i = 0;

	/* Only a DB administrator can add cluster node groups */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create cluster node groups")));

	/* Check if given group already exists */
	if (OidIsValid(get_pgxc_groupoid(group_name)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Group %s: group already defined",
						group_name)));

	inTypes = (Oid *) palloc(member_count * sizeof(Oid));

	/* Build list of Oids for each node listed */
	foreach(lc, nodes)
	{
		char   *node_name = strVal(lfirst(lc));
		Oid	noid = get_pgxc_nodeoid(node_name);

		if (!OidIsValid(noid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("PGXC Node %s: object not defined",
							node_name)));

		if (get_pgxc_nodetype(noid) != PGXC_NODE_DATANODE)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("PGXC node %s: only Datanodes can be group members",
							node_name)));

		/* OK to pick up Oid of this node */
		inTypes[i] = noid;
		i++;
	}

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(inTypes, member_count);

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_group; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* Insert Data correctly */
	values[Anum_pgxc_group_name - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(group_name));
	values[Anum_pgxc_group_members - 1] = PointerGetDatum(nodes_array);

	/* Open the relation for insertion */
	rel = heap_open(PgxcGroupRelationId, RowExclusiveLock);
	tup = heap_form_tuple(rel->rd_att, values, nulls);

	/* Do the insertion */
	(void) simple_heap_insert(rel, tup);

	CatalogUpdateIndexes(rel, tup);

	heap_close(rel, RowExclusiveLock);
}


/*
 * PgxcNodeGroupsRemove():
 *
 * Remove a PGXC node group
 */
void
PgxcGroupRemove(DropGroupStmt *stmt)
{
	Relation	relation;
	HeapTuple	tup;
	const char *group_name = stmt->group_name;
	Oid			group_oid = get_pgxc_groupoid(group_name);

	/* Only a DB administrator can remove cluster node groups */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to remove cluster node groups")));

	/* Check if group exists */
	if (!OidIsValid(group_oid))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Group %s: group not defined",
						group_name)));

	/* Delete the pgxc_group tuple */
	relation = heap_open(PgxcGroupRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCGROUPOID, ObjectIdGetDatum(group_oid), 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "PGXC Group %s: group not defined", group_name);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
