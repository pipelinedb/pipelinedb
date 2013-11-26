/*-------------------------------------------------------------------------
 *
 * pgxc_class.c
 *	routines to support manipulation of the pgxc_class relation
 *
 * Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_class.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "pgxc/locator.h"
#include "utils/array.h"

/*
 * PgxcClassCreate
 *		Create a pgxc_class entry
 */
void
PgxcClassCreate(Oid pcrelid,
				char pclocatortype,
				int pcattnum,
				int pchashalgorithm,
				int pchashbuckets,
				int numnodes,
				Oid *nodes)
{
	Relation	pgxcclassrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_class];
	Datum		values[Natts_pgxc_class];
	int		i;
	oidvector	*nodes_array;

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

	/* Iterate through attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_class; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/* should not happen */
	if (pcrelid == InvalidOid)
	{
		elog(ERROR,"pgxc class relid invalid.");
		return;
	}

	values[Anum_pgxc_class_pcrelid - 1]   = ObjectIdGetDatum(pcrelid);
	values[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	if (pclocatortype == LOCATOR_TYPE_HASH || pclocatortype == LOCATOR_TYPE_MODULO)
	{
		values[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);
		values[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);
		values[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);
	}

	/* Node information */
	values[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

	/* Open the relation for insertion */
	pgxcclassrel = heap_open(PgxcClassRelationId, RowExclusiveLock);

	htup = heap_form_tuple(pgxcclassrel->rd_att, values, nulls);

	(void) simple_heap_insert(pgxcclassrel, htup);

	CatalogUpdateIndexes(pgxcclassrel, htup);

	heap_close(pgxcclassrel, RowExclusiveLock);
}


/*
 * PgxcClassAlter
 *		Modify a pgxc_class entry with given data
 */
void
PgxcClassAlter(Oid pcrelid,
			   char pclocatortype,
			   int pcattnum,
			   int pchashalgorithm,
			   int pchashbuckets,
			   int numnodes,
			   Oid *nodes,
			   PgxcClassAlterType type)
{
	Relation	rel;
	HeapTuple	oldtup, newtup;
	oidvector  *nodes_array;
	Datum		new_record[Natts_pgxc_class];
	bool		new_record_nulls[Natts_pgxc_class];
	bool		new_record_repl[Natts_pgxc_class];

	Assert(OidIsValid(pcrelid));

	rel = heap_open(PgxcClassRelationId, RowExclusiveLock);
	oldtup = SearchSysCacheCopy1(PGXCCLASSRELID,
								 ObjectIdGetDatum(pcrelid));

	if (!HeapTupleIsValid(oldtup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	/* Build array of Oids to be inserted */
	nodes_array = buildoidvector(nodes, numnodes);

	/* Initialize fields */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));

	/* Fields are updated depending on operation type */
	switch (type)
	{
		case PGXC_CLASS_ALTER_DISTRIBUTION:
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			break;
		case PGXC_CLASS_ALTER_NODES:
			new_record_repl[Anum_pgxc_class_nodes - 1] = true;
			break;
		case PGXC_CLASS_ALTER_ALL:
		default:
			new_record_repl[Anum_pgxc_class_pcrelid - 1] = true;
			new_record_repl[Anum_pgxc_class_pclocatortype - 1] = true;
			new_record_repl[Anum_pgxc_class_pcattnum - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashalgorithm - 1] = true;
			new_record_repl[Anum_pgxc_class_pchashbuckets - 1] = true;
			new_record_repl[Anum_pgxc_class_nodes - 1] = true;
	}

	/* Set up new fields */
	/* Relation Oid */
	if (new_record_repl[Anum_pgxc_class_pcrelid - 1])
		new_record[Anum_pgxc_class_pcrelid - 1] = ObjectIdGetDatum(pcrelid);

	/* Locator type */
	if (new_record_repl[Anum_pgxc_class_pclocatortype - 1])
		new_record[Anum_pgxc_class_pclocatortype - 1] = CharGetDatum(pclocatortype);

	/* Attribute number of distribution column */
	if (new_record_repl[Anum_pgxc_class_pcattnum - 1])
		new_record[Anum_pgxc_class_pcattnum - 1] = UInt16GetDatum(pcattnum);

	/* Hash algorithm type */
	if (new_record_repl[Anum_pgxc_class_pchashalgorithm - 1])
		new_record[Anum_pgxc_class_pchashalgorithm - 1] = UInt16GetDatum(pchashalgorithm);

	/* Hash buckets */
	if (new_record_repl[Anum_pgxc_class_pchashbuckets - 1])
		new_record[Anum_pgxc_class_pchashbuckets - 1] = UInt16GetDatum(pchashbuckets);

	/* Node information */
	if (new_record_repl[Anum_pgxc_class_nodes - 1])
		new_record[Anum_pgxc_class_nodes - 1] = PointerGetDatum(nodes_array);

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	simple_heap_update(rel, &oldtup->t_self, newtup);
	CatalogUpdateIndexes(rel, newtup);

	heap_close(rel, RowExclusiveLock);
}

/*
 * RemovePGXCClass():
 *		Remove extended PGXC information
 */
void
RemovePgxcClass(Oid pcrelid)
{
	Relation  relation;
	HeapTuple tup;

	/*
	 * Delete the pgxc_class tuple.
	 */
	relation = heap_open(PgxcClassRelationId, RowExclusiveLock);
	tup = SearchSysCache(PGXCCLASSRELID,
						 ObjectIdGetDatum(pcrelid),
						 0, 0, 0);

	if (!HeapTupleIsValid(tup)) /* should not happen */
		elog(ERROR, "cache lookup failed for pgxc_class %u", pcrelid);

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	heap_close(relation, RowExclusiveLock);
}
