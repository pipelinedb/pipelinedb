/*-------------------------------------------------------------------------
 *
 * nodemgr.c
 *	  Routines to support manipulation of the pgxc_node catalog
 *	  Support concerns CREATE/ALTER/DROP on NODE object.
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/heapam.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pgxc_node.h"
#include "commands/defrem.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/tqual.h"
#include "pgxc/locator.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "access/htup_details.h"
#include "pg_config.h"

/*
 * How many times should we try to find a unique indetifier
 * in case hash of the node name comes out to be duplicate
 */

#define MAX_TRIES_FOR_NID	200

static Datum generate_node_id(const char *node_name);

/*
 * GUC parameters.
 * Shared memory block can not be resized dynamically, so we should have some
 * limits set at startup time to calculate amount of shared memory to store
 * node table. Nodes can be added to running cluster until that limit is reached
 * if cluster needs grow beyond the configuration value should be changed and
 * cluster restarted.
 */
int				MaxCoords = 16;
int				MaxDataNodes = 16;

/* Global number of nodes. Point to a shared memory block */
static int	   *shmemNumCoords;
static int	   *shmemNumDataNodes;

/* Shared memory tables of node definitions */
NodeDefinition *coDefs;
NodeDefinition *dnDefs;

/*
 * NodeTablesInit
 *	Initializes shared memory tables of Coordinators and Datanodes.
 */
void
NodeTablesShmemInit(void)
{
	bool found;
	/*
	 * Initialize the table of Coordinators: first sizeof(int) bytes are to
	 * store actual number of Coordinators, remaining data in the structure is
	 * array of NodeDefinition that can contain up to MaxCoords entries.
	 * That is a bit weird and probably it would be better have these in
	 * separate structures, but I am unsure about cost of having shmem structure
	 * containing just single integer.
	 */
	shmemNumCoords = ShmemInitStruct("Coordinator Table",
								sizeof(int) +
									sizeof(NodeDefinition) * MaxCoords,
								&found);

	/* Have coDefs pointing right behind shmemNumCoords */
	coDefs = (NodeDefinition *) (shmemNumCoords + 1);

	/* Mark it empty upon creation */
	if (!found)
		*shmemNumCoords = 0;

	/* Same for Datanodes */
	shmemNumDataNodes = ShmemInitStruct("Datanode Table",
								   sizeof(int) +
									   sizeof(NodeDefinition) * MaxDataNodes,
								   &found);

	/* Have coDefs pointing right behind shmemNumDataNodes */
	dnDefs = (NodeDefinition *) (shmemNumDataNodes + 1);

	/* Mark it empty upon creation */
	if (!found)
		*shmemNumDataNodes = 0;
}


/*
 * NodeTablesShmemSize
 *	Get the size of shared memory dedicated to node definitions
 */
Size
NodeTablesShmemSize(void)
{
	Size co_size;
	Size dn_size;

	co_size = mul_size(sizeof(NodeDefinition), MaxCoords);
	co_size = add_size(co_size, sizeof(int));
	dn_size = mul_size(sizeof(NodeDefinition), MaxDataNodes);
	dn_size = add_size(dn_size, sizeof(int));

	return add_size(co_size, dn_size);
}

/*
 * Check list of options and return things filled.
 * This includes check on option values.
 */
static void
check_node_options(const char *node_name, List *options, char **node_host,
			int *node_port, char *node_type,
			bool *is_primary, bool *is_preferred)
{
	ListCell   *option;

	if (!options)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("No options specified")));

	/* Filter options */
	foreach(option, options)
	{
		DefElem    *defel = (DefElem *) lfirst(option);

		if (strcmp(defel->defname, "port") == 0)
		{
			*node_port = defGetTypeLength(defel);

			if (*node_port < 1 || *node_port > 65535)
				ereport(ERROR,
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
						 errmsg("port value is out of range")));
		}
		else if (strcmp(defel->defname, "host") == 0)
		{
			*node_host = defGetString(defel);
		}
		else if (strcmp(defel->defname, "type") == 0)
		{
			char *type_loc;

			type_loc = defGetString(defel);

			if (strcmp(type_loc, "coordinator") != 0 &&
				strcmp(type_loc, "datanode") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("type value is incorrect, specify 'coordinator or 'datanode'")));

			if (strcmp(type_loc, "coordinator") == 0)
				*node_type = PGXC_NODE_COORDINATOR;
			else
				*node_type = PGXC_NODE_DATANODE;
		}
		else if (strcmp(defel->defname, "primary") == 0)
		{
			*is_primary = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "preferred") == 0)
		{
			*is_preferred = defGetBoolean(defel);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("incorrect option: %s", defel->defname)));
		}
	}

	/* A primary node has to be a Datanode */
	if (*is_primary && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a primary node, it has to be a Datanode",
						node_name)));

	/* A preferred node has to be a Datanode */
	if (*is_preferred && *node_type != PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot be a preferred node, it has to be a Datanode",
						node_name)));

	/* Node type check */
	if (*node_type == PGXC_NODE_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: Node type not specified",
						node_name)));
}

/*
 * generate_node_id
 *
 * Given a node name compute its hash to generate the identifier
 * If the hash comes out to be duplicate , try some other values
 * Give up after a few tries
 */
static Datum
generate_node_id(const char *node_name)
{
	Datum		node_id;
	uint32		n;
	bool		inc;
	int		i;

	/* Compute node identifier by computing hash of node name */
	node_id = hash_any((unsigned char *)node_name, strlen(node_name));

	/*
	 * Check if the hash is near the overflow limit, then we will
	 * decrement it , otherwise we will increment
	 */
	inc = true;
	n = DatumGetUInt32(node_id);
	if (n >= UINT_MAX - MAX_TRIES_FOR_NID)
		inc = false;

	/*
	 * Check if the identifier is clashing with an existing one,
	 * and if it is try some other
	 */
	for (i = 0; i < MAX_TRIES_FOR_NID; i++)
	{
		HeapTuple	tup;

		tup = SearchSysCache1(PGXCNODEIDENTIFIER, node_id);
		if (tup == NULL)
			break;

		ReleaseSysCache(tup);

		n = DatumGetUInt32(node_id);
		if (inc)
			n++;
		else
			n--;

		node_id = UInt32GetDatum(n);
	}

	/*
	 * This has really few chances to happen, but inform backend that node
	 * has not been registered correctly in this case.
	 */
	if (i >= MAX_TRIES_FOR_NID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Please choose different node name."),
				 errdetail("Name \"%s\" produces a duplicate identifier node_name",
						   node_name)));

	return node_id;
}

/* --------------------------------
 *  cmp_nodes
 *
 *  Compare the Oids of two XC nodes
 *  to sort them in ascending order by their names
 * --------------------------------
 */
static int
cmp_nodes(const void *p1, const void *p2)
{
	Oid n1 = *((Oid *)p1);
	Oid n2 = *((Oid *)p2);

	if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) < 0)
		return -1;

	if (strcmp(get_pgxc_nodename(n1), get_pgxc_nodename(n2)) == 0)
		return 0;

	return 1;
}


/*
 * PgxcNodeListAndCount
 *
 * Update node definitions in the shared memory tables from the catalog
 */
void
PgxcNodeListAndCount(void)
{
	Relation rel;
	HeapScanDesc scan;
	HeapTuple   tuple;

	LWLockAcquire(NodeTableLock, LW_EXCLUSIVE);

	*shmemNumCoords = 0;
	*shmemNumDataNodes = 0;

	/*
	 * Node information initialization is made in one scan:
	 * 1) Scan pgxc_node catalog to find the number of nodes for
	 *	each node type and make proper allocations
	 * 2) Then extract the node Oid
	 * 3) Complete primary/preferred node information
	 */
	rel = heap_open(PgxcNodeRelationId, AccessShareLock);
	scan = heap_beginscan(rel, SnapshotNow, 0, NULL);
	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pgxc_node  nodeForm = (Form_pgxc_node) GETSTRUCT(tuple);
		NodeDefinition *node;

		/* Take definition for given node type */
		switch (nodeForm->node_type)
		{
			case PGXC_NODE_COORDINATOR:
				node = &coDefs[(*shmemNumCoords)++];
				break;
			case PGXC_NODE_DATANODE:
			default:
				node = &dnDefs[(*shmemNumDataNodes)++];
				break;
		}

		/* Populate the definition */
		node->nodeoid = HeapTupleGetOid(tuple);
		memcpy(&node->nodename, &nodeForm->node_name, NAMEDATALEN);
		memcpy(&node->nodehost, &nodeForm->node_host, NAMEDATALEN);
		node->nodeport = nodeForm->node_port;
		node->nodeisprimary = nodeForm->nodeis_primary;
		node->nodeispreferred = nodeForm->nodeis_preferred;
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	/* Finally sort the lists */
	if (*shmemNumCoords > 1)
		qsort(coDefs, *shmemNumCoords, sizeof(NodeDefinition), cmp_nodes);
	if (*shmemNumDataNodes > 1)
		qsort(dnDefs, *shmemNumDataNodes, sizeof(NodeDefinition), cmp_nodes);

	LWLockRelease(NodeTableLock);
}


/*
 * PgxcNodeGetIds
 *
 * List into palloc'ed arrays Oids of Coordinators and Datanodes currently
 * presented in the node table, as well as number of Coordinators and Datanodes.
 * Any parameter may be NULL if caller is not interested in receiving
 * appropriate results. Preferred and primary node information can be updated
 * in session if requested.
 */
void
PgxcNodeGetOids(Oid **coOids, Oid **dnOids,
				int *num_coords, int *num_dns, bool update_preferred)
{
	LWLockAcquire(NodeTableLock, LW_SHARED);

	if (num_coords)
		*num_coords = *shmemNumCoords;
	if (num_dns)
		*num_dns = *shmemNumDataNodes;

	if (coOids)
	{
		int i;

		*coOids = (Oid *) palloc(*shmemNumCoords * sizeof(Oid));
		for (i = 0; i < *shmemNumCoords; i++)
			(*coOids)[i] = coDefs[i].nodeoid;
	}

	if (dnOids)
	{
		int i;

		*dnOids = (Oid *) palloc(*shmemNumDataNodes * sizeof(Oid));
		for (i = 0; i < *shmemNumDataNodes; i++)
			(*dnOids)[i] = dnDefs[i].nodeoid;
	}

	/* Update also preferred and primary node informations if requested */
	if (update_preferred)
	{
		int i;

		/* Initialize primary and preferred node information */
		primary_data_node = InvalidOid;
		num_preferred_data_nodes = 0;

		for (i = 0; i < *shmemNumDataNodes; i++)
		{
			if (dnDefs[i].nodeisprimary)
				primary_data_node = dnDefs[i].nodeoid;

			if (dnDefs[i].nodeispreferred)
			{
				preferred_data_node[num_preferred_data_nodes] = dnDefs[i].nodeoid;
				num_preferred_data_nodes++;
			}
		}
	}

	LWLockRelease(NodeTableLock);
}


/*
 * Find node definition in the shared memory node table.
 * The structure is a copy palloc'ed in current memory context.
 */
NodeDefinition *
PgxcNodeGetDefinition(Oid node)
{
	NodeDefinition *result = NULL;
	int				i;

	LWLockAcquire(NodeTableLock, LW_SHARED);

	/* search through the Datanodes first */
	for (i = 0; i < *shmemNumDataNodes; i++)
	{
		if (dnDefs[i].nodeoid == node)
		{
			result = (NodeDefinition *) palloc(sizeof(NodeDefinition));

			memcpy(result, dnDefs + i, sizeof(NodeDefinition));

			LWLockRelease(NodeTableLock);

			return result;
		}
	}

	/* if not found, search through the Coordinators */
	for (i = 0; i < *shmemNumCoords; i++)
	{
		if (coDefs[i].nodeoid == node)
		{
			result = (NodeDefinition *) palloc(sizeof(NodeDefinition));

			memcpy(result, coDefs + i, sizeof(NodeDefinition));

			LWLockRelease(NodeTableLock);

			return result;
		}
	}

	/* not found, return NULL */
	LWLockRelease(NodeTableLock);
	return NULL;
}


/*
 * PgxcNodeCreate
 *
 * Add a PGXC node
 */
void
PgxcNodeCreate(CreateNodeStmt *stmt)
{
	Relation	pgxcnodesrel;
	HeapTuple	htup;
	bool		nulls[Natts_pgxc_node];
	Datum		values[Natts_pgxc_node];
	const char *node_name = stmt->node_name;
	int		i;
	/* Options with default values */
	char	   *node_host = NULL;
	char		node_type = PGXC_NODE_NONE;
	int			node_port = 0;
	bool		is_primary = false;
	bool		is_preferred = false;
	Datum		node_id;
	Oid			nodeOid;

	/* Only a DB administrator can add nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create cluster nodes")));

	/* Check that node name is node in use */
	if (OidIsValid(get_pgxc_nodeoid(node_name)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("PGXC Node %s: object already defined",
						node_name)));

	/* Check length of node name */
	if (strlen(node_name) > PGXC_NODENAME_LENGTH)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
				 errmsg("Node name \"%s\" is too long",
						node_name)));

	/* Filter options */
	check_node_options(node_name, stmt->options, &node_host,
				&node_port, &node_type,
				&is_primary, &is_preferred);

	/* Compute node identifier */
	node_id = generate_node_id(node_name);

	/*
	 * Check that this node is not created as a primary if one already
	 * exists.
	 */
	if (is_primary && OidIsValid(primary_data_node))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: two nodes cannot be primary",
						node_name)));

	/*
	 * Then assign default values if necessary
	 * First for port.
	 */
	if (node_port == 0)
	{
		node_port = DEF_PGPORT;
		elog(NOTICE, "PGXC node %s: Applying default port value: %d",
			 node_name, node_port);
	}

	/* Then apply default value for host */
	if (!node_host)
	{
		node_host = strdup("localhost");
		elog(NOTICE, "PGXC node %s: Applying default host value: %s",
			 node_name, node_host);
	}

	/* Iterate through all attributes initializing nulls and values */
	for (i = 0; i < Natts_pgxc_node; i++)
	{
		nulls[i]  = false;
		values[i] = (Datum) 0;
	}

	/*
	 * Open the relation for insertion
	 * This is necessary to generate a unique Oid for the new node
	 * There could be a relation race here if a similar Oid
	 * being created before the heap is inserted.
	 */
	pgxcnodesrel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Build entry tuple */
	values[Anum_pgxc_node_name - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_name));
	values[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
	values[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
	values[Anum_pgxc_node_host - 1] = DirectFunctionCall1(namein, CStringGetDatum(node_host));
	values[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
	values[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
	values[Anum_pgxc_node_id - 1] = node_id;

	htup = heap_form_tuple(pgxcnodesrel->rd_att, values, nulls);

	/* Insert tuple in catalog */
	nodeOid = simple_heap_insert(pgxcnodesrel, htup);

	CatalogUpdateIndexes(pgxcnodesrel, htup);

	heap_close(pgxcnodesrel, RowExclusiveLock);

	if (is_primary)
		primary_data_node = nodeOid;
}

/*
 * PgxcNodeAlter
 *
 * Alter a PGXC node
 */
void
PgxcNodeAlter(AlterNodeStmt *stmt)
{
	const char *node_name = stmt->node_name;
	char	   *node_host;
	char		node_type, node_type_old;
	int			node_port;
	bool		is_preferred;
	bool		is_primary;
	bool		was_primary;
	bool		primary_off = false;
	Oid			new_primary = InvalidOid;
	HeapTuple	oldtup, newtup;
	Oid			nodeOid = get_pgxc_nodeoid(node_name);
	Relation	rel;
	Datum		new_record[Natts_pgxc_node];
	bool		new_record_nulls[Natts_pgxc_node];
	bool		new_record_repl[Natts_pgxc_node];
	uint32		node_id;

	/* Only a DB administrator can alter cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to change cluster nodes")));

	/* Look at the node tuple, and take exclusive lock on it */
	rel = heap_open(PgxcNodeRelationId, RowExclusiveLock);

	/* Check that node exists */
	if (!OidIsValid(nodeOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	/* Open new tuple, checks are performed on it and new values */
	oldtup = SearchSysCacheCopy1(PGXCNODEOID, ObjectIdGetDatum(nodeOid));
	if (!HeapTupleIsValid(oldtup))
		elog(ERROR, "cache lookup failed for object %u", nodeOid);

	/*
	 * check_options performs some internal checks on option values
	 * so set up values.
	 */
	node_host = get_pgxc_nodehost(nodeOid);
	node_port = get_pgxc_nodeport(nodeOid);
	is_preferred = is_pgxc_nodepreferred(nodeOid);
	is_primary = was_primary = is_pgxc_nodeprimary(nodeOid);
	node_type = get_pgxc_nodetype(nodeOid);
	node_type_old = node_type;
	node_id = get_pgxc_node_id(nodeOid);

	/* Filter options */
	check_node_options(node_name, stmt->options, &node_host,
				&node_port, &node_type,
				&is_primary, &is_preferred);

	/*
	 * Two nodes cannot be primary at the same time. If the primary
	 * node is this node itself, well there is no point in having an
	 * error.
	 */
	if (is_primary &&
		OidIsValid(primary_data_node) &&
		nodeOid != primary_data_node)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: two nodes cannot be primary",
						node_name)));
	/*
	 * If this node is a primary and the statement says primary = false,
	 * we need to invalidate primary_data_node when the whole operation
	 * is successful.
	 */
	if (was_primary && !is_primary &&
		OidIsValid(primary_data_node) &&
		nodeOid == primary_data_node)
		primary_off = true;
	else if (is_primary)
		new_primary = nodeOid;

	/* Check type dependency */
	if (node_type_old == PGXC_NODE_COORDINATOR &&
		node_type == PGXC_NODE_DATANODE)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot alter Coordinator to Datanode",
						node_name)));
	else if (node_type_old == PGXC_NODE_DATANODE &&
			 node_type == PGXC_NODE_COORDINATOR)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC node %s: cannot alter Datanode to Coordinator",
						node_name)));

	/* Update values for catalog entry */
	MemSet(new_record, 0, sizeof(new_record));
	MemSet(new_record_nulls, false, sizeof(new_record_nulls));
	MemSet(new_record_repl, false, sizeof(new_record_repl));
	new_record[Anum_pgxc_node_port - 1] = Int32GetDatum(node_port);
	new_record_repl[Anum_pgxc_node_port - 1] = true;
	new_record[Anum_pgxc_node_host - 1] =
		DirectFunctionCall1(namein, CStringGetDatum(node_host));
	new_record_repl[Anum_pgxc_node_host - 1] = true;
	new_record[Anum_pgxc_node_type - 1] = CharGetDatum(node_type);
	new_record_repl[Anum_pgxc_node_type - 1] = true;
	new_record[Anum_pgxc_node_is_primary - 1] = BoolGetDatum(is_primary);
	new_record_repl[Anum_pgxc_node_is_primary - 1] = true;
	new_record[Anum_pgxc_node_is_preferred - 1] = BoolGetDatum(is_preferred);
	new_record_repl[Anum_pgxc_node_is_preferred - 1] = true;
	new_record[Anum_pgxc_node_id - 1] = UInt32GetDatum(node_id);
	new_record_repl[Anum_pgxc_node_id - 1] = true;

	/* Update relation */
	newtup = heap_modify_tuple(oldtup, RelationGetDescr(rel),
							   new_record,
							   new_record_nulls, new_record_repl);
	simple_heap_update(rel, &oldtup->t_self, newtup);

	/* Update indexes */
	CatalogUpdateIndexes(rel, newtup);

	/* Invalidate primary_data_node if needed */
	if (primary_off)
		primary_data_node = InvalidOid;
	/* Update primary datanode if needed */
	if (OidIsValid(new_primary))
		primary_data_node = new_primary;
	/* Release lock at Commit */
	heap_close(rel, NoLock);
}


/*
 * PgxcNodeRemove
 *
 * Remove a PGXC node
 */
void
PgxcNodeRemove(DropNodeStmt *stmt)
{
	Relation	relation;
	HeapTuple	tup;
	const char	*node_name = stmt->node_name;
	Oid		noid = get_pgxc_nodeoid(node_name);
	bool 		is_primary;

	/* Only a DB administrator can remove cluster nodes */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to remove cluster nodes")));

	/* Check if node is defined */
	if (!OidIsValid(noid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	if (strcmp(node_name, PGXCNodeName) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("PGXC Node %s: cannot drop local node",
						node_name)));

	/* PGXCTODO:
	 * Is there any group which has this node as member
	 * XC Tables will also have this as a member in their array
	 * Do this search in the local data structure.
	 * If a node is removed, it is necessary to check if there is a distributed
	 * table on it. If there are only replicated table it is OK.
	 * However, we have to be sure that there are no pooler agents in the cluster pointing to it.
	 */

	/* Delete the pgxc_node tuple */
	relation = heap_open(PgxcNodeRelationId, RowExclusiveLock);
	tup = SearchSysCache1(PGXCNODEOID, ObjectIdGetDatum(noid));
	is_primary = is_pgxc_nodeprimary(noid);
	if (!HeapTupleIsValid(tup)) /* should not happen */
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("PGXC Node %s: object not defined",
						node_name)));

	simple_heap_delete(relation, &tup->t_self);

	ReleaseSysCache(tup);

	if (is_primary)
		primary_data_node = InvalidOid;
	heap_close(relation, RowExclusiveLock);
}
