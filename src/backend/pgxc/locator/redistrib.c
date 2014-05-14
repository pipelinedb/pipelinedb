/*-------------------------------------------------------------------------
 *
 * redistrib.c
 *	  Routines related to online data redistribution
 *
 * Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/pgxc/locator/redistrib.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/hash.h"
#include "access/htup.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "commands/tablecmds.h"
#include "pgxc/copyops.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxc.h"
#include "pgxc/redistrib.h"
#include "pgxc/remotecopy.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#define IsCommandTypePreUpdate(x) (x == CATALOG_UPDATE_BEFORE || \
								   x == CATALOG_UPDATE_BOTH)
#define IsCommandTypePostUpdate(x) (x == CATALOG_UPDATE_AFTER || \
									x == CATALOG_UPDATE_BOTH)

/* Functions used for the execution of redistribution commands */
static void distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes);
static void distrib_execute_command(RedistribState *distribState, RedistribCommand *command);
static void distrib_copy_to(RedistribState *distribState);
static void distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes);
static void distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes);

/* Functions used to build the command list */
static void pgxc_redist_build_entry(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);
static void pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
								RelationLocInfo *oldLocInfo,
								RelationLocInfo *newLocInfo);

static void pgxc_redist_build_default(RedistribState *distribState);
static void pgxc_redist_add_reindex(RedistribState *distribState);


/*
 * PGXCRedistribTable
 * Execute redistribution operations after catalog update
 */
void
PGXCRedistribTable(RedistribState *distribState, RedistribCatalog type)
{
	ListCell *item;

	/* Nothing to do if no redistribution operation */
	if (!distribState)
		return;

	/* Nothing to do if on remote node */
	if (IS_PGXC_DATANODE || IsConnFromCoord())
		return;

	/* Execute each command if necessary */
	foreach(item, distribState->commands)
	{
		RedistribCommand *command = (RedistribCommand *)lfirst(item);

		/* Check if command can be run */
		if (!IsCommandTypePostUpdate(type) &&
			IsCommandTypePostUpdate(command->updateState))
			continue;
		if (!IsCommandTypePreUpdate(type) &&
			IsCommandTypePreUpdate(command->updateState))
			continue;

		/* Now enter in execution list */
		distrib_execute_command(distribState, command);
	}
}


/*
 * PGXCRedistribCreateCommandList
 * Look for the list of necessary commands to perform table redistribution.
 */
void
PGXCRedistribCreateCommandList(RedistribState *distribState, RelationLocInfo *newLocInfo)
{
	Relation rel;
	RelationLocInfo *oldLocInfo;

	rel = relation_open(distribState->relid, NoLock);
	oldLocInfo = RelationGetLocInfo(rel);

	/* Build redistribution command list */
	pgxc_redist_build_entry(distribState, oldLocInfo, newLocInfo);

	relation_close(rel, NoLock);
}


/*
 * pgxc_redist_build_entry
 * Entry point for command list building
 */
static void
pgxc_redist_build_entry(RedistribState *distribState,
						RelationLocInfo *oldLocInfo,
						RelationLocInfo *newLocInfo)
{
	/* If distribution has not changed at all, nothing to do */
	if (IsLocatorInfoEqual(oldLocInfo, newLocInfo))
		return;

	/* Evaluate cases for replicated tables */
	pgxc_redist_build_replicate(distribState, oldLocInfo, newLocInfo);

	/* Evaluate cases for replicated to distributed tables */
	pgxc_redist_build_replicate_to_distrib(distribState, oldLocInfo, newLocInfo);

	/* PGXCTODO: perform more complex builds of command list */

	/* Fallback to default */
	pgxc_redist_build_default(distribState);
}


/*
 * pgxc_redist_build_replicate_to_distrib
 * Build redistribution command list from replicated to distributed
 * table.
 */
static void
pgxc_redist_build_replicate_to_distrib(RedistribState *distribState,
							RelationLocInfo *oldLocInfo,
							RelationLocInfo *newLocInfo)
{
	List *removedNodes;
	List *newNodes;

	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* Redistribution is done from replication to distributed (with value) */
	if (!IsRelationReplicated(oldLocInfo) ||
		!IsRelationDistributedByValue(newLocInfo))
		return;

	/* Get the list of nodes that are added to the relation */
	removedNodes = list_difference_int(oldLocInfo->nodeList, newLocInfo->nodeList);

	/* Get the list of nodes that are removed from relation */
	newNodes = list_difference_int(newLocInfo->nodeList, oldLocInfo->nodeList);

	/*
	 * If some nodes are added, turn back to default, we need to fetch data
	 * and then redistribute it properly.
	 */
	if (newNodes != NIL)
		return;

	/* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
	if (removedNodes != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeList = removedNodes;
		/* Add TRUNCATE command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
	}

	/*
	 * If the table is redistributed to a single node, a TRUNCATE on removed nodes
	 * is sufficient so leave here.
	 */
	if (list_length(newLocInfo->nodeList) == 1)
	{
		/* Add REINDEX command if necessary */
		pgxc_redist_add_reindex(distribState);
		return;
	}

	/*
	 * If we are here we are sure that redistribution only requires to delete data on remote
	 * nodes on the new subset of nodes. So launch to remote nodes a DELETE command that only
	 * eliminates the data not verifying the new hashing condition.
	 */
	if (newLocInfo->locatorType == LOCATOR_TYPE_HASH)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeList = newLocInfo->nodeList;
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_DELETE_HASH, CATALOG_UPDATE_AFTER, execNodes));
	}
	else if (newLocInfo->locatorType == LOCATOR_TYPE_MODULO)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeList = newLocInfo->nodeList;
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_DELETE_MODULO, CATALOG_UPDATE_AFTER, execNodes));
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("Incorrect redistribution operation")));

	/* Add REINDEX command if necessary */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_replicate
 * Build redistribution command list for replicated tables
 */
static void
pgxc_redist_build_replicate(RedistribState *distribState,
							RelationLocInfo *oldLocInfo,
							RelationLocInfo *newLocInfo)
{
	List *removedNodes;
	List *newNodes;

	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* Case of a replicated table whose set of nodes is changed */
	if (!IsRelationReplicated(newLocInfo) ||
		!IsRelationReplicated(oldLocInfo))
		return;

	/* Get the list of nodes that are added to the relation */
	removedNodes = list_difference_int(oldLocInfo->nodeList, newLocInfo->nodeList);

	/* Get the list of nodes that are removed from relation */
	newNodes = list_difference_int(newLocInfo->nodeList, oldLocInfo->nodeList);

	/*
	 * If nodes have to be added, we need to fetch data for redistribution first.
	 * So add a COPY TO command to fetch data.
	 */
	if (newNodes != NIL)
	{
		/* Add COPY TO command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
	}

	/* Nodes removed have to be truncated, so add a TRUNCATE commands to removed nodes */
	if (removedNodes != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeList = removedNodes;
		/* Add TRUNCATE command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, execNodes));
	}

	/* If necessary, COPY the data obtained at first step to the new nodes. */
	if (newNodes != NIL)
	{
		ExecNodes *execNodes = makeNode(ExecNodes);
		execNodes->nodeList = newNodes;
		/* Add COPY FROM command */
		distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, execNodes));
	}

	/* Add REINDEX command if necessary */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_default
 * Build a default list consisting of
 * COPY TO -> TRUNCATE -> COPY FROM ( -> REINDEX )
 */
static void
pgxc_redist_build_default(RedistribState *distribState)
{
	/* If a command list has already been built, nothing to do */
	if (list_length(distribState->commands) != 0)
		return;

	/* COPY TO command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_TO, CATALOG_UPDATE_BEFORE, NULL));
	/* TRUNCATE command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_TRUNCATE, CATALOG_UPDATE_BEFORE, NULL));
	/* COPY FROM command */
	distribState->commands = lappend(distribState->commands,
					 makeRedistribCommand(DISTRIB_COPY_FROM, CATALOG_UPDATE_AFTER, NULL));

	/* REINDEX command */
	pgxc_redist_add_reindex(distribState);
}


/*
 * pgxc_redist_build_reindex
 * Add a reindex command if necessary
 */
static void
pgxc_redist_add_reindex(RedistribState *distribState)
{
	Relation rel;

	rel = relation_open(distribState->relid, NoLock);

	/* Build REINDEX command if necessary */
	if (RelationGetIndexList(rel) != NIL)
	{
		distribState->commands = lappend(distribState->commands,
			 makeRedistribCommand(DISTRIB_REINDEX, CATALOG_UPDATE_AFTER, NULL));
	}

	relation_close(rel, NoLock);
}


/*
 * distrib_execute_command
 * Execute a redistribution operation
 */
static void
distrib_execute_command(RedistribState *distribState, RedistribCommand *command)
{
	/* Execute redistribution command */
	switch (command->type)
	{
		case DISTRIB_COPY_TO:
			distrib_copy_to(distribState);
			break;
		case DISTRIB_COPY_FROM:
			distrib_copy_from(distribState, command->execNodes);
			break;
		case DISTRIB_TRUNCATE:
			distrib_truncate(distribState, command->execNodes);
			break;
		case DISTRIB_REINDEX:
			distrib_reindex(distribState, command->execNodes);
			break;
		case DISTRIB_DELETE_HASH:
		case DISTRIB_DELETE_MODULO:
			distrib_delete_hash(distribState, command->execNodes);
			break;
		case DISTRIB_NONE:
		default:
			Assert(0); /* Should not happen */
	}
}


/*
 * distrib_copy_to
 * Copy all the data of table to be distributed.
 * This data is saved in a tuplestore saved in distribution state.
 * a COPY FROM operation is always done on nodes determined by the locator data
 * in catalogs, explaining why this cannot be done on a subset of nodes. It also
 * insures that no read operations are done on nodes where data is not yet located.
 */
static void
distrib_copy_to(RedistribState *distribState)
{
	Oid			relOid = distribState->relid;
	Relation	rel;
	RemoteCopyOptions *options;
	RemoteCopyData *copyState;
	Tuplestorestate *store; /* Storage of redistributed data */

	/* Fetch necessary data to prepare for the table data acquisition */
	options = makeRemoteCopyOptions();

	/* All the fields are separated by tabs in redistribution */
	options->rco_delim = palloc(2);
	options->rco_delim[0] = COPYOPS_DELIMITER;
	options->rco_delim[1] = '\0';

	copyState = (RemoteCopyData *) palloc0(sizeof(RemoteCopyData));
	copyState->is_from = false;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);
	RemoteCopy_GetRelationLoc(copyState, rel, NIL);
	RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Copying data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Begin the COPY process */
	copyState->connections = pgxcNodeCopyBegin(copyState->query_buf.data,
											   copyState->exec_nodes->nodeList,
											   GetActiveSnapshot(),
											   PGXC_NODE_DATANODE);

	/* Create tuplestore storage */
	store = tuplestore_begin_heap(true, false, work_mem);

	/* Then get rows and copy them to the tuplestore used for redistribution */
	DataNodeCopyOut(copyState->exec_nodes,
					copyState->connections,
					RelationGetDescr(rel),	/* Need also to set up the tuple descriptor */
					NULL,
					store,					/* Tuplestore used for redistribution */
					REMOTE_COPY_TUPLESTORE);

	/* Do necessary clean-up */
	FreeRemoteCopyOptions(options);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);

	/* Save results */
	distribState->store = store;
}


/*
 * PGXCDistribTableCopyFrom
 * Execute commands related to COPY FROM
 * Redistribute all the data of table with a COPY FROM from given tuplestore.
 */
static void
distrib_copy_from(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Oid relOid = distribState->relid;
	Tuplestorestate *store = distribState->store;
	Relation	rel;
	RemoteCopyOptions *options;
	RemoteCopyData *copyState;
	bool replicated, contains_tuple = true;
	TupleDesc tupdesc;

	/* Nothing to do if on remote node */
	if (IS_PGXC_DATANODE || IsConnFromCoord())
		return;

	/* Fetch necessary data to prepare for the table data acquisition */
	options = makeRemoteCopyOptions();
	/* All the fields are separated by tabs in redistribution */
	options->rco_delim = palloc(2);
	options->rco_delim[0] = COPYOPS_DELIMITER;
	options->rco_delim[1] = '\0';

	copyState = (RemoteCopyData *) palloc0(sizeof(RemoteCopyData));
	copyState->is_from = true;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);
	RemoteCopy_GetRelationLoc(copyState, rel, NIL);
	RemoteCopy_BuildStatement(copyState, rel, options, NIL, NIL);

	/*
	 * When building COPY FROM command in redistribution list,
	 * use the list of nodes that has been calculated there.
	 * It might be possible that this COPY is done only on a portion of nodes.
	 */
	if (exec_nodes && exec_nodes->nodeList != NIL)
	{
		copyState->exec_nodes->nodeList = exec_nodes->nodeList;
		copyState->rel_loc->nodeList = exec_nodes->nodeList;
	}

	tupdesc = RelationGetDescr(rel);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Redistributing data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Begin redistribution on remote nodes */
	copyState->connections = pgxcNodeCopyBegin(copyState->query_buf.data,
											   copyState->exec_nodes->nodeList,
											   GetActiveSnapshot(),
											   PGXC_NODE_DATANODE);

	/* Transform each tuple stored into a COPY message and send it to remote nodes */
	while (contains_tuple)
	{
		char *data;
		int len;
		Form_pg_attribute *attr = tupdesc->attrs;
		Datum	dist_col_value = (Datum) 0;
		bool	dist_col_is_null = true;
		Oid		dist_col_type = UNKNOWNOID;
		TupleTableSlot *slot;
		ExecNodes   *local_execnodes;

		/* Build table slot for this relation */
		slot = MakeSingleTupleTableSlot(tupdesc);

		/* Get tuple slot from the tuplestore */
		contains_tuple = tuplestore_gettupleslot(store, true, false, slot);
		if (!contains_tuple)
		{
			ExecDropSingleTupleTableSlot(slot);
			break;
		}

		/* Make sure the tuple is fully deconstructed */
		slot_getallattrs(slot);

		/* Find value of distribution column if necessary */
		if (copyState->idx_dist_by_col >= 0)
		{
			dist_col_value = slot->tts_values[copyState->idx_dist_by_col];
			dist_col_is_null =  slot->tts_isnull[copyState->idx_dist_by_col];
			dist_col_type = attr[copyState->idx_dist_by_col]->atttypid;
		}

		/* Build message to be sent to Datanodes */
		data = CopyOps_BuildOneRowTo(tupdesc, slot->tts_values, slot->tts_isnull, &len);

		/* Build relation node list */
		local_execnodes = GetRelationNodes(copyState->rel_loc,
										   dist_col_value,
										   dist_col_is_null,
										   dist_col_type,
										   RELATION_ACCESS_INSERT);
		/* Take a copy of the node lists so as not to interfere with locator info */
		local_execnodes->primarynodelist = list_copy(local_execnodes->primarynodelist);
		local_execnodes->nodeList = list_copy(local_execnodes->nodeList);

		/* Process data to Datanodes */
		DataNodeCopyIn(data,
					   len,
					   local_execnodes,
					   copyState->connections);

		/* Clean up */
		pfree(data);
		FreeExecNodes(&local_execnodes);
		ExecClearTuple(slot);
		ExecDropSingleTupleTableSlot(slot);
	}

	/* Finish the redistribution process */
	replicated = copyState->rel_loc->locatorType == LOCATOR_TYPE_REPLICATED;
	pgxcNodeCopyFinish(copyState->connections,
					   replicated ? PGXCNodeGetNodeId(primary_data_node, PGXC_NODE_DATANODE) : -1,
					   replicated ? COMBINE_TYPE_SAME : COMBINE_TYPE_SUM, PGXC_NODE_DATANODE);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);
}


/*
 * distrib_truncate
 * Truncate all the data of specified table.
 * This is used as a second step of online data redistribution.
 */
static void
distrib_truncate(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;

	/* Nothing to do if on remote node */
	if (IS_PGXC_DATANODE || IsConnFromCoord())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Truncating data for relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Build query to clean up table before redistribution */
	appendStringInfo(buf, "TRUNCATE %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/*
	 * Lock is maintained until transaction commits,
	 * relation needs also to be closed before effectively launching the query.
	 */
	relation_close(rel, NoLock);

	/* Execute the query */
	distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);
}


/*
 * distrib_reindex
 * Reindex the table that has been redistributed
 */
static void
distrib_reindex(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;

	/* Nothing to do if on remote node */
	if (IS_PGXC_DATANODE || IsConnFromCoord())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Reindexing relation \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Generate the query */
	appendStringInfo(buf, "REINDEX TABLE %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/* Execute the query */
	distrib_execute_query(buf->data, IsTempTable(relOid), exec_nodes);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);

	/* Lock is maintained until transaction commits */
	relation_close(rel, NoLock);
}


/*
 * distrib_delete_hash
 * Perform a partial tuple deletion of remote tuples not checking the correct hash
 * condition. The new distribution condition is set up in exec_nodes when building
 * the command list.
 */
static void
distrib_delete_hash(RedistribState *distribState, ExecNodes *exec_nodes)
{
	Relation	rel;
	StringInfo	buf;
	Oid			relOid = distribState->relid;
	ListCell   *item;

	/* Nothing to do if on remote node */
	if (IS_PGXC_DATANODE || IsConnFromCoord())
		return;

	/* A sufficient lock level needs to be taken at a higher level */
	rel = relation_open(relOid, NoLock);

	/* Inform client of operation being done */
	ereport(DEBUG1,
			(errmsg("Deleting necessary tuples \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(rel)),
					RelationGetRelationName(rel))));

	/* Initialize buffer */
	buf = makeStringInfo();

	/* Build query to clean up table before redistribution */
	appendStringInfo(buf, "DELETE FROM %s.%s",
					 get_namespace_name(RelationGetNamespace(rel)),
					 RelationGetRelationName(rel));

	/*
	 * Launch the DELETE query to each node as the DELETE depends on
	 * local conditions for each node.
	 */
	foreach(item, exec_nodes->nodeList)
	{
		StringInfo	buf2;
		char	   *hashfuncname, *colname = NULL;
		Oid			hashtype;
		RelationLocInfo *locinfo = RelationGetLocInfo(rel);
		int			nodenum = lfirst_int(item);
		int			nodepos = 0;
		ExecNodes  *local_exec_nodes = makeNode(ExecNodes);
		TupleDesc	tupDesc = RelationGetDescr(rel);
		Form_pg_attribute *attr = tupDesc->attrs;
		ListCell   *item2;

		/* Here the query is launched to a unique node */
		local_exec_nodes->nodeList = lappend_int(NIL, nodenum);

		/* Get the hash type of relation */
		hashtype = attr[locinfo->partAttrNum - 1]->atttypid;

		/* Get function hash name */
		hashfuncname = get_compute_hash_function(hashtype, locinfo->locatorType);

		/* Get distribution column name */
		if (IsRelationDistributedByValue(locinfo))
			colname = GetRelationDistribColumn(locinfo);
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("Incorrect redistribution operation")));

		/*
		 * Find the correct node position in node list of locator information.
		 * So scan the node list and fetch the position of node.
		 */
		foreach(item2, locinfo->nodeList)
		{
			int loc = lfirst_int(item2);
			if (loc == nodenum)
				break;
			nodepos++;
		}

		/*
		 * Then build the WHERE clause for deletion.
		 * The condition that allows to keep the tuples on remote nodes
		 * is of the type "RemoteNodeNumber != abs(hash_func(dis_col)) % NumDatanodes".
		 * the remote Datanode has no knowledge of its position in cluster so this
		 * number needs to be compiled locally on Coordinator.
		 * Taking the absolute value is necessary as hash may return a negative value.
		 * For hash distributions a condition with correct hash function is used.
		 * For modulo distribution, well we might need a hash function call but not
		 * all the time, this is determined implicitely by get_compute_hash_function.
		 */
		buf2 = makeStringInfo();
		if (hashfuncname)
		{
			/* Lets leave NULLs on the first node and delete from the rest */
			if (nodepos != 0)
				appendStringInfo(buf2, "%s WHERE %s IS NULL OR abs(%s(%s)) %% %d != %d",
								buf->data, colname, hashfuncname, colname,
								list_length(locinfo->nodeList), nodepos);
			else
				appendStringInfo(buf2, "%s WHERE abs(%s(%s)) %% %d != %d",
								buf->data, hashfuncname, colname,
								list_length(locinfo->nodeList), nodepos);
		}
		else
		{
			/* Lets leave NULLs on the first node and delete from the rest */
			if (nodepos != 0)
				appendStringInfo(buf2, "%s WHERE %s IS NULL OR abs(%s) %% %d != %d",
								buf->data, colname, colname,
								list_length(locinfo->nodeList), nodepos);
			else
				appendStringInfo(buf2, "%s WHERE abs(%s) %% %d != %d",
								buf->data, colname,
								list_length(locinfo->nodeList), nodepos);
		}

		/* Then launch this single query */
		distrib_execute_query(buf2->data, IsTempTable(relOid), local_exec_nodes);

		FreeExecNodes(&local_exec_nodes);
		pfree(buf2->data);
		pfree(buf2);
	}

	relation_close(rel, NoLock);

	/* Clean buffers */
	pfree(buf->data);
	pfree(buf);
}


/*
 * makeRedistribState
 * Build a distribution state operator
 */
RedistribState *
makeRedistribState(Oid relOid)
{
	RedistribState *res = (RedistribState *) palloc(sizeof(RedistribState));
	res->relid = relOid;
	res->commands = NIL;
	res->store = NULL;
	return res;
}


/*
 * FreeRedistribState
 * Free given distribution state
 */
void
FreeRedistribState(RedistribState *state)
{
	ListCell *item;

	/* Leave if nothing to do */
	if (!state)
		return;

	foreach(item, state->commands)
		FreeRedistribCommand((RedistribCommand *) lfirst(item));
	if (list_length(state->commands) > 0)
		list_free(state->commands);
	if (state->store)
		tuplestore_clear(state->store);
}

/*
 * makeRedistribCommand
 * Build a distribution command
 */
RedistribCommand *
makeRedistribCommand(RedistribOperation type, RedistribCatalog updateState, ExecNodes *nodes)
{
	RedistribCommand *res = (RedistribCommand *) palloc0(sizeof(RedistribCommand));
	res->type = type;
	res->updateState = updateState;
	res->execNodes = nodes;
	return res;
}

/*
 * FreeRedistribCommand
 * Free given distribution command
 */
void
FreeRedistribCommand(RedistribCommand *command)
{
	ExecNodes *nodes;
	/* Leave if nothing to do */
	if (!command)
		return;
	nodes = command->execNodes;

	if (nodes)
		FreeExecNodes(&nodes);
	pfree(command);
}

/*
 * distrib_execute_query
 * Execute single raw query on given list of nodes
 */
static void
distrib_execute_query(char *sql, bool is_temp, ExecNodes *exec_nodes)
{
	RemoteQuery *step = makeNode(RemoteQuery);
	step->combine_type = COMBINE_TYPE_SAME;
	step->exec_nodes = exec_nodes;
	step->sql_statement = pstrdup(sql);
	step->force_autocommit = false;

	/* Redistribution operations only concern Datanodes */
	step->exec_type = EXEC_ON_DATANODES;
	step->is_temp = is_temp;
	ExecRemoteUtility(step);
	pfree(step->sql_statement);
	pfree(step);

	/* Be sure to advance the command counter after the last command */
	CommandCounterIncrement();
}
