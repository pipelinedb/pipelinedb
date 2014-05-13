/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#ifdef PGXC
#include "catalog/pgxc_node.h"
#endif /* PGXC */
#include "commands/cluster.h"
#ifdef PGXC
#include "commands/copy.h"
#include "commands/createas.h"
#endif /* PGXC */
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "pgxc/remotecopy.h"
#include "pgxc/copyops.h"
#endif /* PGXC */
#include "rewrite/rewriteHandler.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#ifdef PGXC
#include "utils/tqual.h"
#endif /* PGXC */


typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			hi_options;		/* heap_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_transientrel;

static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static void transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static void refresh_matview_datafill(DestReceiver *dest, Query *query,
						 const char *queryString);

/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewPopulatedState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = heap_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relispopulated = newstate;

	simple_heap_update(pgrel, &tuple->t_self, tuple);

	CatalogUpdateIndexes(pgrel, tuple);

	heap_freetuple(tuple);
	heap_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenodes of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 */
void
ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   ParamListInfo params, char *completionTag)
{
	Oid			matviewOid;
	Relation	matviewRel;
	RewriteRule *rule;
	List	   *actions;
	Query	   *dataQuery;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	Oid			tableSpace;
	Oid			OIDNewHeap;
	DestReceiver *dest;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(stmt->relation,
										  AccessExclusiveLock, false, false,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = heap_open(matviewOid, NoLock);

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	/* We don't allow an oid column for a materialized view. */
	Assert(!matviewRel->rd_rel->relhasoids);

	/*
	 * Check that everything is correct for a refresh. Problems at this point
	 * are internal errors, so elog is sufficient.
	 */
	if (matviewRel->rd_rel->relhasrules == false ||
		matviewRel->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matviewRel));

	if (matviewRel->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matviewRel));

	rule = matviewRel->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matviewRel));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matviewRel));

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 */
	dataQuery = (Query *) linitial(actions);
	Assert(IsA(dataQuery, Query));

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using HEAP_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also lock down security-restricted operations and arrange to
	 * make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(matviewRel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !stmt->skipData);

	tableSpace = matviewRel->rd_rel->reltablespace;

	/* Create the transient table that will receive the regenerated data. */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace);
	dest = CreateTransientRelDestReceiver(OIDNewHeap);

#ifdef PGXC
	/*
	 * If the REFRESH command was received from other coordinator, it will also send
	 * the data to be filled in the materialized view, using COPY protocol.
	 */
	if (IsConnFromCoord())
	{
		Assert(IS_PGXC_COORDINATOR);
		pgxc_fill_matview_by_copy(dest, stmt->skipData, 0, NULL);
	}
	else
#endif /* PGXC */
	/* Generate the data, if wanted. */
	if (!stmt->skipData)
		refresh_matview_datafill(dest, dataQuery, queryString);

	heap_close(matviewRel, NoLock);

	/*
	 * Swap the physical files of the target and transient tables, then
	 * rebuild the target's indexes and throw away the transient table.
	 */
	finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
					 RecentXmin, ReadNextMultiXactId());

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
}

/*
 * refresh_matview_datafill
 */
static void
refresh_matview_datafill(DestReceiver *dest, Query *query,
						 const char *queryString)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */
	plan = pg_plan_query(query, 0, NULL);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.	(This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, 0);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, EXEC_FLAG_WITHOUT_OIDS);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
}

DestReceiver *
CreateTransientRelDestReceiver(Oid transientoid)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->transientoid = transientoid;

	return (DestReceiver *) self;
}

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	Relation	transientrel;

	transientrel = heap_open(myState->transientoid, NoLock);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->transientrel = transientrel;
	myState->output_cid = GetCurrentCommandId(true);

	/*
	 * We can skip WAL-logging the insertions, unless PITR or streaming
	 * replication is in use. We can skip the FSM in any case.
	 */
	myState->hi_options = HEAP_INSERT_SKIP_FSM | HEAP_INSERT_FROZEN;
	if (!XLogIsNeeded())
		myState->hi_options |= HEAP_INSERT_SKIP_WAL;
	myState->bistate = GetBulkInsertState();

	/* Not using WAL requires smgr_targblock be initially invalid */
	Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static void
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	HeapTuple	tuple;

	/*
	 * get the heap tuple out of the tuple table slot, making sure we have a
	 * writable copy
	 */
	tuple = ExecMaterializeSlot(slot);

	heap_insert(myState->transientrel,
				tuple,
				myState->output_cid,
				myState->hi_options,
				myState->bistate);

	/* We know this is a newly created relation, so there are no indexes */
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	FreeBulkInsertState(myState->bistate);

	/* If we skipped using WAL, must heap_sync before commit */
	if (myState->hi_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(myState->transientrel);

	/* close transientrel, but keep lock until commit */
	heap_close(myState->transientrel, NoLock);
	myState->transientrel = NULL;
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}

#ifdef PGXC
/*
 * This function accepts the data from the coordinator which initiated the
 * REFRESH MV command and inserts it into the transient relation created for the
 * materialized view.
 * The caller provides the materialized view receiver and the argument values to
 * be passed to the startup function of this receiver.
 */
extern void
pgxc_fill_matview_by_copy(DestReceiver *mv_dest, bool skipdata, int operation,
							TupleDesc tupdesc)
{
	CopyState	cstate;
	Relation	mv_rel;
	TupleDesc	tupDesc;
	Datum		*values;
	bool		*isnulls;
	TupleTableSlot	*slot = MakeTupleTableSlot();

	Assert(IS_PGXC_COORDINATOR && IsConnFromCoord());

	/*
	 * We need to provide the Relation where to copy to BeginCopyFrom. The
	 * Relation is populated by startup method of the DestReceiver. The startup
	 * method also creates the relation (in this case the materialized view
	 * itself or heap thereof), so irrespective of whether we are going to
	 * populate the MV or not, always fire the starup function.
	 */
	mv_dest->rStartup(mv_dest, operation, tupdesc);

	/*
	 * If we are to populate the the materialized view, only then start copy
	 * protocol and accept the data. The initiating coordinator too, will send
	 * the data only when the MV is to be populated.
	 */
	if (!skipdata)
	{
		if (mv_dest->mydest == DestTransientRel)
			mv_rel = ((DR_transientrel *)mv_dest)->transientrel;
		else if (mv_dest->mydest == DestIntoRel)
			mv_rel = get_dest_into_rel(mv_dest);

		tupDesc = RelationGetDescr(mv_rel);
		values = (Datum *) palloc0(sizeof(Datum) * tupDesc->natts);
		isnulls = (bool *) palloc0(sizeof(bool) * tupDesc->natts);
		ExecSetSlotDescriptor(slot, tupDesc);
		/*
		 * Prepare structures to start receiving the data sent by the other
		 * coordinator through COPY protocol.
		 */
		cstate = BeginCopyFrom(mv_rel, NULL, false, NULL, NULL);
		/* Read the rows one by one and insert into the materialized view */
		for(;;)
		{
			Oid				tupleOid; /* Temporary variable passed to NextCopyFrom() */
			HeapTuple		tuple;
			ExecClearTuple(slot);
			/*
			 * Pull next row. The expression context is not used here, since there
			 * are no default expressions expected. Tuple OID too is not expected
			 * for materialized view.
			 */
			if (!NextCopyFrom(cstate, NULL, values, isnulls, &tupleOid))
				break;

			/* Create the tuple and slot out of the values read */
			tuple = heap_form_tuple(tupDesc, values, isnulls);
			ExecStoreTuple(tuple, slot, 0, false);

			/* Insert the row in the materialized view */
			mv_dest->receiveSlot(slot, mv_dest);
		}
		ReleaseTupleDesc(tupDesc);
		EndCopyFrom(cstate);
	}

	/* Done, close the receiver and flag the end of COPY FROM */
	mv_dest->rShutdown(mv_dest);

}

/*
 * The function scans the recently refreshed materialized view and send the data
 * to the other coordinators to "refresh" materialized views at those
 * coordinators.
 * The query_string is expected to contain the DDL which requires this data
 * transfer e.g. CREATE MV or REFRSH MV. The DDL is sent to the other
 * coordinators, which in turn start receiving the data to populate the
 * materialized view.
 */
extern void
pgxc_send_matview_data(RangeVar *matview_rv, const char *query_string)
{
	Oid				matviewOid;
	Relation		matviewRel;
	RemoteCopyData	*copyState;
	TupleDesc 		tupdesc;
	HeapScanDesc 	scandesc;
	HeapTuple		tuple;
	Datum			*values;
	bool			*nulls;

	/*
	 * This function should be called only from the coordinator where the
	 * REFRESH MV command is fired.
	 */
	Assert (IS_PGXC_COORDINATOR && !IsConnFromCoord());
	/*
	 * The other coordinator will start accepting data through COPY protocol in
	 * response to the DDL. So, start sending the data with COPY
	 * protocol to the other coordinators.
	 */

	/* Prepare the RemoteCopyData for the COPYing data to the other coordinators */
	copyState = (RemoteCopyData *) palloc0(sizeof(RemoteCopyData));
	copyState->exec_nodes = makeNode(ExecNodes);
	/* We are copying the data from the materialized view */
	copyState->is_from = false;
	/* Materialized views are available on all the coordinators. */
	copyState->exec_nodes->nodeList = GetAllCoordNodes();
	initStringInfo(&(copyState->query_buf));
	appendStringInfoString(&(copyState->query_buf), query_string);
	/* Begin redistribution on remote nodes */
	copyState->connections = pgxcNodeCopyBegin(copyState->query_buf.data,
											   copyState->exec_nodes->nodeList,
											   GetActiveSnapshot(),
											   PGXC_NODE_COORDINATOR);

	/*
	 * Open the relation for reading.
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(matview_rv,
										  AccessShareLock, false, false,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = heap_open(matviewOid, NoLock);
	tupdesc = RelationGetDescr(matviewRel);
	values = (Datum *) palloc(tupdesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupdesc->natts * sizeof(bool));
	scandesc = heap_beginscan(matviewRel, SnapshotAny, 0, NULL);

	/* Send each tuple to the other coordinators in COPY format */
	while ((tuple = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		char *data;
		int len;

		CHECK_FOR_INTERRUPTS();
		/* Deconstruct the tuple to get the values for the attributes */
		heap_deform_tuple(tuple, tupdesc, values, nulls);

		/* Format and send the data */
		data = CopyOps_BuildOneRowTo(tupdesc, values, nulls, &len);

		DataNodeCopyIn(data,
					   len,
					   copyState->exec_nodes,
					   copyState->connections);
		pfree(data);
	}

	heap_endscan(scandesc);
	pfree(values);
	pfree(nulls);

	/*
	 * Finish the redistribution process. There is no primary node for
	 * Materialized view and it's replicated on all the coordinators
	 */
	pgxcNodeCopyFinish(copyState->connections, -1, COMBINE_TYPE_SAME, PGXC_NODE_COORDINATOR);

	/* Lock is maintained until transaction commits */
	relation_close(matviewRel, NoLock);
	return;
}
#endif /* PGXC */
