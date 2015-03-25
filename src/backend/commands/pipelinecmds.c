/*-------------------------------------------------------------------------
 *
 * pipelinecmds.c
 *	  commands specific to PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/commands/pipelinecmds.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "pgstat.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pipeline_tstate_fn.h"
#include "catalog/toasting.h"
#include "executor/execdesc.h"
#include "executor/tstoreReceiver.h"
#include "libpq/libpq-be.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqmatrel.h"
#include "pipeline/cqproc.h"
#include "pipeline/cqwindow.h"
#include "pipeline/stream.h"
#include "pipeline/tuplebuf.h"
#include "regex/regex.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#define CQ_MATREL_INDEX_TYPE "btree"
#define DEFAULT_TYPEMOD -1

static ColumnDef *
make_cv_columndef(char *name, Oid type, Oid typemod)
{
	ColumnDef *result;
	TypeName *typename;

	typename = makeNode(TypeName);
	typename->typeOid = type;
	typename->typemod = typemod;

	result = makeNode(ColumnDef);
	result->colname = name;
	result->inhcount = 0;
	result->is_local = true;
	result->is_not_null = false;
	result->raw_default = NULL;
	result->cooked_default = NULL;
	result->constraints = NIL;
	result->typeName = typename;

	return result;
}

/*
 * create_indices_on_mat_relation
 *
 * If feasible, create an index on the new materialization table to make
 * combine retrievals on it as efficient as possible. Sometimes this may be
 * impossible to do automatically in a smart way, but for some queries,
 * such as single-column GROUP BYs, it's straightforward.
 */
static void
create_indices_on_mat_relation(Oid matreloid, RangeVar *matrelname, SelectStmt *workerstmt, SelectStmt *viewstmt)
{
	IndexStmt *index;
	IndexElem *indexcol;
	Node *node;
	ColumnRef *col;
	char *namespace;
	char *name;

	if (IsSlidingWindowSelectStmt(workerstmt))
		node = (Node *) GetColumnRefInSlidingWindowExpr(viewstmt);
	else if (list_length(workerstmt->groupClause) == 1)
		node = linitial(workerstmt->groupClause);
	else
		return;

	if (IsA(node, TypeCast))
	{
		TypeCast *tc = (TypeCast *) node;
		node = tc->arg;
	}

	Assert(IsA(node, ColumnRef));
	col = (ColumnRef *) node;

	indexcol = makeNode(IndexElem);

	DeconstructQualifiedName(col->fields, &namespace, &name);

	indexcol->name = name;
	indexcol->expr = NULL;
	indexcol->indexcolname = NULL;
	indexcol->collation = NULL;
	indexcol->opclass = NULL;
	indexcol->ordering = SORTBY_DEFAULT;
	indexcol->nulls_ordering = SORTBY_NULLS_DEFAULT;

	index = makeNode(IndexStmt);
	index->idxname = NULL;
	index->relation = matrelname;
	index->accessMethod = CQ_MATREL_INDEX_TYPE;
	index->tableSpace = NULL;
	index->indexParams = list_make1(indexcol);
	/*
	 * Index should be unique iff there is a single GROUP BY
	 * on the worker.
	 */
	index->unique = list_length(workerstmt->groupClause) == 1;
	index->primary = false;
	index->isconstraint = false;
	index->deferrable = false;
	index->initdeferred = false;
	index->concurrent = false;

	DefineIndex(matreloid, index, InvalidOid, false, false, false, false);
	CommandCounterIncrement();
}

/*
 * CreateContinuousView
 *
 * Creates a table for backing the result of the continuous query,
 * and stores the query in a catalog table.
 */
void
ExecCreateContinuousViewStmt(CreateContinuousViewStmt *stmt, const char *querystring)
{
	CreateStmt *create_stmt;
	ViewStmt *view_stmt;
	Query *query;
	RangeVar *mat_relation;
	RangeVar *view;
	List *tableElts = NIL;
	List *tlist;
	ListCell *col;
	Oid reloid;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	SelectStmt *workerselect;
	SelectStmt *viewselect;
	CQAnalyzeContext context;
	bool saveAllowSystemTableMods;

	view = stmt->into->rel;
	if (view->schemaname)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_SCHEMA_NAME),
				errmsg("continuous views cannot be given a namespace")));

	mat_relation = makeRangeVar(view->schemaname, GetUniqueMatRelName(view->relname, view->schemaname), -1);

	/*
	 * Check if CV already exists?
	 */
	if (IsAContinuousView(view))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" already exists", view->relname)));

	/*
	 * allowSystemTableMods is a global flag that, when true, allows certain column types
	 * to be created. We need it set to true to create some hidden state columns. In particular,
	 * ones with a type of anyarray.
	 */
	saveAllowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;

	/*
	 * Analyze the SelectStmt portion of the CreateContinuousViewStmt to make
	 * sure it's well-formed.
	 */
	query = parse_analyze(copyObject(stmt->query), querystring, 0, 0);

	/*
	 * Get the transformed SelectStmt used by CQ workers. We do this
	 * because the targetList of this SelectStmt contains all columns
	 * that need to be created in the underlying materialization table.
	 */
	workerselect = GetSelectStmtForCQWorker(copyObject(stmt->query), &viewselect);
	InitializeCQAnalyzeContext(workerselect, NULL, &context);

	query = parse_analyze(copyObject(workerselect), querystring, 0, 0);
	tlist = query->targetList;

	/*
	 * Build a list of columns from the SELECT statement that we
	 * can use to create a table with
	 */
	foreach(col, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(col);
		ColumnDef   *coldef;
		char		*colname;
		Oid			hiddentype;
		Oid type;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);

		/*
		 * Set typeOid and typemod. The name of the type is derived while
		 * generating query
		 */
		type = exprType((Node *) tle->expr);
		/*
		 * TODO(usmanm): Check for pseudo-types and replace with a char type.
		 */
		if (type == 2278)
			type = 18;
		coldef = make_cv_columndef(colname, type, exprTypmod((Node *) tle->expr));
		tableElts = lappend(tableElts, coldef);

		/*
		 * If this column requires state to support incremental transitions, create it. Note: since this
		 * column isn't in the target list, it won't be visible when selecting from this CV,
		 * which will have an overlay view that only exposes target list columns.
		 */
		hiddentype = GetCombineStateColumnType(tle->expr);
		if (OidIsValid(hiddentype))
		{
			char *hiddenname = GetUniqueInternalColname(&context);
			ColumnDef *hidden = make_cv_columndef(hiddenname, hiddentype, DEFAULT_TYPEMOD);
			tableElts = lappend(tableElts, hidden);
		}
	}

	/*
	 * Create the actual underlying materialzation relation.
	 */
	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = mat_relation;
	create_stmt->tableElts = tableElts;
	create_stmt->tablespacename = stmt->into->tableSpaceName;
	create_stmt->oncommit = stmt->into->onCommit;
	create_stmt->options = stmt->into->options;

	reloid = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid);
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);

	AlterTableCreateToastTable(reloid, toast_options, AccessExclusiveLock);

	/*
	 * Now save the underlying query in the `pipeline_query` catalog
	 * relation.
	 */
	RegisterContinuousView(view, querystring, mat_relation, IsSlidingWindowSelectStmt(viewselect), SelectsFromStreamOnly(workerselect));
	CommandCounterIncrement();

	/*
	 * Create a VIEW over the CQ materialization relation which exposes
	 * only the columns that users expect. This is needed primarily for three
	 * reasons:
	 *
	 * 1. Sliding window queries. For such queries, this VIEW filters events out
	 *    of the window (that have not been GC'd).
	 * 2. Some aggregate operators require storing some additional state along
	 *    with partial results and this VIEW filters out such hidden
	 *    columns.
	 * 3. View also computes expressions on aggregates.
	 */
	viewselect->fromClause = list_make1(mat_relation);
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;

	DefineView(view_stmt, NULL);
	CommandCounterIncrement();
	allowSystemTableMods = saveAllowSystemTableMods;

	/*
	 * Index the materialization table smartly if we can
	 */
	allowSystemTableMods = saveAllowSystemTableMods;
	create_indices_on_mat_relation(reloid, mat_relation, workerselect, viewselect);
}

/*
 * ExecDropContinuousViewStmt
 *
 * Drops the query row in the pipeline_query catalog table.
 */
void
ExecDropContinuousViewStmt(DropStmt *stmt)
{
	Relation pipeline_query;
	List *relations = NIL;
	ListCell *item;

	/*
	 * Scan the pipeline_query relation to find the OID of the views(s) to be
	 * deleted.
	 */
	pipeline_query = heap_open(PipelineQueryRelationId, AccessExclusiveLock);

	foreach(item, stmt->objects)
	{
		RangeVar *rv = makeRangeVarFromNameList((List *) lfirst(item));
		HeapTuple tuple;
		Form_pipeline_query row;

		tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));
		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
					errmsg("continuous view \"%s\" does not exist", rv->relname)));
		row = (Form_pipeline_query) GETSTRUCT(tuple);
		if (row->state == PIPELINE_QUERY_STATE_ACTIVE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_CONTINUOUS_VIEW_STATE),
					 errmsg("continuous view \"%s\" is currently active", rv->relname),
					 errhint("Only inactive continuous views can be dropped, which requires deactivating them. For example, DEACTIVATE %s", rv->relname)));

		/*
		 * Add object for the CQ's underlying materialization table.
		 */
		relations = lappend(relations, list_make1(makeString(GetMatRelationName(rv->relname))));

		/*
		 * Remove the view from the pipeline_query table
		 */
		simple_heap_delete(pipeline_query, &tuple->t_self);

		ReleaseSysCache(tuple);

		/*
		 * Advance command counter so that later iterations of this loop will
		 * see the changes already made.
		 */
		CommandCounterIncrement();

		/* Remove transition state entry */
		RemoveTStateEntry(rv->relname);
	}

	UpdateStreamQueries(pipeline_query);

	/*
	 * Now we can clean up
	 */
	heap_close(pipeline_query, NoLock);

	/*
	 * Remove the VIEWs and underlying materialization relations
	 * of all CVs.
	 */
	stmt->objects = list_concat(stmt->objects, relations);
	RemoveObjects(stmt);
}

static List *
get_views(Node *where)
{
	CommandDest dest = DestTuplestore;
	Tuplestorestate *store;
	Node *parsetree;
	List *querytree_list;
	PlannedStmt *plan;
	Portal portal;
	DestReceiver *receiver;
	ResTarget *resTarget;
	ColumnRef *colRef;
	SelectStmt *selectStmt;
	TupleTableSlot *slot;
	QueryDesc *queryDesc;
	MemoryContext oldcontext;
	MemoryContext runctx;
	List *views = NIL;

	if (!where)
		return GetAllContinuousViewNames();

	runctx = AllocSetContextCreate(CurrentMemoryContext,
			"CQAutoVacuumContext",
			ALLOCSET_DEFAULT_MINSIZE,
			ALLOCSET_DEFAULT_INITSIZE,
			ALLOCSET_DEFAULT_MAXSIZE);

	oldcontext = MemoryContextSwitchTo(runctx);

	resTarget = makeNode(ResTarget);
	colRef = makeNode(ColumnRef);
	colRef->fields = list_make1(makeString("name"));
	resTarget->val = (Node *) colRef;
	selectStmt =  makeNode(SelectStmt);
	selectStmt->targetList = list_make1(resTarget);
	selectStmt->fromClause = list_make1(makeRangeVar(NULL, "pipeline_query", -1));;
	selectStmt->whereClause = where;
	parsetree = (Node *) selectStmt;

	querytree_list = pg_analyze_and_rewrite(parsetree, NULL,
			NULL, 0);
	plan = pg_plan_query((Query *) linitial(querytree_list), 0, NULL);

	PushActiveSnapshot(GetTransactionSnapshot());

	portal = CreatePortal("__get_continuous_views__", true, true);
	portal->visible = false;

	store = tuplestore_begin_heap(true, true, work_mem);
	receiver = CreateDestReceiver(dest);
	SetTuplestoreDestReceiverParams(receiver, store, PortalGetHeapMemory(portal), true);

	PortalDefineQuery(portal,
			NULL,
			NULL,
			"SELECT",
			list_make1(plan),
			NULL);

	PortalStart(portal, NULL, 0, GetActiveSnapshot());

	(void) PortalRun(portal,
			FETCH_ALL,
			true,
			receiver,
			receiver,
			NULL);

	queryDesc = PortalGetQueryDesc(portal);
	slot = MakeSingleTupleTableSlot(queryDesc->tupDesc);
	foreach_tuple(slot, store)
	{
		bool isnull;
		Datum *tmp;
		char *viewName;

		slot_getallattrs(slot);
		tmp = (Datum *) heap_getattr(slot->tts_tuple, 1, slot->tts_tupleDescriptor, &isnull);
		/*
		 * Must change context back to old context here, because we're going
		 * to be using these RangeVars we create after we reset the runctx.
		 */
		MemoryContextSwitchTo(oldcontext);
		viewName = pstrdup(NameStr(*DatumGetName(tmp)));
		views = lappend(views, makeRangeVar(NULL, viewName, -1));
		oldcontext = MemoryContextSwitchTo(runctx);
	}

	(*receiver->rDestroy) (receiver);
	tuplestore_end(store);

	PortalDrop(portal, false);
	PopActiveSnapshot();

	MemoryContextReset(runctx);
	MemoryContextSwitchTo(oldcontext);

	return views;
}

int
ExecActivateContinuousViewStmt(ActivateContinuousViewStmt *stmt, bool skipActive)
{
	ListCell *lc;
	int success = 0;
	int fail = 0;
	CQProcEntry *entry;
	Relation pipeline_query = heap_open(PipelineQueryRelationId, CQExclusiveLock);
	Oid dboid = InvalidOid;

	if (stmt->dboid)
		dboid = stmt->dboid;

	if (!stmt->views)
		stmt->views = get_views(stmt->whereClause);

	foreach(lc, stmt->views)
	{
		RangeVar *rv = lfirst(lc);
		ListCell *lcwithOptions;
		ContinuousViewState state;
		bool wasInactive = MarkContinuousViewAsActive(rv, pipeline_query);

		/*
		 * If the user tries to activate an active CV, they'll know because
		 * the count of CVs that were activated will be less than they
		 * expected. We only count CVs that go from inactive to active here.
		 */
		if (!wasInactive && skipActive)
			continue;

		GetContinousViewState(rv, &state);

		/*
		 * Update any tuning parameters passed in with the ACTIVATE
		 * command.
		 */
		foreach(lcwithOptions, stmt->withOptions)
		{
			DefElem *elem = (DefElem *) lfirst(lcwithOptions);
			int64 value = intVal(elem->arg);

			if (pg_strcasecmp(elem->defname, CQ_BATCH_SIZE_KEY) == 0)
				state.batchsize = value;
			else if (pg_strcasecmp(elem->defname, CQ_WAIT_MS_KEY) == 0)
				state.maxwaitms = (int32) value;
			else if (pg_strcasecmp(elem->defname, CQ_SLEEP_MS_KEY) == 0)
				state.emptysleepms = (int32) value;
			else if (pg_strcasecmp(elem->defname, CQ_PARALLELISM_KEY) == 0)
				state.parallelism = (int16) value;
		}

		SetContinousViewState(rv, &state, pipeline_query);

		entry = CQProcEntryCreate(state.id, GetProcessGroupSizeFromCatalog(rv));

		/* CQ procs are already running */
		if (entry == NULL)
			continue;

		RunCQProcs(rv->relname, &state, entry, dboid);

		/*
		 * Spin here waiting for the number of waiting CQ related processes
		 * to complete.
		 */
		if (WaitForCQProcsToStart(state.id))
		{
			success++;
			if (continuous_query_crash_recovery)
				EnableCQProcsRecovery(state.id);
		}
		else
		{
			fail++;
			/*
			 * If some of the bg procs failed, mark the continuous view
			 * as inactive and kill any of the remaining bg procs.
			 */
			TerminateCQProcs(state.id);
			CQProcEntryRemove(state.id);
			MarkContinuousViewAsInactive(rv, pipeline_query);
		}
	}

	if (success)
		UpdateStreamReaders(pipeline_query);

	heap_close(pipeline_query, NoLock);

	if (fail)
		elog(LOG, "failed to activate %d continuous view(s)", fail);

	return success;
}

int
ExecDeactivateContinuousViewStmt(DeactivateContinuousViewStmt *stmt)
{
	List *deactivated_cq_ids = NIL;
	ListCell *lc;
	Relation pipeline_query = heap_open(PipelineQueryRelationId, CQExclusiveLock);

	if (!stmt->views)
		stmt->views = get_views(stmt->whereClause);

	foreach(lc, stmt->views)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		ContinuousViewState state;
		CQProcEntry *entry;
		bool wasActive = MarkContinuousViewAsInactive(rv, pipeline_query);

		/* deactivating an inactive CV is a noop */
		if (!wasActive)
			continue;

		GetContinousViewState(rv, &state);

		entry = GetCQProcEntry(state.id);
		if (!entry)
			continue;

		/* Disable recovery and wait for any recovering processes to recover */
		if (continuous_query_crash_recovery)
			DisableCQProcsRecovery(state.id);
		WaitForCQProcsToStart(state.id);

		/* Indicate to the child processes that this CV has been marked for inactivation */
		SetActiveFlag(state.id, false);

		/* This should be a good place to release the waiting latch on the background procs */
		TupleBufferNotify(WorkerTupleBuffer, state.id);
		TupleBufferNotify(CombinerTupleBuffer, state.id);

		/*
		 * Block till all the processes in the group have terminated
		 * and remove the CVMetadata entry.
		 */
		WaitForCQProcsToTerminate(state.id);

		deactivated_cq_ids = lappend_int(deactivated_cq_ids, state.id);

		CQProcEntryRemove(state.id);
	}

	if (deactivated_cq_ids)
	{
		MemoryContext oldcontext;
		bool was_snapshot_set = false;

		/*
		 * In case some CQs were deactivated, we should update the pipeline_stream catalog
		 * table and commit right now. After the commit any insert into a stream
		 * will see the correct version of the pipeline_stream catalog table. The only thing
		 * left to do after that is to unpin any slots in the WorkerTupleBuffer which
		 * might have a reader bit set for a now deactivated CQ.
		 */
		UpdateStreamReaders(pipeline_query);
		heap_close(pipeline_query, NoLock);

		/*
		 * We can get here from the extended or simple query protocols, which may
		 * or may not have set a snapshot at this point, so we need to check.
		 */
		if (ActiveSnapshotSet())
		{
			was_snapshot_set = true;
			PopActiveSnapshot();
		}

		/*
		 * CommitTransactionCommand deletes the TopTransactionContext, so we need to copy
		 * this list into the TopMemoryContext.
		 */
		oldcontext = MemoryContextSwitchTo(TopMemoryContext);
		deactivated_cq_ids = list_copy(deactivated_cq_ids);
		MemoryContextSwitchTo(oldcontext);

		CommitTransactionCommand();

		CQExecutionContext = TopMemoryContext;

		foreach(lc, deactivated_cq_ids)
			TupleBufferDrain(WorkerTupleBuffer, lfirst_int(lc), 0, 1);

		/*
		 * We need to restart a transaction because the executor expects us to be in a
		 * transaction.
		 */
 		StartTransactionCommand();
		if (was_snapshot_set)
			PushActiveSnapshot(GetTransactionSnapshot());
	}
	else
		heap_close(pipeline_query, NoLock);

	return list_length(deactivated_cq_ids);
}

void
ExecTruncateContinuousViewStmt(TruncateStmt *stmt)
{
	ListCell *lc;
	Relation pipeline_query;
	List *views = NIL;

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	/* Ensure that all *relations* are CQs. */
	foreach(lc, stmt->relations)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		HeapTuple tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));
		Form_pipeline_query row;

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
					errmsg("continuous view \"%s\" does not exist", rv->relname)));

		row = (Form_pipeline_query) GETSTRUCT(tuple);
		if (row->state == PIPELINE_QUERY_STATE_ACTIVE)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_CONTINUOUS_VIEW_STATE),
					 errmsg("continuous view \"%s\" is currently active", rv->relname),
					 errhint("Only inactive continuous views can be truncated, which requires deactivating them. For example, DEACTIVATE %s", rv->relname)));

		ReleaseSysCache(tuple);

		views = lappend(views, rv->relname);
		rv->relname = GetMatRelationName(rv->relname);
	}

	/* Reset all CQ level transition state */
	foreach(lc, views)
		ResetTStateEntry((char *) lfirst(lc));

	/* Call TRUNCATE on the backing view table(s). */
	stmt->objType = OBJECT_TABLE;
	ExecuteTruncate(stmt);

	heap_close(pipeline_query, NoLock);
}
