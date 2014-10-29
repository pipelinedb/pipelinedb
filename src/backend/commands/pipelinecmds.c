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
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "catalog/toasting.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqslidingwindow.h"
#include "pipeline/streambuf.h"
#include "regex/regex.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "pipeline/cvmetadata.h"

/* Whether or not to block till the events are consumed by a cv
   Also used to represent whether the activate/deactivate are to be
   synchronous
 */
bool DebugSyncStreamInsert;

#define CQ_TABLE_SUFFIX "_pdb"
#define DEFAULT_TYPEMOD -1

/*
 * Appends a suffix to a string, ensuring that the result fits
 * in a NameData
 */
static char *
append_suffix(char *base, char *suffix)
{
	char relname[NAMEDATALEN];

	/* we truncate the CV name if needed */
	int chunk = Min(strlen(base), NAMEDATALEN - strlen(suffix));
	strcpy(relname, base);
	strcpy(&relname[chunk], suffix);

	return strdup(relname);
}

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
 * GetCQMatRelationName
 *
 * Returns the name of the given CV's underlying materialization table
 */
char *
GetCQMatRelationName(char *cvname)
{
	/*
	 * The name of the underlying materialized table should
	 * be CV name suffixed with "_pdb".
	 */
	return append_suffix(cvname, CQ_TABLE_SUFFIX);
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
	mat_relation = makeRangeVar(view->schemaname, GetCQMatRelationName(view->relname), -1);

	/*
	 * Check if CV already exists?
	 */
	if (IsAContinuousView(view))
		elog(ERROR, "continuous view \"%s\" already exists", view->relname);

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

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		colname = pstrdup(tle->resname);


		/*
		 * allowSystemTableMods is a global flag that, when true, allows certain column types
		 * to be created. We need it set to true to create some hidden state columns. In particular,
		 * ones with a type of anyarray.
		 */
		saveAllowSystemTableMods = allowSystemTableMods;
		allowSystemTableMods = true;

		/*
		 * Set typeOid and typemod. The name of the type is derived while
		 * generating query
		 */
		coldef = make_cv_columndef(colname, exprType((Node *) tle->expr), exprTypmod((Node *) tle->expr));
		allowSystemTableMods = saveAllowSystemTableMods;
		tableElts = lappend(tableElts, coldef);

		/*
		 * If this column requires state to support incremental transitions, create it. Note: since this
		 * column isn't in the target list, it won't be visible when selecting from this CV,
		 * which will have an overlay view that only exposes target list columns.
		 */
		hiddentype = GetCombineStateColumnType(tle);
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

	allowSystemTableMods = true;
	reloid = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid);
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);

	AlterTableCreateToastTable(reloid, toast_options, AccessExclusiveLock);

	/*
	 * Create a VIEW over the CQ materialization relation which exposes
	 * only the columns that users expect. This is needed primarily for two
	 * reasons:
	 *
	 * 1. Sliding window queries. For such queries, this VIEW filters events out
	 *    of the window (that have not been GC'd).
	 * 2. Some aggregate operators require storing some additional state along
	 *    with partial results and this VIEW filters out such hidden
	 *    columns.
	 * 3. View also computes expressions on aggregates.
	 */
	FixAggArgForCQView(viewselect, workerselect, mat_relation);
	viewselect->fromClause = list_make1(mat_relation);
	view_stmt = makeNode(ViewStmt);
	view_stmt->view = view;
	view_stmt->query = (Node *) viewselect;
	DefineView(view_stmt, querystring);

	/*
	 * Now save the underlying query in the `pipeline_queries` catalog
	 * relation.
	 */
	RegisterContinuousView(view, querystring);
}

/*
 * DumpState
 *
 * Dumps the state of a given object by sending tuples which describe
 * the state back to the client
 */
void
ExecDumpStmt(DumpStmt *stmt)
{
	char *name = NULL;
	if (stmt->name)
		name = stmt->name->relname;

	elog(LOG, "DUMP \"%s\"", name);
}

/*
 * RemoveContinuousViewFromCatalog
 *
 * Drops the query row in the pipeline_queries catalog table.
 */
void
ExecDropContinuousViewStmt(DropStmt *stmt)
{
	Relation pipeline_queries;
	List *relations = NIL;
	ListCell *item;

	/*
	 * Scan the pipeline_queries relation to find the OID of the views(s) to be
	 * deleted.
	 */
	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);

	foreach(item, stmt->objects)
	{
		RangeVar *rv = makeRangeVarFromNameList((List *) lfirst(item));
		HeapTuple tuple;
		Form_pipeline_queries row;

		tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(rv->relname));
		if (!HeapTupleIsValid(tuple))
		{
			elog(ERROR, "continuous view \"%s\" does not exist", rv->relname);
		}

		row = (Form_pipeline_queries) GETSTRUCT(tuple);
		if (row->state == PIPELINE_QUERY_STATE_ACTIVE)
		{
			elog(ERROR, "continuous view \"%s\" is currently active; can't be dropped", rv->relname);
		}

		/*
		 * Remove the view from the pipeline_queries table
		 */
		simple_heap_delete(pipeline_queries, &tuple->t_self);

		ReleaseSysCache(tuple);

		/*
		 * Advance command counter so that later iterations of this loop will
		 * see the changes already made.
		 */
		CommandCounterIncrement();

		/*
		 * Add object for the CQ's underlying materialization table.
		 */
		relations = lappend(relations, list_make1(makeString(GetCQMatRelationName(rv->relname))));
	}

	/*
	 * Now we can clean up
	 */
	heap_close(pipeline_queries, RowExclusiveLock);

	/*
	 * Remove the VIEWs and underlying materialization relations
	 * of all CVs.
	 */
	stmt->objects = list_concat(stmt->objects, relations);
	RemoveObjects(stmt);
}

static
void
RunContinuousQueryProcs(const char *cvname, ContinuousViewState *state)
{
	int32 id;
	Assert(state);
	id = state->id;

	RunContinuousQueryProcess(CQCombiner, cvname, state);
	RunContinuousQueryProcess(CQWorker, cvname, state);
	RunContinuousQueryProcess(CQGarbageCollector, cvname, state);

	/*
	 * Spin here waiting for the number of waiting CQ related processes
	 * to complete.
	 */
	WaitForCQProcessStart(id);
}

int
ExecActivateContinuousViewStmt(ActivateContinuousViewStmt *stmt)
{
	ListCell *lc;
	CVMetadata *entry;
	int count = 0;

	foreach(lc, stmt->views)
	{
		RangeVar *rv = lfirst(lc);
		ListCell *lcwithOptions;
		ContinuousViewState state;
		bool wasInactive = MarkContinuousViewAsActive(rv);

		/*
		 * If the user tries to activate an active CV, they'll know because
		 * the count of CVs that were activated will be less than they
		 * expected. We only count CVs that go from inactive to active here.
		 */
		if (!wasInactive)
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

		SetContinousViewState(rv, &state);

		/*
		 * Initialize the metadata entry for the CV
		 * Input would be an id (used as key) and a Process group size.
		 *
		 * Here we don't have to worry about racing transactions.
		 * If we see the CV as inactive but another transaction has created
		 * its CVMetadata entry, the next call will fail--albeit with a weird
		 * message saying the CV is being deactivated. That's only one of the
		 * cases when this can happen.
		 */
		entry = EntryAlloc(state.id, GetProcessGroupSizeFromCatalog(rv));

		if (entry == NULL)
			elog(ERROR, "continuous view \"%s\" is being deactivated",
					rv->relname);

		RunContinuousQueryProcs(rv->relname, &state);
		count++;
	}

	return count;
}

int
ExecDeactivateContinuousViewStmt(DeactivateContinuousViewStmt *stmt)
{
	int count = 0;
	ListCell *lc;

	foreach(lc, stmt->views)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		ContinuousViewState state;
		bool wasActive = MarkContinuousViewAsInactive(rv);

		/* deactivating an inactive CV is a noop */
		if (!wasActive)
			continue;

		GetContinousViewState(rv, &state);

		/* If the another transaction which deactivated this CV hasn't
		 * committed yet, we might still see it as active. Since there's only
		 * one postmaster, the previous transaction's process could have
		 * removed the CVMetadata--transactions don't protect it. */
		if (GetCVMetadata(state.id) == NULL)
			continue;

		/* Indicate to the child processes that this CV has been marked for inactivation */
		SetActiveFlag(state.id, false);

		/* This should be a good place to release the waiting latch on the worker */
		SetStreamBufferLatch(state.id);

		/*
		 * Block till all the processes in the group have terminated
		 * and remove the CVMetadata entry.
		 */
		WaitForCQProcessEnd(state.id);
		count++;

		EntryRemove(state.id);
	}

	/*
	 * This will stop the stream buffer from assigning new events to this
	 * continuous view immediately, even if the worker procs actually take a few
	 * more seconds to shut themselves down. This seems like the behavior we want.
	 */
	NotifyUpdateGlobalStreamBuffer();

	return count;
}

void
ExecTruncateContinuousViewStmt(TruncateStmt *stmt)
{
	ListCell *lc;

	/* Ensure that all *relations* are CQs. */
	foreach(lc, stmt->relations)
	{
		RangeVar *rv = (RangeVar *) lfirst(lc);
		if (!IsAContinuousView(rv))
			elog(ERROR, "continuous view \"%s\" does not exist", rv->relname);

		rv->relname = GetCQMatRelationName(rv->relname);
	}

	/* Call TRUNCATE on the backing view table(s). */
	stmt->objType = OBJECT_TABLE;
	ExecuteTruncate(stmt);

	/* TODO(usmanm): Do we need to do any other state clean up? */
}
