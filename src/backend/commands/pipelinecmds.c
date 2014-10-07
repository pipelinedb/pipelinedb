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
#include "commands/pipelinecmds.h"
#include "commands/tablecmds.h"
#include "utils/builtins.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "catalog/toasting.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/streambuf.h"
#include "regex/regex.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * CreateContinuousView
 *
 * Creates a table for backing the result of the continuous query,
 * and stores the query in a catalog table.
 */
void
CreateContinuousView(CreateContinuousViewStmt *stmt, const char *querystring)
{
	CreateStmt *create_stmt;
	Query *query;
	RangeVar *relation;
	IntoClause *into;
	List *tableElts = NIL;
	List *tlist;
	ListCell *lc;
	ListCell *col;
	Oid reloid;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	SelectStmt *select_stmt;

	relation = stmt->into->rel;

	create_stmt = makeNode(CreateStmt);
	create_stmt->relation = relation;
	into = stmt->into;

	select_stmt = (SelectStmt *) copyObject(stmt->query);

	// Analyze the SelectStmt portion of the CreateContinuousViewStmt to make
	// sure it's well-formed.
	query = parse_analyze(stmt->query, querystring, 0, 0);

	// Transform the SelectStmt to add any ColRefs to the targetList
	// that need to be kept around for sliding window queries.
	select_stmt = transformSelectStmtForCQWorker(select_stmt);

	query = parse_analyze((Node *) select_stmt, querystring, 0, 0);
	tlist = query->targetList;

	/*
	 * Build a list of columns from the SELECT statement that we
	 * can use to create a table with
	 */
	/* TODO(usmanm): This into business is janky. Revisit post-alpha. */
	lc = list_head(into->colNames);

	foreach(col, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(col);
		ColumnDef   *coldef;
		TypeName    *typename;

		/* Ignore junk columns from the targetlist */
		if (tle->resjunk)
			continue;

		coldef = makeNode(ColumnDef);
		typename = makeNode(TypeName);

		/* Take the column name specified if any */
		if (lc)
		{
			coldef->colname = strVal(lfirst(lc));
			lc = lnext(lc);
		}
		else
			coldef->colname = pstrdup(tle->resname);

		coldef->inhcount = 0;
		coldef->is_local = true;
		coldef->is_not_null = false;
		coldef->raw_default = NULL;
		coldef->cooked_default = NULL;
		coldef->constraints = NIL;

		/*
		 * Set typeOid and typemod. The name of the type is derived while
		 * generating query
		 */
		typename->typeOid = exprType((Node *)tle->expr);
		typename->typemod = exprTypmod((Node *)tle->expr);

		coldef->typeName = typename;

		tableElts = lappend(tableElts, coldef);
	}

	create_stmt->tableElts = tableElts;
	create_stmt->tablespacename = stmt->into->tableSpaceName;
	create_stmt->oncommit = stmt->into->onCommit;
	create_stmt->options = stmt->into->options;

	/*
	 * Actually create the relation
	 */
	reloid = DefineRelation(create_stmt, RELKIND_RELATION, InvalidOid);
	CommandCounterIncrement();

	toast_options = transformRelOptions((Datum) 0, create_stmt->options, "toast",
			validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options,
						   true);

	AlterTableCreateToastTable(reloid, toast_options, AccessExclusiveLock);

	/*
	 * Now save the underlying query for ACTIVATION/DEACTIVATION
	 */
	RegisterContinuousView(relation, querystring);
}

/*
 * DumpState
 *
 * Dumps the state of a given object by sending tuples which describe
 * the state back to the client
 */
void
DumpState(DumpStmt *stmt)
{
	char *name = NULL;
	if (stmt->name)
		name = stmt->name->relname;

	elog(LOG, "DUMP \"%s\"", name);
}

/*
 * DropContinuousView
 *
 * Drops the query row in the pipeline_queries catalog table.
 */
void
DropContinuousView(DropStmt *stmt)
{
	Relation pipeline_queries;
	ListCell *item;

	/*
	 * Scan the pipeline_queries relation to find the OID of the views(s) to be
	 * deleted.
	 */
	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);

	foreach(item, stmt->objects)
	{
		RangeVar *view_name = makeRangeVarFromNameList((List *) lfirst(item));
		HeapTuple	tuple;
		Form_pipeline_queries row;

		tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(view_name->relname));
		if (!HeapTupleIsValid(tuple))
		{
			elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist", view_name->relname);
		}

		row = (Form_pipeline_queries) GETSTRUCT(tuple);
		if (row->state == PIPELINE_QUERY_STATE_ACTIVE)
		{
			elog(ERROR, "CONTINUOUS VIEW \"%s\" is currently active; can't be dropped", view_name->relname);
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
	}

	/*
	 * Now we can clean up; but keep locks until commit.
	 */
	heap_close(pipeline_queries, NoLock);
}

static
void
RunContinuousQueryProcs(const char *cvname, ContinuousViewState *state)
{
	RunContinuousQueryProcess(CQCombiner, cvname, state);
	RunContinuousQueryProcess(CQWorker, cvname, state);
	RunContinuousQueryProcess(CQGarbageCollector, cvname, state);
}

void
ActivateContinuousView(ActivateContinuousViewStmt *stmt)
{
	RangeVar *rv = linitial(stmt->views);
	ListCell *lc;
	ContinuousViewState state;

	if (IsContinuousViewActive(rv))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" is already active.",
				rv->relname);

	GetContinousViewState(rv, &state);

	/*
	 * Update any tuning parameters passed in with the ACTIVATE
	 * command.
	 */
	foreach(lc, stmt->withOptions)
	{
		DefElem *elem = (DefElem *) lfirst(lc);
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

	RunContinuousQueryProcs(rv->relname, &state);
}

void
DeactivateContinuousView(DeactivateContinuousViewStmt *stmt)
{
	MarkContinuousViewAsInactive((RangeVar *) linitial(stmt->views));

	/*
	 * This will stop the stream buffer from assigning new events to this
	 * continuous view immediately, even if the worker procs actually take a few
	 * more seconds to shut themselves down. This seems like the behavior we want.
	 */
	NotifyUpdateGlobalStreamBuffer();
}

void
ClearContinuousView(ClearContinuousViewStmt *stmt)
{
	/* TODO(usmanm): Do we need to do any other state clean up? */
	/* Call TRUNCATE on the backing view table. */
	TruncateStmt *truncate_stmt = makeNode(TruncateStmt);
	truncate_stmt->relations = list_make1((RangeVar *) linitial(stmt->views));
	truncate_stmt->behavior = DROP_RESTRICT;
	truncate_stmt->restart_seqs = false;
	ExecuteTruncate(truncate_stmt);
}
