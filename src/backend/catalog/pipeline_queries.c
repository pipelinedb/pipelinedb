/*-------------------------------------------------------------------------
 *
 * pipeline_queries.c
 *	  routines to support manipulation of the pipeline_queries relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_queries.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * skip
 *
 * Skips to the end of a substring within another string, beginning
 * at the given position
 */
static int
skip(const char *needle, const char *haystack, int pos)
{
	while(pg_strncasecmp(needle, &haystack[pos++], strlen(needle)) != 0 &&
			pos < strlen(haystack));

	if (pos == strlen(haystack))
		return -1;

	return pos + strlen(needle);
}

/*
 * get_next_id
 *
 * Gets the smallest possible id to assign to the next continuous view.
 * We keep this minimal so that we can minimize the size of bitmaps used
 * to tag stream buffer events with.
 */
static int32
get_next_id(void)
{
	Relation rel;
	HeapScanDesc scandesc;
	HeapTuple tup;
	int32 id = -1;

	rel = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);
	scandesc = heap_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_queries catrow = (Form_pipeline_queries) GETSTRUCT(tup);
		id = catrow->id > id ? catrow->id : id;
	}

	heap_endscan(scandesc);
	heap_close(rel, AccessExclusiveLock);

	return id + 1;
}

/*
 * RegisterContinuousView
 *
 * Adds a CV to the `pipeline_queries` catalog table.
 */
void
RegisterContinuousView(RangeVar *name, const char *query_string)
{
	Relation	pipeline_queries;
	HeapTuple	tup;
	bool 		nulls[Natts_pipeline_queries];
	Datum 		values[Natts_pipeline_queries];
	NameData 	name_data;

	if (!name)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!query_string)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	MemSet(nulls, 0, Natts_pipeline_queries);

	pipeline_queries = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);

	namestrcpy(&name_data, name->relname);
	values[Anum_pipeline_queries_id - 1] = Int32GetDatum(get_next_id());
	values[Anum_pipeline_queries_name - 1] = NameGetDatum(&name_data);
	values[Anum_pipeline_queries_query - 1] = CStringGetTextDatum(query_string);

	/* Use default values for state and tuning parameters. */
	values[Anum_pipeline_queries_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_INACTIVE);
	values[Anum_pipeline_queries_batchsize - 1] = Int64GetDatum(CQ_DEFAULT_BATCH_SIZE);
	values[Anum_pipeline_queries_maxwaitms - 1] = Int32GetDatum(CQ_DEFAULT_WAIT_MS);
	values[Anum_pipeline_queries_emptysleepms - 1] = Int32GetDatum(CQ_DEFAULT_SLEEP_MS);
	values[Anum_pipeline_queries_parallelism - 1] = Int16GetDatum(CQ_DEFAULT_PARALLELISM);

	tup = heap_form_tuple(pipeline_queries->rd_att, values, nulls);

	simple_heap_insert(pipeline_queries, tup);
	CatalogUpdateIndexes(pipeline_queries, tup);
	CommandCounterIncrement();

	heap_freetuple(tup);
	heap_close(pipeline_queries, AccessExclusiveLock);
}

/*
 * DeregisterContinuousView
 *
 * Removes the CV entry from the `pipeline_queries` catalog table.
 */
void
DeregisterContinuousView(RangeVar *name)
{
	Relation pipeline_queries;
	HeapTuple tup;

	pipeline_queries = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);
	tup = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist", name->relname);

	simple_heap_delete(pipeline_queries, &tup->t_self);
	CommandCounterIncrement();

	ReleaseSysCache(tup);
	heap_close(pipeline_queries, NoLock);
}

/*
 * MarkContinuousViewAsActive
 *
 * Updates the parameters of an already registered CV in the `pipeline_queries`
 * catalog table and sets the state to *ACTIVE*. If the CV is already active,
 * nothing is changed.
 *
 * Returns whether the catalog table was updated or not.
 */
bool
MarkContinuousViewAsActive(RangeVar *name)
{
	Relation pipeline_queries;
	HeapTuple tuple;
	HeapTuple newtuple;
	Form_pipeline_queries row;
	bool nulls[Natts_pipeline_queries];
	bool replaces[Natts_pipeline_queries];
	Datum values[Natts_pipeline_queries];
	bool alreadyActive = true;

	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
	tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist.",
				name->relname);

	row = (Form_pipeline_queries) GETSTRUCT(tuple);

	if (row->state != PIPELINE_QUERY_STATE_ACTIVE)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pipeline_queries_state - 1] = true;
		values[Anum_pipeline_queries_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_ACTIVE);

		newtuple = heap_modify_tuple(tuple, pipeline_queries->rd_att,
				values, nulls, replaces);
		simple_heap_update(pipeline_queries, &newtuple->t_self, newtuple);
		CommandCounterIncrement();

		alreadyActive = false;
	}

	ReleaseSysCache(tuple);
	heap_close(pipeline_queries, NoLock);

	return !alreadyActive;
}

/*
 * MarkContinuousViewAsInactive
 *
 * If the CV is not already marked as INACTIVE in the `pipeline_queries`
 * catalog table, mark it as INACTIVE. If the CV is already inactive,
 * nothing is changed.
 *
 * Returns whether the catalog table was updated or not.
 */
bool
MarkContinuousViewAsInactive(RangeVar *name)
{
	Relation pipeline_queries;
	HeapTuple tuple;
	HeapTuple newtuple;
	Form_pipeline_queries row;
	bool nulls[Natts_pipeline_queries];
	bool replaces[Natts_pipeline_queries];
	Datum values[Natts_pipeline_queries];

	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
	tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist.",
				name->relname);

	row = (Form_pipeline_queries) GETSTRUCT(tuple);

	if (row->state != PIPELINE_QUERY_STATE_INACTIVE)
	{
		MemSet(values, 0, sizeof(Natts_pipeline_queries));
		MemSet(nulls, false, sizeof(Natts_pipeline_queries));
		MemSet(replaces, false, sizeof(Natts_pipeline_queries));

		replaces[Anum_pipeline_queries_state - 1] = true;
		values[Anum_pipeline_queries_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_INACTIVE);

		newtuple = heap_modify_tuple(tuple, pipeline_queries->rd_att,
				values, nulls, replaces);
		simple_heap_update(pipeline_queries, &newtuple->t_self, newtuple);
		CommandCounterIncrement();
	}

	ReleaseSysCache(tuple);
	heap_close(pipeline_queries, NoLock);

	return row->state != PIPELINE_QUERY_STATE_INACTIVE;
}

/*
 * GetContinuousViewParams
 *
 * Fetch the parameters for the CV from the `pipeline_queries` catalog table.
 */
void
GetContinousViewState(RangeVar *name, ContinuousViewState *cv_state)
{
	HeapTuple tuple;
	Form_pipeline_queries row;

	if (!cv_state)
		return;

	tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist.",
				name->relname);

	row = (Form_pipeline_queries) GETSTRUCT(tuple);

	ReleaseSysCache(tuple);

	cv_state->id = row->id;
	cv_state->state = row->state;
	cv_state->batchsize = row->batchsize;
	cv_state->maxwaitms = row->maxwaitms;
	cv_state->emptysleepms = row->emptysleepms;
	cv_state->parallelism = row->parallelism;
}

/*
 * SetContinuousViewParams
 *
 * Set the tuning parameters for the CV in the `pipeline_queries`
 * catalog table by reading them from cv_state.
 */
void
SetContinousViewState(RangeVar *name, ContinuousViewState *cv_state)
{
	Relation pipeline_queries;
	HeapTuple tuple;
	HeapTuple newtuple;
	bool nulls[Natts_pipeline_queries];
	bool replaces[Natts_pipeline_queries];
	Datum values[Natts_pipeline_queries];

	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
	tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "CONTINUOUS VIEW \"%s\" does not exist.",
				name->relname);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	replaces[Anum_pipeline_queries_batchsize - 1] = true;
	values[Anum_pipeline_queries_batchsize - 1] = Int64GetDatum(cv_state->batchsize);

	replaces[Anum_pipeline_queries_maxwaitms - 1] = true;
	values[Anum_pipeline_queries_maxwaitms - 1] = Int32GetDatum(cv_state->maxwaitms);

	replaces[Anum_pipeline_queries_emptysleepms - 1] = true;
	values[Anum_pipeline_queries_emptysleepms - 1] = Int32GetDatum(cv_state->emptysleepms);

	replaces[Anum_pipeline_queries_parallelism - 1] = true;
	values[Anum_pipeline_queries_parallelism - 1] = Int16GetDatum(cv_state->parallelism);

	newtuple = heap_modify_tuple(tuple, pipeline_queries->rd_att,
			values, nulls, replaces);
	simple_heap_update(pipeline_queries, &newtuple->t_self, newtuple);
	CommandCounterIncrement();

	ReleaseSysCache(tuple);
	heap_close(pipeline_queries, NoLock);
}

bool
IsContinuousViewActive(RangeVar *name)
{
	HeapTuple tuple;
	Form_pipeline_queries row;
	bool isActive;

	tuple = SearchSysCache1(PIPELINEQUERIESNAME, CStringGetDatum(name->relname));
	/* If the HeapTuple is invalid, consider the CV inactive. */
	isActive = HeapTupleIsValid(tuple);
	if (isActive)
	{
		row = (Form_pipeline_queries) GETSTRUCT(tuple);
		isActive = (row->state ==
				PIPELINE_QUERY_STATE_ACTIVE);
	}
	ReleaseSysCache(tuple);

	return isActive;
}

/*
 * Retrieves a REGISTERed query from the pipeline_queries catalog table
 */
char *
GetQueryString(RangeVar *rvname, bool select_only)
{
	HeapTuple tuple;
	NameData name;
	Datum tmp;
	bool isnull;
	char *result;

	namestrcpy(&name, rvname->relname);
	tuple = SearchSysCache1(PIPELINEQUERIESNAME, NameGetDatum(&name));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "continuous view \"%s\" does not exist", rvname->relname);

	tmp = SysCacheGetAttr(PIPELINEQUERIESNAME, tuple, Anum_pipeline_queries_query, &isnull);
	result = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);

	/* do we only want the return the SELECT portion of statement? */
	if (select_only)
	{
		/*
		 * Technically the CV could be named "create" or "continuous",
		 * so it's not enough to simply advance to the CV name. We need
		 * to skip past the keywords first. Note that these find() calls
		 * should never return -1 for this string since it's already been
		 * validated.
		 */
		int trimmedlen;
		char *trimmed;
		int pos = skip("CREATE", result, 0);
		pos = skip("CONTINUOUS", result, pos);
		pos = skip("VIEW", result, pos);
		pos = skip(rvname->relname, result, pos);
		pos = skip("AS", result, pos);

		trimmedlen = strlen(result) - pos + 1;
		trimmed = palloc(trimmedlen);

		memcpy(trimmed, &result[pos], trimmedlen);
		pfree(result);

		result = trimmed;
	}

	return result;
}
