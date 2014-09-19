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

	rel = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
	scandesc = heap_beginscan(rel, SnapshotAny, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_queries catrow = (Form_pipeline_queries) GETSTRUCT(tup);
		id = catrow->id > id ? catrow->id : id;
	}

	heap_endscan(scandesc);
	heap_close(rel, RowExclusiveLock);

	return id + 1;
}

/*
 * AddQuery
 *
 * Adds a REGISTERed query to the pipeline_queries catalog table
 */
void
AddQuery(const char *rawname, const char *query, char state)
{
	Relation	pipeline_queries;
	HeapTuple	tup;
	bool nulls[Natts_pipeline_queries];
	Datum values[Natts_pipeline_queries];
	NameData name;

	if (!rawname)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!query)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	if (!(state == PIPELINE_QUERY_STATE_ACTIVE || state == PIPELINE_QUERY_STATE_INACTIVE))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("invalid state: '%c'", state)));

	nulls[0] = false;
	nulls[1] = false;
	nulls[2] = false;
	nulls[3] = false;

	values[0] = (Datum) NULL;
	values[1] = (Datum) NULL;
	values[2] = (Datum) NULL;
	values[3] = (Datum) NULL;

	pipeline_queries = heap_open(PipelineQueriesRelationId, AccessExclusiveLock);

	namestrcpy(&name, rawname);
	values[Anum_pipeline_queries_id - 1] = Int32GetDatum(get_next_id());
	values[Anum_pipeline_queries_name - 1] = NameGetDatum(&name);
	values[Anum_pipeline_queries_query - 1] = CStringGetTextDatum(query);
	values[Anum_pipeline_queries_state - 1] = CharGetDatum(state);

	tup = heap_form_tuple(pipeline_queries->rd_att, values, nulls);

	simple_heap_insert(pipeline_queries, tup);
	CatalogUpdateIndexes(pipeline_queries, tup);

	heap_freetuple(tup);
	heap_close(pipeline_queries, AccessExclusiveLock);
}


/*
 * Retrieves a REGISTERed query from the pipeline_queries catalog table
 */
char *
GetQueryString(RangeVar *rvname, int *cqid, bool select_only)
{
	HeapTuple	tuple;
	Form_pipeline_queries row;
	NameData name;

	char *result;

	namestrcpy(&name, rvname->relname);
	tuple = SearchSysCache1(PIPELINEQUERIESNAME, NameGetDatum(&name));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "continuous view \"%s\" does not exist", rvname->relname);

	row = (Form_pipeline_queries) GETSTRUCT(tuple);

	result = TextDatumGetCString(&(row->query));

	if (cqid != NULL)
		*cqid = DatumGetInt32(row->id);

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


/*
 * SetQueryState
 *
 * Sets a query's state
 */
void
SetQueryState(RangeVar *name, char state)
{
	if (!name->relname)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!(state == PIPELINE_QUERY_STATE_ACTIVE || state == PIPELINE_QUERY_STATE_INACTIVE))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("invalid state: '%c'", state)));

	elog(LOG, "Set state for %s to '%c'", name->relname, state);
}
