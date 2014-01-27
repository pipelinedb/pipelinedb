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
#include "catalog/indexing.h"
#include "catalog/pipeline_queries.h"
#include "catalog/pipeline_queries_fn.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/syscache.h"


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

	values[0] = (Datum) NULL;
	values[1] = (Datum) NULL;
	values[2] = (Datum) NULL;

	namestrcpy(&name, rawname);
	values[Anum_pipeline_queries_name - 1] = NameGetDatum(&name);
	values[Anum_pipeline_queries_query - 1] = CStringGetTextDatum(query);
	values[Anum_pipeline_queries_state - 1] = CharGetDatum(state);

	pipeline_queries = heap_open(PipelineQueriesRelationId, RowExclusiveLock);
	tup = heap_form_tuple(pipeline_queries->rd_att, values, nulls);

	simple_heap_insert(pipeline_queries, tup);
	CatalogUpdateIndexes(pipeline_queries, tup);

	heap_freetuple(tup);
	heap_close(pipeline_queries, RowExclusiveLock);
}


/*
 * Retrieves a REGISTERed query from the pipeline_queries catalog table
 */
char *
GetQueryString(RangeVar *rvname)
{
	HeapTuple	tuple;
	Form_pipeline_queries row;
	NameData name;

	char *result;

	namestrcpy(&name, rvname->relname);
	tuple = SearchSysCache1(PIPELINEQUERIES, NameGetDatum(&name));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "no registered queries found with name '%s'", rvname->relname);

	row = (Form_pipeline_queries) GETSTRUCT(tuple);

	result = TextDatumGetCString(&(row->query));
	ReleaseSysCache(tuple);

	return result;
}


/*
 * SetQueryState
 *
 * Sets a querie's state
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
}
