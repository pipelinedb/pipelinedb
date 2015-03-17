/*-------------------------------------------------------------------------
 *
 * pipeline_query.c
 *	  routines to support manipulation of the pipeline_query relation
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_query_fn.h"
#include "catalog/pipeline_tstate_fn.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
#include "pipeline/cqproc.h"
#include "pipeline/miscutils.h"
#include "postmaster/bgworker.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

/*
 * compare_int32s
 */
static int
compare_int32s (const void *a, const void *b)
{
  const int32 *ia = (const int32 *) a;
  const int32 *ib = (const int32 *) b;

  return (*ia > *ib) - (*ia < *ib);
}

/*
 * get_next_id
 *
 * Gets the smallest possible id to assign to the next continuous view.
 * We keep this minimal so that we can minimize the size of bitmaps used
 * to tag stream buffer events with.
 */
static int32
get_next_id(Relation rel)
{
	HeapScanDesc	scandesc;
	HeapTuple		tup;
	int32			id = 0;
	List			*idsList = NIL;
	ListCell		*lc;

	scandesc = heap_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		idsList = lappend(idsList, (void *) Int32GetDatum(row->id));
	}

	heap_endscan(scandesc);

	if (idsList != NIL)
	{
		int32 ids[idsList->length];
		int i = 0;
		foreach(lc, idsList)
		{
			ids[i] = DatumGetInt32(lfirst(lc));
			i++;
		}

		qsort(ids, idsList->length, sizeof(int32), &compare_int32s);

		for (id = 0; id < idsList->length; id++)
		{
			if (ids[id] > id)
			{
				break;
			}
		}
	}

	return id;
}

/*
 * GetAllContinuousViewNames
 *
 * Retrieve a List of all continuous view names, regardless of their state
 */
List *
GetAllContinuousViewNames(void)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;
	List *result = NIL;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		char *s = pstrdup(NameStr(row->name));
		RangeVar *rv = makeRangeVar(NULL, s, -1);

		result = lappend(result, rv);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);

	return result;
}

/*
 * RegisterContinuousView
 *
 * Adds a CV to the `pipeline_query` catalog table.
 */
void
RegisterContinuousView(RangeVar *name, const char *query_string, RangeVar* matrelname, bool gc, bool long_xact)
{
	Relation	pipeline_query;
	HeapTuple	tup;
	bool 		nulls[Natts_pipeline_query];
	Datum 		values[Natts_pipeline_query];
	NameData 	name_data;
	NameData	matrelname_data;

	if (!name)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!query_string)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	MemSet(nulls, 0, sizeof(nulls));

	pipeline_query = heap_open(PipelineQueryRelationId, AccessExclusiveLock);

	namestrcpy(&name_data, name->relname);
	values[Anum_pipeline_query_id - 1] = Int32GetDatum(get_next_id(pipeline_query));
	values[Anum_pipeline_query_name - 1] = NameGetDatum(&name_data);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_string);

	/* Use default values for state and tuning parameters. */
	values[Anum_pipeline_query_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_INACTIVE);
	values[Anum_pipeline_query_batchsize - 1] = Int64GetDatum(CQ_DEFAULT_BATCH_SIZE);
	values[Anum_pipeline_query_maxwaitms - 1] = Int32GetDatum(CQ_DEFAULT_WAIT_MS);
	values[Anum_pipeline_query_emptysleepms - 1] = Int32GetDatum(CQ_DEFAULT_SLEEP_MS);
	values[Anum_pipeline_query_parallelism - 1] = Int16GetDatum(CQ_DEFAULT_PARALLELISM);

	/* Copy matrelname */
	namestrcpy(&matrelname_data, matrelname->relname);
	values[Anum_pipeline_query_matrelname - 1] = NameGetDatum(&matrelname_data);

	/* Copy flags */
	values[Anum_pipeline_query_gc - 1] = BoolGetDatum(gc);
	values[Anum_pipeline_query_long_xact - 1] = BoolGetDatum(long_xact);

	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	simple_heap_insert(pipeline_query, tup);
	CatalogUpdateIndexes(pipeline_query, tup);
	CommandCounterIncrement();

	/* Create transition state entry */
	CreateTStateEntry(name->relname);

	heap_freetuple(tup);
	heap_close(pipeline_query, NoLock);
}

/*
 * MarkContinuousViewAsActive
 *
 * Updates the parameters of an already registered CV in the `pipeline_query`
 * catalog table and sets the state to *ACTIVE*. If the CV is already active,
 * nothing is changed.
 *
 * Returns whether the catalog table was updated or not.
 */
bool
MarkContinuousViewAsActive(RangeVar *rv, Relation pipeline_query)
{
	HeapTuple tuple;
	HeapTuple newtuple;
	Form_pipeline_query row;
	bool nulls[Natts_pipeline_query];
	bool replaces[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	if (row->state != PIPELINE_QUERY_STATE_ACTIVE)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pipeline_query_state - 1] = true;
		values[Anum_pipeline_query_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_ACTIVE);

		newtuple = heap_modify_tuple(tuple, pipeline_query->rd_att,
				values, nulls, replaces);

		simple_heap_update(pipeline_query, &newtuple->t_self, newtuple);
		CatalogUpdateIndexes(pipeline_query, newtuple);

		CommandCounterIncrement();
	}

	ReleaseSysCache(tuple);

	return row->state != PIPELINE_QUERY_STATE_ACTIVE;
}

/*
 * MarkContinuousViewAsInactive
 *
 * If the CV is not already marked as INACTIVE in the `pipeline_query`
 * catalog table, mark it as INACTIVE. If the CV is already inactive,
 * nothing is changed.
 *
 * Returns whether the catalog table was updated or not.
 */
bool
MarkContinuousViewAsInactive(RangeVar *rv, Relation pipeline_query)
{
	HeapTuple tuple;
	HeapTuple newtuple;
	Form_pipeline_query row;
	bool nulls[Natts_pipeline_query];
	bool replaces[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	if (row->state != PIPELINE_QUERY_STATE_INACTIVE)
	{
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));
		MemSet(replaces, false, sizeof(replaces));

		replaces[Anum_pipeline_query_state - 1] = true;
		values[Anum_pipeline_query_state - 1] = CharGetDatum(PIPELINE_QUERY_STATE_INACTIVE);

		newtuple = heap_modify_tuple(tuple, pipeline_query->rd_att,
				values, nulls, replaces);
		simple_heap_update(pipeline_query, &newtuple->t_self, newtuple);

		CatalogUpdateIndexes(pipeline_query, newtuple);
		CommandCounterIncrement();
	}
	ReleaseSysCache(tuple);

	return row->state != PIPELINE_QUERY_STATE_INACTIVE;
}

/*
 * GetContinuousViewParams
 *
 * Fetch the parameters for the CV from the `pipeline_query` catalog table.
 */
void
GetContinousViewState(RangeVar *rv, ContinuousViewState *cv_state)
{
	HeapTuple tuple;
	Form_pipeline_query row;

	if (!cv_state)
		return;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	ReleaseSysCache(tuple);

	cv_state->id = row->id;
	cv_state->state = row->state;
	cv_state->long_xact = row->long_xact;
	cv_state->batchsize = row->batchsize;
	cv_state->maxwaitms = row->maxwaitms;
	cv_state->emptysleepms = row->emptysleepms;
	cv_state->parallelism = row->parallelism;
	cv_state->viewid = RangeVarGetRelid(rv, NoLock, false);

	namestrcpy(&cv_state->matrelname, NameStr(row->matrelname));
}

/*
 * SetContinuousViewParams
 *
 * Set the tuning parameters for the CV in the `pipeline_query`
 * catalog table by reading them from cv_state.
 */
void
SetContinousViewState(RangeVar *rv, ContinuousViewState *cv_state, Relation pipeline_query)
{
	HeapTuple tuple;
	HeapTuple newtuple;
	bool nulls[Natts_pipeline_query];
	bool replaces[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(rv->relname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, false, sizeof(nulls));
	MemSet(replaces, false, sizeof(replaces));

	replaces[Anum_pipeline_query_batchsize - 1] = true;
	values[Anum_pipeline_query_batchsize - 1] = Int64GetDatum(cv_state->batchsize);

	replaces[Anum_pipeline_query_maxwaitms - 1] = true;
	values[Anum_pipeline_query_maxwaitms - 1] = Int32GetDatum(cv_state->maxwaitms);

	replaces[Anum_pipeline_query_emptysleepms - 1] = true;
	values[Anum_pipeline_query_emptysleepms - 1] = Int32GetDatum(cv_state->emptysleepms);

	replaces[Anum_pipeline_query_parallelism - 1] = true;
	values[Anum_pipeline_query_parallelism - 1] = Int16GetDatum(cv_state->parallelism);
	newtuple = heap_modify_tuple(tuple, pipeline_query->rd_att,
			values, nulls, replaces);
	simple_heap_update(pipeline_query, &newtuple->t_self, newtuple);
	CatalogUpdateIndexes(pipeline_query, newtuple);

	CommandCounterIncrement();
	ReleaseSysCache(tuple);
}

/*
 * IsContinuousViewActive
 */
bool
IsContinuousViewActive(RangeVar *name)
{
	HeapTuple tuple;
	Form_pipeline_query row;
	bool isActive;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(name->relname));
	/* If the HeapTuple is invalid, consider the CV inactive. */
	isActive = HeapTupleIsValid(tuple);
	if (isActive)
	{
		row = (Form_pipeline_query) GETSTRUCT(tuple);
		isActive = (row->state == PIPELINE_QUERY_STATE_ACTIVE);
		ReleaseSysCache(tuple);
	}

	return isActive;
}

/*
 * GetMatRelationName
 */
char *GetMatRelationName(char *cvname)
{
	HeapTuple tuple;
	Form_pipeline_query row;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(cvname));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname)));
	row = (Form_pipeline_query) GETSTRUCT(tuple);
	ReleaseSysCache(tuple);
	return pstrdup(NameStr(row->matrelname));
}

/*
 * GetCVNameForMatRelationName
 */
char *
GetCVNameForMatRelationName(char *matrelname)
{
	char *cvname = NULL;
	Relation pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		if (strcmp(matrelname, NameStr(row->matrelname)) == 0)
		{
			cvname = pstrdup(NameStr(row->name));
			break;
		}
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);
	return cvname;
}

/*
 * GetQueryStringOrNull
 */
char *
GetQueryStringOrNull(char *cvname, bool select_only)
{
	HeapTuple tuple;
	Datum tmp;
	bool isnull;
	char *result;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(cvname));

	if (!HeapTupleIsValid(tuple))
		return NULL;

	tmp = SysCacheGetAttr(PIPELINEQUERYNAME, tuple, Anum_pipeline_query_query, &isnull);
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
		int pos = skip_substring(result, "CREATE", 0);
		pos = skip_substring(result, "CONTINUOUS", pos);
		pos = skip_substring(result, "VIEW", pos);
		pos = skip_substring(result, cvname, pos);
		pos = skip_substring(result, "AS", pos);

		trimmedlen = strlen(result) - pos + 1;
		trimmed = palloc(trimmedlen);

		memcpy(trimmed, &result[pos], trimmedlen);
		pfree(result);

		result = trimmed;
	}

	return result;
}

/*
 * GetQueryString
 *
 * Retrieves the query string of a registered continuous
 * view. If `select_only` is true, only the SELECT portion
 * of the query string is returned.
 */
char *
GetQueryString(char *cvname, bool select_only)
{
	char *result = GetQueryStringOrNull(cvname, select_only);
	if (result == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname)));
	return result;
}

/*
 * IsContinuousView
 *
 * Returns true if the RangeVar represents a registered
 * continuous view.
 */
bool
IsAContinuousView(RangeVar *name)
{
	HeapTuple tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(name->relname));
	if (!HeapTupleIsValid(tuple))
		return false;
	ReleaseSysCache(tuple);
	return true;
}

/*
 * IsAMatRel
 *
 * Returns true if the RangeVar represents a materialization table,
 * and also assigns the given string (if it's non-NULL) to the name
 * of the corresponding continuous view
 */
bool
IsAMatRel(RangeVar *name, RangeVar **cvname)
{
	Relation pipeline_query;
	HeapScanDesc scandesc;
	HeapTuple tup;
	bool ismatrel = false;
	NameData cv;

	pipeline_query = heap_open(PipelineQueryRelationId, NoLock);
	scandesc = heap_beginscan_catalog(pipeline_query, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		if (strcmp(NameStr(row->matrelname), name->relname) == 0)
		{
			cv = row->name;
			ismatrel = true;
			break;
		}
	}

	heap_endscan(scandesc);
	heap_close(pipeline_query, NoLock);

	if (cvname)
		*cvname = makeRangeVar(NULL, pstrdup(NameStr(cv)), -1);

	return ismatrel;
}

/*
 * GetGCFlag
 */
bool
GetGCFlag(RangeVar *name)
{
	bool gc = false;
	HeapTuple tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(name->relname));

	if (HeapTupleIsValid(tuple))
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		gc = row->gc;
		ReleaseSysCache(tuple);
	}

	return gc;
}

/*
 * GetContinuousQuery
 *
 * Returns an analyzed continuous query
 */
Query *
GetContinuousQuery(RangeVar *rv)
{
	char *sql;
	List *parsetree_list;
	SelectStmt *sel;

	sql = GetQueryString(rv->relname, true);
	parsetree_list = pg_parse_query(sql);
	sel = linitial(parsetree_list);

	RewriteStreamingAggs(sel);

	sel->forContinuousView = true;

	return parse_analyze((Node *) sel, sql, 0, 0);
}
