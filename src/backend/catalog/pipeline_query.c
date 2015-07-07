/* Copyright (c) 2013-2015 PipelineDB */
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
#include "catalog/pipeline_stream_fn.h"
#include "catalog/pipeline_tstate_fn.h"
#include "libpq/libpq.h"
#include "miscadmin.h"
#include "catalog/namespace.h"
#include "nodes/makefuncs.h"
#include "parser/analyze.h"
#include "pipeline/cqanalyze.h"
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

#define MURMUR_SEED 0xadc83b19ULL

/*
 * compare_int32s
 */
static int
compare_int32(const void *a, const void *b)
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
	int32			id = 1;
	List			*idsList = NIL;
	ListCell		*lc;

	scandesc = heap_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		idsList = lappend_int(idsList, row->id);
	}

	heap_endscan(scandesc);

	if (idsList != NIL)
	{
		int32 ids[idsList->length];
		int i = 0;
		foreach(lc, idsList)
		{
			ids[i] = lfirst_int(lc);
			i++;
		}

		qsort(ids, idsList->length, sizeof(Oid), &compare_int32);

		for (id = 0; id < idsList->length; id++)
		{
			if (ids[id] > id + 1)
				break;
		}

		id++;
	}

	return id;
}

/*
 * CreateContinuousView
 *
 * Adds a CV to the `pipeline_query` catalog table.
 */
void
CreateContinuousView(RangeVar *name, const char *query_string, RangeVar* matrelname, bool gc, bool needs_xact)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	NameData name_data;
	NameData matrelname_data;
	Oid id;
	uint64_t hash;

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

	id = get_next_id(pipeline_query);

	namestrcpy(&name_data, name->relname);
	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_name - 1] = NameGetDatum(&name_data);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_string);

	/* Copy matrelname */
	namestrcpy(&matrelname_data, matrelname->relname);
	values[Anum_pipeline_query_matrelname - 1] = NameGetDatum(&matrelname_data);

	/* Copy flags */
	values[Anum_pipeline_query_gc - 1] = BoolGetDatum(gc);
	values[Anum_pipeline_query_needs_xact - 1] = BoolGetDatum(needs_xact);

	hash = MurmurHash3_64(name->relname, strlen(name->relname), MURMUR_SEED) ^ MurmurHash3_64(query_string, strlen(query_string), MURMUR_SEED);
	values[Anum_pipeline_query_hash - 1] = Int32GetDatum(hash);

	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	simple_heap_insert(pipeline_query, tup);
	CatalogUpdateIndexes(pipeline_query, tup);
	CommandCounterIncrement();

	/* Create transition state entry */
	CreateTStateEntry(id);

	heap_freetuple(tup);

	UpdateStreamQueries(pipeline_query);

	heap_close(pipeline_query, NoLock);
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
 * GetQueryString
 *
 * Retrieves the query string of a registered continuous
 * view. If `select_only` is true, only the SELECT portion
 * of the query string is returned.
 */
char *
GetQueryString(char *cvname)
{
	HeapTuple tuple;
	bool isnull;
	Datum tmp;
	char *result = NULL;

	tuple = SearchSysCache1(PIPELINEQUERYNAME, CStringGetDatum(cvname));

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname)));

	tmp = SysCacheGetAttr(PIPELINEQUERYNAME, tuple, Anum_pipeline_query_query, &isnull);
	result = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);

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
		if (pg_strcasecmp(NameStr(row->matrelname), name->relname) == 0)
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

	sql = GetQueryString(rv->relname);
	parsetree_list = pg_parse_query(sql);
	sel = linitial(parsetree_list);

	RewriteStreamingAggs(sel);

	sel->forContinuousView = true;

	return parse_analyze((Node *) sel, sql, 0, 0);
}

/*
 * GetContinuousView
 */
ContinuousView *
GetContinuousView(Oid id)
{
	HeapTuple tuple = SearchSysCache1(PIPELINEQUERYID, Int32GetDatum(id));
	ContinuousView *view;
	Form_pipeline_query row;
	Datum tmp;
	bool isnull;

	if (!HeapTupleIsValid(tuple))
		return NULL;

	view = palloc(sizeof(ContinuousView));
	row = (Form_pipeline_query) GETSTRUCT(tuple);

	view->id = id;
	namestrcpy(&view->name, NameStr(row->name));
	namestrcpy(&view->matrelname, NameStr(row->matrelname));
	view->needs_xact = row->needs_xact;
	view->hash = row->hash;

	tmp = SysCacheGetAttr(PIPELINEQUERYNAME, tuple, Anum_pipeline_query_query, &isnull);
	view->query = TextDatumGetCString(tmp);

	ReleaseSysCache(tuple);

	return view;
}

/*
 * GetAllContinuousViewIds
 */
Bitmapset *
GetAllContinuousViewIds(void)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;
	Bitmapset *result = NULL;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Oid id = row->id;
		result = bms_add_member(result, id);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);

	return result;
}
