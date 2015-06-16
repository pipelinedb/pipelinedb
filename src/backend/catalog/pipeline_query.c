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
#include "utils/lsyscache.h"
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
 * Lookup a raw tuple from pipeline_query from the given continuous view name
 */
HeapTuple
GetPipelineQueryTuple(RangeVar *name)
{
	HeapTuple tuple;
	Oid namespace;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, false);

	Assert(OidIsValid(namespace));

	tuple = SearchSysCache2(PIPELINEQUERYNAMESPACENAME, ObjectIdGetDatum(namespace), CStringGetDatum(name->relname));

	return tuple;
}

/*
 * DefineContinuousView
 *
 * Adds a CV to the `pipeline_query` catalog table.
 */
Oid
DefineContinuousView(RangeVar *name, const char *query_string, RangeVar* matrelname, bool gc, bool needs_xact)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	NameData name_data;
	NameData matrelname_data;
	Oid id;
	uint64_t hash;
	Oid namespace;
	Oid result;

	if (!name)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!query_string)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	/*
	 * This should have already been done by the caller when creating the matrel,
	 * but just to be safe...
	 */
	namespace = RangeVarGetAndCheckCreationNamespace(name, NoLock, NULL);

	pipeline_query = heap_open(PipelineQueryRelationId, AccessExclusiveLock);

	id = get_next_id(pipeline_query);

	namestrcpy(&name_data, name->relname);
	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_name - 1] = NameGetDatum(&name_data);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_string);
	values[Anum_pipeline_query_namespace - 1] = ObjectIdGetDatum(namespace);

	/* Copy matrelname */
	namestrcpy(&matrelname_data, matrelname->relname);
	values[Anum_pipeline_query_matrelname - 1] = NameGetDatum(&matrelname_data);

	/* Copy flags */
	values[Anum_pipeline_query_gc - 1] = BoolGetDatum(gc);
	values[Anum_pipeline_query_needs_xact - 1] = BoolGetDatum(needs_xact);

	hash = MurmurHash3_64(name->relname, strlen(name->relname), MURMUR_SEED) ^ MurmurHash3_64(query_string, strlen(query_string), MURMUR_SEED);
	values[Anum_pipeline_query_hash - 1] = Int32GetDatum(hash);

	MemSet(nulls, 0, sizeof(nulls));
	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	result = simple_heap_insert(pipeline_query, tup);
	CatalogUpdateIndexes(pipeline_query, tup);
	CommandCounterIncrement();

	/* Create transition state entry */
	CreateTStateEntry(id);

	heap_freetuple(tup);

	UpdatePipelineStreamCatalog(pipeline_query);

	heap_close(pipeline_query, NoLock);

	return result;
}

/*
 * GetMatRelationName
 */
RangeVar *
GetMatRelationName(RangeVar *cvname)
{
	HeapTuple tuple;
	Form_pipeline_query row;
	char *namespace;
	RangeVar *result;

	tuple = GetPipelineQueryTuple(cvname);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);
	namespace = get_namespace_name(row->namespace);
	result = makeRangeVar(namespace, pstrdup(NameStr(row->matrelname)), -1);

	ReleaseSysCache(tuple);

	return result;
}

/*
 * GetCVNameFromMatRelName
 */
RangeVar *
GetCVNameFromMatRelName(RangeVar *name)
{
	HeapTuple tup;
	Oid namespace;
	Form_pipeline_query row;
	RangeVar *cv;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, false);

	Assert(OidIsValid(namespace));

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL, ObjectIdGetDatum(namespace), CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	cv = makeRangeVar(get_namespace_name(namespace), pstrdup(NameStr(row->name)), -1);

	ReleaseSysCache(tup);

	return cv;
}

/*
 * GetQueryString
 *
 * Retrieves the query string of a registered continuous
 * view. If `select_only` is true, only the SELECT portion
 * of the query string is returned.
 */
char *
GetQueryString(RangeVar *cvname)
{
	HeapTuple tuple;
	bool isnull;
	Datum tmp;
	char *result = NULL;

	tuple = GetPipelineQueryTuple(cvname);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname->relname)));

	tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tuple, Anum_pipeline_query_query, &isnull);
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
	HeapTuple tuple = GetPipelineQueryTuple(name);
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
	HeapTuple tup;
	Oid namespace;
	Form_pipeline_query row;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, false);

	Assert(OidIsValid(namespace));

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL, ObjectIdGetDatum(namespace), CStringGetDatum(name->relname));

	if (!HeapTupleIsValid(tup))
	{
		if (cvname)
			*cvname = NULL;
		return false;
	}

	row = (Form_pipeline_query) GETSTRUCT(tup);

	if (cvname)
		*cvname = makeRangeVar(get_namespace_name(namespace), pstrdup(NameStr(row->name)), -1);

	ReleaseSysCache(tup);

	return true;
}

/*
 * GetGCFlag
 */
bool
GetGCFlag(RangeVar *name)
{
	bool gc = false;
	HeapTuple tuple = GetPipelineQueryTuple(name);

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

	sql = GetQueryString(rv);
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
	char *namespace;

	if (!HeapTupleIsValid(tuple))
		return NULL;

	view = palloc0(sizeof(ContinuousView));
	row = (Form_pipeline_query) GETSTRUCT(tuple);

	view->id = id;

	namespace = get_namespace_name(row->namespace);
	view->matrel = makeRangeVar(namespace, pstrdup(NameStr(row->matrelname)), -1);

	namestrcpy(&view->name, NameStr(row->name));
	view->needs_xact = row->needs_xact;
	view->hash = row->hash;

	tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tuple, Anum_pipeline_query_query, &isnull);
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

/*
 * RemoveContinuousViewById
 *
 * Remove a row from pipeline_query along with its associated transition state
 */
void
RemoveContinuousViewById(Oid oid)
{
	Relation pipeline_query;
	HeapTuple tuple;
	Form_pipeline_query row;

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(PIPELINEQUERYOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for continuous view with OID %u", oid);

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	/* Remove transition state entry */
	RemoveTStateEntry(row->id);

	simple_heap_delete(pipeline_query, &tuple->t_self);

	ReleaseSysCache(tuple);

	CommandCounterIncrement();
	UpdatePipelineStreamCatalog(pipeline_query);

	heap_close(pipeline_query, NoLock);
}
