/*-------------------------------------------------------------------------
 *
 * pipeline_query.c
 *	  routines to support manipulation of the pipeline_query relation
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pipeline_query.c
 *
 *-------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "pgstat.h"

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
#include "pipeline/cont_analyze.h"
#include "pipeline/cont_scheduler.h"
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
 * compare_oid
 */
static int
compare_oid(const void *a, const void *b)
{
  const Oid *ia = (const Oid *) a;
  const Oid *ib = (const Oid *) b;

  return (*ia > *ib) - (*ia < *ib);
}

/*
 * get_next_id
 *
 * Gets the smallest possible id to assign to the next continuous view.
 * We keep this minimal so that we can minimize the size of bitmaps used
 * to tag stream buffer events with.
 */
static Oid
get_next_id(Relation rel)
{
	HeapScanDesc scandesc;
	HeapTuple tup;
	List *ids_list = NIL;
	int num_ids;

	Assert(MAX_CQS % 32 == 0);

	scandesc = heap_beginscan_catalog(rel, 0, NULL);

	while ((tup = heap_getnext(scandesc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		ids_list = lappend_oid(ids_list, row->id);
	}

	heap_endscan(scandesc);

	num_ids = list_length(ids_list);

	if (num_ids)
	{
		Oid ids[num_ids];
		int counts_per_combiner[continuous_query_num_combiners];
		int i = 0;
		Oid max;
		ListCell *lc;
		int j;
		int target_combiner;
		List *potential_ids;

		MemSet(counts_per_combiner, 0, sizeof(counts_per_combiner));

		foreach(lc, ids_list)
		{
			ids[i] = lfirst_oid(lc);
			counts_per_combiner[ids[i] % continuous_query_num_combiners] += 1;
			i++;
		}

		qsort(ids, num_ids, sizeof(Oid), &compare_oid);

		if (num_ids == MAX_CQS - 1) /* -1 because 0 is an invalid id */
			ereport(ERROR,
					(errcode(ERRCODE_TOO_MANY_CONTINUOUS_VIEWS),
					errmsg("maximum number of continuous views exceeded"),
					errhint("Please drop a existing continuous view before trying to create a new one.")));

		max = ids[num_ids - 1];
		Assert(max >= num_ids);

		/*
		 * FIXME(usmanm): We do some randomization of ID generation here to make sure that CQs that
		 * are created and dropped in quick succession don't read an event that was not for them.
		 */

		/*
		 * Collect any unused ids in [1, max].
		 */
		list_free(ids_list);
		ids_list = NIL;

		for (i = 1, j = 0; j < num_ids; i++)
		{
			if (ids[j] > i)
				ids_list = lappend_oid(ids_list, (Oid) i);
			else
				j++;
		}

		/*
		 * Add all IDs between max and the next multiple of 32.
		 */
		j = Min((max / 32 + 1) * 32, MAX_CQS);
		for (i = max + 1; i < j; i++)
			ids_list = lappend_oid(ids_list, (Oid) i);

		/*
		 * Less than 16 options? Throw in some more.
		 */
		if (list_length(ids_list) < 16 && j < MAX_CQS)
			for (i = j; i < j + 32; i++)
				ids_list = lappend_oid(ids_list, (Oid) i);

		/*
		 * Figure out the target combiner (one with least IDs allocated) and try to allocate
		 * an ID that belongs to it.
		 */
		target_combiner = 0;
		for (i = 0; i < continuous_query_num_combiners; i++)
			if (counts_per_combiner[i] < counts_per_combiner[target_combiner])
				target_combiner = i;

		potential_ids = NIL;
		foreach(lc, ids_list)
		{
			Oid id = lfirst_oid(lc);
			if (id % continuous_query_num_combiners == target_combiner)
				potential_ids = lappend_oid(potential_ids, id);
		}

		if (list_length(potential_ids))
			return list_nth_oid(potential_ids, rand() % list_length(potential_ids));

		return list_nth_oid(ids_list, rand() % list_length(ids_list));
	}

	/*
	 * No CVs exist, give any id in [1, 16).
	 */
	return (rand() % 15) + 1;
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
DefineContinuousView(RangeVar *name, Query *query, Oid matrel, bool gc, bool adhoc, Oid *pq_id)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	NameData name_data;
	Oid id;
	Oid namespace;
	Oid result;
	char *query_str;

	if (!name)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("name is null")));

	if (!query)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	query_str = nodeToString(query);

	/*
	 * This should have already been done by the caller when creating the matrel,
	 * but just to be safe...
	 */
	namespace = RangeVarGetCreationNamespace(name);

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	id = get_next_id(pipeline_query);

	Assert(OidIsValid(id));

	namestrcpy(&name_data, name->relname);
	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_name - 1] = NameGetDatum(&name_data);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_str);
	values[Anum_pipeline_query_namespace - 1] = ObjectIdGetDatum(namespace);
	values[Anum_pipeline_query_matrel - 1] = ObjectIdGetDatum(matrel);

	/* Copy flags */
	values[Anum_pipeline_query_gc - 1] = BoolGetDatum(gc);
	values[Anum_pipeline_query_adhoc - 1] = BoolGetDatum(adhoc);

	MemSet(nulls, 0, sizeof(nulls));
	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	result = simple_heap_insert(pipeline_query, tup);
	CatalogUpdateIndexes(pipeline_query, tup);
	CommandCounterIncrement();

	/* Create transition state entry */
	CreateTStateEntry(id);

	heap_freetuple(tup);

	UpdatePipelineStreamCatalog();

	heap_close(pipeline_query, NoLock);

	*pq_id = id;

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
	RangeVar *result;

	tuple = GetPipelineQueryTuple(cvname);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", cvname->relname)));

	row = (Form_pipeline_query) GETSTRUCT(tuple);
	result = makeRangeVar(get_namespace_name(row->namespace), get_rel_name(row->matrel), -1);

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

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL, ObjectIdGetDatum(namespace),
			ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	cv = makeRangeVar(get_namespace_name(namespace), pstrdup(NameStr(row->name)), -1);

	ReleaseSysCache(tup);

	return cv;
}

/*
 * GetContSelectStmt
 */
SelectStmt *
GetContSelectStmt(RangeVar *rv)
{
	HeapTuple tuple;
	bool isnull;
	Datum tmp;
	char *sql;
	Query *query;
	SelectStmt *select;

	tuple = GetPipelineQueryTuple(rv);

	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view \"%s\" does not exist", rv->relname)));

	tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tuple, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));

	sql = deparse_query_def(query);
	select = (SelectStmt *) linitial(pg_parse_query(sql));
	select->swStepFactor = query->swStepFactor;

	ReleaseSysCache(tuple);

	return select;
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
 * ContainsSlidingWindowContinuousView
 *
 * Returns true if any of the given nodes represents a
 * sliding window continuous view
 */
bool
ContainsSlidingWindowContinuousView(List *nodes)
{
	ListCell *lc;
	foreach(lc, nodes)
	{
		if (IsA(lfirst(lc), RangeVar))
		{
			if (IsAContinuousView((RangeVar *) lfirst(lc)) && GetGCFlag((RangeVar *) lfirst(lc)))
				return true;
		}
	}
	return false;
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

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL, ObjectIdGetDatum(namespace),
			ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

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
 * RelIdIsAMatRel
 *
 * Returns true if the given oid represents a materialization table
 */
bool
RelIdIsAMatRel(Oid relid)
{
	Relation rel = heap_open(relid, NoLock);
	Oid namespace = RelationGetNamespace(rel);
	HeapTuple tup;

	heap_close(rel, NoLock);

	tup = SearchSysCache2(PIPELINEQUERYNAMESPACEMATREL,
			ObjectIdGetDatum(namespace), ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
		return false;

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
 * GetContinuousView
 */
ContinuousView *
GetContinuousView(Oid id)
{
	HeapTuple tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(id));
	ContinuousView *view;
	Form_pipeline_query row;
	Datum tmp;
	bool isnull;
	Query *query;

	if (!HeapTupleIsValid(tup))
		return NULL;

	view = palloc0(sizeof(ContinuousView));
	row = (Form_pipeline_query) GETSTRUCT(tup);

	view->id = id;
	view->oid = HeapTupleGetOid(tup);

	view->namespace = row->namespace;
	view->matrel = makeRangeVar(get_namespace_name(row->namespace), get_rel_name(row->matrel), -1);

	namestrcpy(&view->name, NameStr(row->name));

	tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));
	view->query = deparse_query_def(query);
	view->sw_step_factor = query->swStepFactor;

	ReleaseSysCache(tup);

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

		if (row->adhoc)
			continue;

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

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);

	tuple = SearchSysCache1(PIPELINEQUERYOID, ObjectIdGetDatum(oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for continuous view with OID %u", oid);

	row = (Form_pipeline_query) GETSTRUCT(tuple);

	/* Remove transition state entry */
	RemoveTStateEntry(row->id);

	simple_heap_delete(pipeline_query, &tuple->t_self);

	ReleaseSysCache(tuple);

	CommandCounterIncrement();
	UpdatePipelineStreamCatalog();

	cq_stat_report_create_drop_cv(false);

	heap_close(pipeline_query, NoLock);
}

/*
 * GetAdhocContinuousViewIds
 *
 * Returns a bitmapset of all the continuous views that
 * are marked as adhoc.
 */
Bitmapset *
GetAdhocContinuousViewIds(void)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;
	Bitmapset *result = NULL;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Oid id = row->id;

		if (!row->adhoc)
			continue;

		result = bms_add_member(result, id);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);

	return result;
}
