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
#include "utils/bytea.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/tqual.h"

#define MURMUR_SEED 0xadc83b19ULL

#define is_sw(row) ((row)->step_factor > 0)

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
					errmsg("maximum number of continuous queries exceeded"),
					errhint("Please drop an existing continuous query before trying to create a new one.")));

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
	Oid namespace = InvalidOid;

	if (name->schemaname == NULL)
		namespace = RangeVarGetCreationNamespace(name);
	else
		namespace = get_namespace_oid(name->schemaname, true);

	if (!OidIsValid(namespace))
		return NULL;

	Assert(OidIsValid(namespace));

	tuple = SearchSysCache1(PIPELINEQUERYRELID, ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	return tuple;
}

/*
 * DefineContinuousView
 *
 * Adds a CV to the `pipeline_query` catalog table.
 */
Oid
DefineContinuousView(Oid relid, Query *query, Oid matrelid, Oid seqrelid, int ttl,
		AttrNumber ttl_attno, Oid *pq_id)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	Oid id;
	Oid result;
	char *query_str;

	if (!query)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	query_str = nodeToString(query);

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	id = get_next_id(pipeline_query);

	Assert(OidIsValid(id));

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_query_type - 1] = Int8GetDatum(PIPELINE_QUERY_VIEW);

	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pipeline_query_active - 1] = BoolGetDatum(continuous_queries_enabled);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_str);
	values[Anum_pipeline_query_matrelid - 1] = ObjectIdGetDatum(matrelid);
	values[Anum_pipeline_query_seqrelid - 1] = ObjectIdGetDatum(seqrelid);
	values[Anum_pipeline_query_ttl - 1] = Int32GetDatum(ttl);
	values[Anum_pipeline_query_ttl_attno - 1] = Int16GetDatum(ttl_attno);
	values[Anum_pipeline_query_step_factor - 1] = Int16GetDatum(query->swStepFactor);

	/* unused */
	values[Anum_pipeline_query_tgfn - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(0);
	values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(""));

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

void
UpdateContViewIndexIds(Oid cvid, Oid pkindid, Oid lookupindid)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);
	HeapTuple tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(cvid));
	bool replace[Natts_pipeline_query];
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	HeapTuple new;

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view with id \"%d\" does not exist", cvid)));

	MemSet(replace, 0 , sizeof(replace));
	MemSet(nulls, 0 , sizeof(nulls));
	replace[Anum_pipeline_query_pkidxid - 1] = true;
	replace[Anum_pipeline_query_lookupidxid - 1] = true;
	values[Anum_pipeline_query_pkidxid - 1] = ObjectIdGetDatum(pkindid);
	values[Anum_pipeline_query_lookupidxid - 1] = ObjectIdGetDatum(lookupindid);

	new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);

	simple_heap_update(pipeline_query, &tup->t_self, new);
	CatalogUpdateIndexes(pipeline_query, new);
	CommandCounterIncrement();

	ReleaseSysCache(tup);
	heap_close(pipeline_query, RowExclusiveLock);
}

void
UpdateContViewRelIds(Oid cvid, Oid cvrelid, Oid osrelid)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);
	HeapTuple tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(cvid));
	bool replace[Natts_pipeline_query];
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	HeapTuple new;

	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_CONTINUOUS_VIEW),
				errmsg("continuous view with id \"%d\" does not exist", cvid)));

	MemSet(replace, 0 , sizeof(replace));
	MemSet(nulls, 0 , sizeof(nulls));
	replace[Anum_pipeline_query_relid - 1] = true;
	replace[Anum_pipeline_query_osrelid - 1] = true;
	replace[Anum_pipeline_query_pkidxid - 1] = true;
	replace[Anum_pipeline_query_lookupidxid - 1] = true;
	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(cvrelid);
	values[Anum_pipeline_query_osrelid - 1] = ObjectIdGetDatum(osrelid);

	new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);

	simple_heap_update(pipeline_query, &tup->t_self, new);
	CatalogUpdateIndexes(pipeline_query, new);
	CommandCounterIncrement();

	ReleaseSysCache(tup);
	heap_close(pipeline_query, RowExclusiveLock);
}

/*
 * GetMatRelationName
 */
RangeVar *
GetMatRelName(RangeVar *cvname)
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
	result = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);

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

	tup = SearchSysCache1(PIPELINEQUERYMATRELID, ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	cv = makeRangeVar(get_namespace_name(get_rel_namespace(row->relid)), get_rel_name(row->relid), -1);

	ReleaseSysCache(tup);

	return cv;
}

/*
 * OpenCVRelFromMatRel
 */
Relation
OpenCVRelFromMatRel(Relation matrel, LOCKMODE lockmode)
{
	HeapTuple tup;
	Oid namespace = RelationGetNamespace(matrel);
	Form_pipeline_query row;
	RangeVar *cv;

	Assert(OidIsValid(namespace));

	tup = SearchSysCache1(PIPELINEQUERYMATRELID, RelationGetRelid(matrel));

	if (!HeapTupleIsValid(tup))
		return NULL;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	cv = makeRangeVar(get_namespace_name(namespace), get_rel_name(row->relid), -1);

	ReleaseSysCache(tup);

	return heap_openrv(cv, lockmode);
}

/*
 * IsAContinuousView
 *
 * Returns true if the RangeVar represents a registered
 * continuous view.
 */
bool
IsAContinuousView(RangeVar *name)
{
	HeapTuple tup = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	bool success;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);
	success = row->type == PIPELINE_QUERY_VIEW;

	ReleaseSysCache(tup);

	return success;
}

/*
 * ContainsSlidingWindowContinuousView
 *
 * Returns true if any of the given nodes represents a
 * sliding window continuous view
 */
RangeVar *
GetSWContinuousViewRangeVar(List *nodes)
{
	ListCell *lc;
	foreach(lc, nodes)
	{
		if (IsA(lfirst(lc), RangeVar))
		{
			RangeVar *rv = lfirst(lc);

			if (IsAContinuousView(rv) && IsSWContView(rv))
				return rv;
		}
	}
	return NULL;
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

	tup = SearchSysCache1(PIPELINEQUERYMATRELID, ObjectIdGetDatum(get_relname_relid(name->relname, namespace)));

	if (!HeapTupleIsValid(tup))
	{
		if (cvname)
			*cvname = NULL;
		return false;
	}

	row = (Form_pipeline_query) GETSTRUCT(tup);

	if (cvname)
		*cvname = makeRangeVar(get_namespace_name(namespace), get_rel_name(row->relid), -1);

	ReleaseSysCache(tup);

	return true;
}

/*
 * RelIdIsAMatRel
 *
 * Returns true if the given oid represents a materialization table
 */
bool
RelIdIsForMatRel(Oid relid, Oid *id)
{
	HeapTuple tup;

	tup = SearchSysCache1(PIPELINEQUERYMATRELID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tup))
	{
		if (id)
			*id = InvalidOid;
		return false;
	}

	if (id)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		*id = row->id;
	}

	ReleaseSysCache(tup);

	return true;
}


/*
 * IsSWContView
 */
bool
IsSWContView(RangeVar *name)
{
	bool sw = false;
	HeapTuple tuple = GetPipelineQueryTuple(name);

	if (HeapTupleIsValid(tuple))
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		sw = is_sw(row);
		ReleaseSysCache(tuple);
	}

	return sw;
}

/*
 * IsTTLContView
 */
bool
IsTTLContView(RangeVar *name)
{
	bool ttl = false;
	HeapTuple tuple = GetPipelineQueryTuple(name);

	if (HeapTupleIsValid(tuple))
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tuple);
		ttl = AttributeNumberIsValid(row->ttl_attno);
		ReleaseSysCache(tuple);
	}

	return ttl;
}

ContQuery *
GetContQueryForId(Oid id)
{
	HeapTuple tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(id));
	ContQuery *cq;
	Form_pipeline_query row;
	Datum tmp;
	bool isnull;
	Query *query;

	if (!HeapTupleIsValid(tup))
		return NULL;

	cq = palloc0(sizeof(ContQuery));
	row = (Form_pipeline_query) GETSTRUCT(tup);

	cq->id = id;
	cq->oid = HeapTupleGetOid(tup);

	Assert(row->type == PIPELINE_QUERY_TRANSFORM || row->type == PIPELINE_QUERY_VIEW);
	cq->type = row->type == PIPELINE_QUERY_TRANSFORM ? CONT_TRANSFORM : CONT_VIEW;

	cq->relid = row->relid;
	cq->name = makeRangeVar(get_namespace_name(get_rel_namespace(row->relid)), get_rel_name(row->relid), -1);
	cq->seqrelid = row->seqrelid;
	cq->matrelid = row->matrelid;
	cq->active = row->active;
	cq->osrelid = row->osrelid;
	cq->pkidxid = row->pkidxid;
	cq->lookupidxid = row->lookupidxid;

	if (cq->type == CONT_VIEW)
	{
		cq->matrel = makeRangeVar(get_namespace_name(get_rel_namespace(row->matrelid)), get_rel_name(row->matrelid), -1);
		/* Ignore inherited tables when working with the matrel */
		cq->matrel->inhOpt = INH_NO;
	}
	else
		cq->matrel = NULL;

	tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
	query = (Query *) stringToNode(TextDatumGetCString(tmp));
	cq->sql = deparse_query_def(query);

	if (is_sw(row))
	{
		Interval *i;

		cq->is_sw = true;
		cq->sw_attno = row->ttl_attno;
		cq->sw_step_factor = query->swStepFactor;
		i = GetSWInterval(cq->name);
		cq->sw_interval_ms = 1000 * (int) DatumGetFloat8(
				DirectFunctionCall2(interval_part, CStringGetTextDatum("epoch"), (Datum) i));
		cq->sw_step_ms = (int) (cq->sw_interval_ms * cq->sw_step_factor / 100.0);
	}

	cq->tgfn = row->tgfn;
	cq->tgnargs = row->tgnargs;

	/* This code is copied from trigger.c:RelationBuildTriggers */
	if (cq->tgnargs > 0)
	{
		bytea *val;
		char *p;
		int i;

		val = DatumGetByteaP(SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_tgargs, &isnull));
		Assert(!isnull);

		p = (char *) VARDATA(val);
		cq->tgargs = (char **) palloc(cq->tgnargs * sizeof(char *));

		for (i = 0; i < cq->tgnargs; i++)
		{
			cq->tgargs[i] = pstrdup(p);
			p += strlen(p) + 1;
		}
	}
	else
		cq->tgargs = NULL;

	ReleaseSysCache(tup);

	return cq;
}

/*
 * GetContQueryForView
 */
ContQuery *
GetContQueryForViewId(Oid id)
{
	ContQuery *cq = GetContQueryForId(id);

	if (cq)
	{
		if (cq->type != CONT_VIEW)
			elog(ERROR, "continuous query with id %d is not a continuous view", id);

		return cq;
	}

	return NULL;
}

/*
 * GetContQueryForView
 */
ContQuery *
GetContQueryForTransformId(Oid id)
{
	ContQuery *cq = GetContQueryForId(id);

	if (cq)
	{
		if (cq->type != CONT_TRANSFORM)
			elog(ERROR, "continuous query with id %d is not a continuous transform", id);

		return cq;
	}
	return NULL;
}

/*
 * RangeVarGetContinuousView
 */
ContQuery *
GetContQueryForView(RangeVar *cv_name)
{
	Oid id = GetContQueryId(cv_name);

	if (!OidIsValid(id))
		return NULL;

	return GetContQueryForViewId(id);
}

static Bitmapset *
get_cont_query_ids(char type)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, AccessShareLock);
	HeapScanDesc scan_desc = heap_beginscan_catalog(pipeline_query, 0, NULL);
	HeapTuple tup;
	Bitmapset *result = NULL;

	while ((tup = heap_getnext(scan_desc, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Oid id = row->id;

		if (type && row->type != type)
			continue;

		result = bms_add_member(result, id);
	}

	heap_endscan(scan_desc);
	heap_close(pipeline_query, AccessShareLock);

	return result;
}

/*
 * GetAllContinuousViewIds
 */
Bitmapset *
GetContinuousViewIds(void)
{
	return get_cont_query_ids(PIPELINE_QUERY_VIEW);
}

Bitmapset *
GetContinuousTransformIds(void)
{
	return get_cont_query_ids(PIPELINE_QUERY_TRANSFORM);
}

Bitmapset *
GetContinuousQueryIds(void)
{
	return get_cont_query_ids(0);
}

/*
 * RemovePipelineQueryById
 *
 * Remove a row from pipeline_query along with its associated transition state
 */
void
RemovePipelineQueryById(Oid oid)
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

	pgstat_report_create_drop_cv(false);

	heap_close(pipeline_query, NoLock);
}

Oid
GetContQueryId(RangeVar *name)
{
	HeapTuple tuple = GetPipelineQueryTuple(name);
	Form_pipeline_query row;
	Oid row_id = InvalidOid;

	if (HeapTupleIsValid(tuple))
	{
		row = (Form_pipeline_query) GETSTRUCT(tuple);
		row_id = row->id;
		ReleaseSysCache(tuple);
	}

	return row_id;
}

Oid
DefineContinuousTransform(Oid relid, Query *query, Oid typoid, Oid osrelid, Oid fnoid, List *args)
{
	Relation pipeline_query;
	HeapTuple tup;
	bool nulls[Natts_pipeline_query];
	Datum values[Natts_pipeline_query];
	Oid id;
	Oid result;
	char *query_str;

	if (!query)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("query is null")));

	query_str = nodeToString(query);

	pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);

	id = get_next_id(pipeline_query);

	Assert(OidIsValid(id));

	MemSet(nulls, 0, sizeof(nulls));

	values[Anum_pipeline_query_type - 1] = Int8GetDatum(PIPELINE_QUERY_TRANSFORM);

	values[Anum_pipeline_query_id - 1] = Int32GetDatum(id);
	values[Anum_pipeline_query_relid - 1] = ObjectIdGetDatum(relid);
	values[Anum_pipeline_query_active - 1] = BoolGetDatum(continuous_queries_enabled);
	values[Anum_pipeline_query_query - 1] = CStringGetTextDatum(query_str);
	values[Anum_pipeline_query_tgfn - 1] = ObjectIdGetDatum(fnoid);
	values[Anum_pipeline_query_matrelid - 1] = ObjectIdGetDatum(typoid); /* HACK(usmanm): So matrel index works */
	values[Anum_pipeline_query_osrelid - 1] = ObjectIdGetDatum(osrelid);
	values[Anum_pipeline_query_pkidxid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pipeline_query_lookupidxid - 1] = ObjectIdGetDatum(InvalidOid);

	/* This code is copied from trigger.c:CreateTrigger */
	if (list_length(args))
	{
		ListCell *lc;
		char *argsbytes;
		int16 nargs = list_length(args);
		int	len = 0;

		foreach(lc, args)
		{
			char *ar = strVal(lfirst(lc));

			len += strlen(ar) + 4;
			for (; *ar; ar++)
			{
				if (*ar == '\\')
					len++;
			}
		}

		argsbytes = (char *) palloc(len + 1);
		argsbytes[0] = '\0';

		foreach(lc, args)
		{
			char *s = strVal(lfirst(lc));
			char *d = argsbytes + strlen(argsbytes);

			while (*s)
			{
				if (*s == '\\')
					*d++ = '\\';
				*d++ = *s++;
			}
			strcpy(d, "\\000");
		}

		values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(nargs);
		values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(argsbytes));
	}
	else
	{
		values[Anum_pipeline_query_tgnargs - 1] = Int16GetDatum(0);
		values[Anum_pipeline_query_tgargs - 1] = DirectFunctionCall1(byteain, CStringGetDatum(""));
	}

	/* unused */
	values[Anum_pipeline_query_seqrelid - 1] = ObjectIdGetDatum(InvalidOid);
	values[Anum_pipeline_query_ttl - 1] = Int32GetDatum(-1);
	values[Anum_pipeline_query_ttl_attno - 1] = Int16GetDatum(InvalidAttrNumber);
	values[Anum_pipeline_query_step_factor - 1] = Int16GetDatum(0);

	tup = heap_form_tuple(pipeline_query->rd_att, values, nulls);

	result = simple_heap_insert(pipeline_query, tup);
	CatalogUpdateIndexes(pipeline_query, tup);
	CommandCounterIncrement();

	heap_freetuple(tup);

	UpdatePipelineStreamCatalog();

	heap_close(pipeline_query, NoLock);

	return result;
}

bool
ContQuerySetActive(Oid id, bool active)
{
	Relation pipeline_query = heap_open(PipelineQueryRelationId, RowExclusiveLock);
	HeapTuple tup = SearchSysCache1(PIPELINEQUERYID, ObjectIdGetDatum(id));
	Form_pipeline_query row;
	bool changed = false;

	if (!HeapTupleIsValid(tup))
		return false;

	row = (Form_pipeline_query) GETSTRUCT(tup);

	if (row->active != active)
	{
		bool replace[Natts_pipeline_query];
		bool nulls[Natts_pipeline_query];
		Datum values[Natts_pipeline_query];
		HeapTuple new;

		MemSet(replace, 0 , sizeof(replace));
		MemSet(nulls, 0 , sizeof(nulls));
		replace[Anum_pipeline_query_active - 1] = true;
		values[Anum_pipeline_query_active - 1] = BoolGetDatum(active);

		new = heap_modify_tuple(tup, RelationGetDescr(pipeline_query), values, nulls, replace);

		simple_heap_update(pipeline_query, &tup->t_self, new);
		CatalogUpdateIndexes(pipeline_query, new);
		CommandCounterIncrement();

		changed = true;
	}

	ReleaseSysCache(tup);
	heap_close(pipeline_query, NoLock);

	return changed;
}

void
GetTTLInfo(RangeVar *cvname, char **ttl_col, int *ttl)
{
	HeapTuple tup = GetPipelineQueryTuple(cvname);
	Form_pipeline_query row;
	Relation rel;
	TupleDesc desc;
	int i;

	if (!HeapTupleIsValid(tup))
		elog(ERROR, "continuous view \"%s\" does not exist", cvname->relname);

	Assert(ttl_col);
	Assert(ttl);

	row = (Form_pipeline_query) GETSTRUCT(tup);
	Assert(AttributeNumberIsValid(row->ttl_attno));
	Assert(row->ttl > 0);
	*ttl = row->ttl;

	rel = heap_open(row->matrelid, NoLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < desc->natts; i++)
	{
		if (desc->attrs[i]->attnum == row->ttl_attno)
		{
			*ttl_col = pstrdup(NameStr(desc->attrs[i]->attname));
			break;
		}
	}

	Assert(*ttl_col);

	heap_close(rel, NoLock);
	ReleaseSysCache(tup);
}
