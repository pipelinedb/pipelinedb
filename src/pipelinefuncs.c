/*-------------------------------------------------------------------------
 *
 * pipelinefuncs.c
 *		Functions for PipelineDB operations
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "funcapi.h"

#include "catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "config.h"
#include "miscutils.h"
#include "pipeline_query.h"
#include "pipeline_stream.h"
#include "ruleutils.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#define PIPELINEDB_VERSION_TEMPLATE "PipelineDB %s at revision %s"

typedef struct
{
	Relation rel;
	HeapScanDesc scan;
} RelationScanData;

static int
cstring_cmp(const void *a, const void *b)
{
	const char **ia = (const char **)a;
	const char **ib = (const char **)b;
	return strcmp(*ia, *ib);
}

/*
 * pipeline_get_streams
 */
PG_FUNCTION_INFO_V1(pipeline_get_streams);
Datum
pipeline_get_streams(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	MemoryContext old;
	RelationScanData *data;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(3, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "queries", TEXTARRAYOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = OpenPipelineStream(AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
		data = (RelationScanData *) funcctx->user_fctx;

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);
		Datum values[3];
		bool nulls[3];
		HeapTuple rtup;
		Datum result;
		Datum tmp;
		bool isnull;
		Relation rel = try_relation_open(row->relid, AccessShareLock);
		char *relname;
		char *namespace;

		if (!rel)
			continue;

		relname = RelationGetRelationName(rel);
		namespace = get_namespace_name(RelationGetNamespace(rel));

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(namespace);
		values[1] = CStringGetTextDatum(relname);

		tmp = PipelineCatalogGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

		if (isnull)
			nulls[2] = true;
		else
		{
			int nqueries;
			char **queries;
			Datum *qdatums;
			bytea *bytes;
			int nbytes;
			int nwords;
			Bitmapset *bms;
			int i;
			int j;

			bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(tmp));
			nbytes = VARSIZE(bytes) - VARHDRSZ;
			nwords = nbytes / sizeof(bitmapword);

			bms = palloc0(BITMAPSET_SIZE(nwords));
			bms->nwords = nwords;

			memcpy(bms->words, VARDATA(bytes), nbytes);

			nqueries = bms_num_members(bms);
			queries = palloc0(sizeof(char *) * nqueries);

			i = 0;
			while ((j = bms_first_member(bms)) >= 0)
			{
				ContQuery *view = GetContQueryForId(j);
				char *cq_name;
				Relation rel;

				if (!view)
					continue;

				rel = try_relation_open(view->relid, AccessShareLock);
				if (!rel)
					continue;

				if (!RelationIsVisible(view->relid))
					cq_name = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
							RelationGetRelationName(rel));
				else
					cq_name = quote_qualified_identifier(NULL, RelationGetRelationName(rel));

				relation_close(rel, AccessShareLock);

				queries[i] = cq_name;
				i++;
			}

			/* In case a view wasn't found above, nqueries is stale */
			Assert(i <= nqueries);
			nqueries = i;

			qsort(queries, nqueries, sizeof(char *), cstring_cmp);

			qdatums = palloc0(sizeof(Datum) * nqueries);

			for (i = 0; i < nqueries; i++)
				qdatums[i] = CStringGetTextDatum(queries[i]);

			values[2] = PointerGetDatum(construct_array(qdatums, nqueries, TEXTOID, -1, false, 'i'));
		}

		relation_close(rel, AccessShareLock);

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	heap_close(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_get_views
 */
PG_FUNCTION_INFO_V1(pipeline_get_views);
Datum
pipeline_get_views(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	MemoryContext old;
	RelationScanData *data;

	if (SRF_IS_FIRSTCALL())
	{

		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "active", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "query", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = OpenPipelineQuery(AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
		data = (RelationScanData *) funcctx->user_fctx;

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Datum values[5];
		bool nulls[5];
		HeapTuple rtup;
		Datum result;
		char *relname;
		Oid nsp;
		Query *query;

		if (row->type != PIPELINE_QUERY_VIEW)
			continue;

		relname = get_rel_name(row->relid);
		nsp = get_rel_namespace(row->relid);

		if (!relname || !OidIsValid(nsp))
			continue;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(row->id);
		values[1] = CStringGetTextDatum(get_namespace_name(nsp));
		values[2] = CStringGetTextDatum(relname);
		values[3] = BoolGetDatum(row->active);

		query = GetContQueryDef(row->defrelid);
		values[4] = CStringGetTextDatum(deparse_query_def(query));

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	ClosePipelineQuery(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_get_transforms
 */
PG_FUNCTION_INFO_V1(pipeline_get_transforms);
Datum
pipeline_get_transforms(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	MemoryContext old;
	RelationScanData *data;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(7, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "active", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "tgfunc", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "tgargs", TEXTARRAYOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "query", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = OpenPipelineQuery(AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
		data = (RelationScanData *) funcctx->user_fctx;

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Datum values[7];
		bool nulls[7];
		HeapTuple rtup;
		Datum result;
		bool isnull;
		char *fn;
		char *relname;
		Oid nsp;
		Oid tgfnid = InvalidOid;
		Query *query;

		if (row->type != PIPELINE_QUERY_TRANSFORM)
			continue;

		relname = get_rel_name(row->relid);
		nsp = get_rel_namespace(row->relid);

		if (!relname || !OidIsValid(nsp))
			continue;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(row->id);
		values[1] = CStringGetTextDatum(get_namespace_name(nsp));
		values[2] = CStringGetTextDatum(relname);
		values[3] = BoolGetDatum(row->active);

		tgfnid = GetTriggerFnOid(row->defrelid);

		if (OidIsValid(tgfnid))
		{
			if (!FunctionIsVisible(tgfnid))
				fn = quote_qualified_identifier(get_namespace_name(get_func_namespace(tgfnid)), get_func_name(tgfnid));
			else
				fn = quote_qualified_identifier(NULL, get_func_name(tgfnid));

			values[4] = CStringGetTextDatum(fn);
		}
		else
		{
			nulls[4] = true;
		}

		if (row->tgnargs > 0)
		{
			Datum *elems = palloc0(sizeof(Datum) * row->tgnargs);
			bytea *val;
			char *p;
			int i;

			val = DatumGetByteaP(PipelineCatalogGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_tgargs, &isnull));
			Assert(!isnull);

			p = (char *) VARDATA(val);

			for (i = 0; i < row->tgnargs; i++)
			{
				elems[i] = CStringGetTextDatum(pstrdup(p));
				p += strlen(p) + 1;
			}

			values[5] = PointerGetDatum(construct_array(elems, row->tgnargs, TEXTOID, -1, false, 'i'));
		}
		else
			nulls[5] = true;

		query = GetContQueryDef(row->defrelid);
		values[6] = CStringGetTextDatum(deparse_query_def(query));

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	ClosePipelineQuery(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_get_stream_readers
 */
PG_FUNCTION_INFO_V1(pipeline_get_stream_readers);
Datum
pipeline_get_stream_readers(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	MemoryContext old;
	RelationScanData *data;
	Datum result;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(2, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "stream", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "continuous_queries", TEXTARRAYOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = OpenPipelineStream(AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
	{
		data = (RelationScanData *) funcctx->user_fctx;
	}

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);
		Bitmapset *readers = GetAllStreamReaders(row->relid);
		ArrayBuildState *state = NULL;
		int cq = -1;
		int	dims[1];
		int	lbs[1];
		HeapTuple rtup;
		Datum values[2];
		bool nulls[2];

		MemSet(nulls, 0, sizeof(nulls));

		while ((cq = bms_next_member(readers, cq)) >= 0)
		{
			ContQuery *cont_query = GetContQueryForId((Oid) cq);

			state = accumArrayResult(state,
					CStringGetTextDatum(cont_query->name->relname), false, TEXTOID, CurrentMemoryContext);
		}

		if (state == NULL)
			continue;

		dims[0] = state->nelems;
		lbs[0] = 1;

		values[0] = CStringGetTextDatum(get_rel_name(row->relid));
		values[1] = makeMdArrayResult(state, 1, dims, lbs, CurrentMemoryContext, false);

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	ClosePipelineStream(data->rel, NoLock);
	SRF_RETURN_DONE(funcctx);
}


/*
 * pipeline_version
 *
 * Returns the PipelineDB version string
 */
PG_FUNCTION_INFO_V1(pipeline_version);
Datum
pipeline_version(PG_FUNCTION_ARGS)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, PIPELINEDB_VERSION_TEMPLATE, pipeline_version_str, pipeline_revision_str);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
