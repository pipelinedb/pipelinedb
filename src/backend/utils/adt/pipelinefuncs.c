/*-------------------------------------------------------------------------
 *
 * pipelinefuncs.c
 *		Functions for PipelineDB functions
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/pipelinefuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "pipeline/cont_analyze.h"
#include "pipeline/dsm_cqueue.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/pipelinefuncs.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * cq_proc_stat_get
 *
 * Look up CQ process-level stats in the stats hashtables
 * and return them in the form of rows so that we can use
 * this function as a data source in views
 */
Datum
cq_proc_stat_get(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HTAB *stats;
	CQStatEntry *entry;
	HASH_SEQ_STATUS *iter;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(11, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "start_time", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "updated_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "executions", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "errors", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		stats = cq_stat_fetch_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (CQStatEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[11];
		bool nulls[11];
		HeapTuple tup;
		Datum result;
		pid_t pid = GetCQStatProcPid(entry->key);

		/* keep scanning if it's a CQ-level stats entry */
		if (!pid)
			continue;

		/*
		 * If a pid doesn't exist, kill() with signal 0 will fail and set
		 * errno to ESRCH
		 */
		if (kill(pid, 0) == -1 && errno == ESRCH)
		{
			/* stale proc, purge it */
			cq_stat_send_purge(0, pid, GetCQStatProcType(entry->key));
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum((GetCQStatProcType(entry->key) == CQ_STAT_WORKER ? "worker" : "combiner"));
		values[1] = Int32GetDatum(pid);
		values[2] = TimestampTzGetDatum(entry->start_ts);
		values[3] = Int64GetDatum(entry->input_rows);
		values[4] = Int64GetDatum(entry->output_rows);
		values[5] = Int64GetDatum(entry->updates);
		values[6] = Int64GetDatum(entry->input_bytes);
		values[7] = Int64GetDatum(entry->output_bytes);
		values[8] = Int64GetDatum(entry->updated_bytes);
		values[9] = Int64GetDatum(entry->executions);
		values[10] = Int64GetDatum(entry->errors);

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * cq_stat_get
 *
 * Look up all-time CQ-level stats in the stats hashtables
 * and return them in the form of rows so that we can use
 * this function as a data source in views.
 */
Datum
cq_stat_get(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HTAB *stats;
	CQStatEntry *entry;
	HASH_SEQ_STATUS *iter;

	if (SRF_IS_FIRSTCALL())
	{

		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(9, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "updates", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "errors", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		stats = cq_stat_fetch_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (CQStatEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[9];
		bool nulls[9];
		HeapTuple tup;
		Datum result;
		Oid viewid = GetCQStatView(entry->key);
		ContinuousView *cv;
		char *viewname;

		/* keep scanning if it's a proc-level stats entry */
		if (!viewid)
			continue;

		/* just ignore stale stats entries */
		cv = GetContinuousView(viewid);
		if (!cv)
			continue;

		viewname = NameStr(cv->name);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(viewname);
		values[1] = CStringGetTextDatum((GetCQStatProcType(entry->key) == CQ_STAT_WORKER ? "worker" : "combiner"));
		values[2] = Int64GetDatum(entry->input_rows);
		values[3] = Int64GetDatum(entry->output_rows);
		values[4] = Int64GetDatum(entry->updates);
		values[5] = Int64GetDatum(entry->input_bytes);
		values[6] = Int64GetDatum(entry->output_bytes);
		values[7] = Int64GetDatum(entry->updated_bytes);
		values[8] = Int64GetDatum(entry->errors);

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * stream_stat_get
 */
Datum
stream_stat_get(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HTAB *stats;
	StreamStatEntry *entry;
	HASH_SEQ_STATUS *iter;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(5, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "input_batches", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "input_bytes", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		stats = stream_stat_fetch_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (StreamStatEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[5];
		bool nulls[5];
		HeapTuple tup;
		Datum result;

		if (!IsStream(entry->relid))
		{
			/* TODO(usmanm): Purge the entry. */
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(get_namespace_name(get_rel_namespace(entry->relid)));
		values[1] = CStringGetTextDatum(get_rel_name(entry->relid));
		values[2] = Int64GetDatum(entry->input_rows);
		values[3] = Int64GetDatum(entry->input_batches);
		values[4] = Int64GetDatum(entry->input_bytes);

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_stat_get
 *
 * Global PipelineDB stats. These don't go away when CVs are dropped.
 */
Datum
pipeline_stat_get(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HTAB *stats;
	CQStatEntry *global;
	TupleDesc	tupdesc;
	MemoryContext oldcontext;
	Datum values[12];
	bool nulls[12];
	Datum result;
	HeapTuple tup;
	CQStatsType ptype;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(12, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "start_time", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "updated_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "executions", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "errors", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "cv_create", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "cv_drop", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	/* there are only two rows: one for combiner stats and one for worker stats */
	if (funcctx->call_cntr > 1)
		SRF_RETURN_DONE(funcctx);

	stats = cq_stat_fetch_all();
	ptype = funcctx->call_cntr == 0 ? CQ_STAT_COMBINER : CQ_STAT_WORKER;
	global = cq_stat_get_global(stats, ptype);

	if (!global->start_ts)
		SRF_RETURN_DONE(funcctx);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum(ptype == CQ_STAT_COMBINER ? "combiner" : "worker");
	values[1] = TimestampTzGetDatum(global->start_ts);
	values[2] = Int64GetDatum(global->input_rows);
	values[3] = Int64GetDatum(global->output_rows);
	values[4] = Int64GetDatum(global->updates);
	values[5] = Int64GetDatum(global->input_bytes);
	values[6] = Int64GetDatum(global->output_bytes);
	values[7] = Int64GetDatum(global->updated_bytes);
	values[8] = Int64GetDatum(global->executions);
	values[9] = Int64GetDatum(global->errors);
	values[10] = Int64GetDatum(global->cv_create);
	values[11] = Int64GetDatum(global->cv_drop);

	tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tup);
	SRF_RETURN_NEXT(funcctx, result);
}

typedef struct
{
	Relation rel;
	HeapScanDesc scan;
} RelationScanData;

/*
 * pipeline_queries
 */
Datum
pipeline_queries(PG_FUNCTION_ARGS)
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
		tupdesc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "query", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = heap_open(PipelineQueryRelationId, AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
		data = (RelationScanData *) funcctx->user_fctx;

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_query row = (Form_pipeline_query) GETSTRUCT(tup);
		Datum values[4];
		bool nulls[4];
		HeapTuple rtup;
		Datum result;
		Datum tmp;
		bool isnull;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(row->id);
		values[1] = CStringGetTextDatum(get_namespace_name(row->namespace));
		values[2] = CStringGetTextDatum(NameStr(row->name));

		tmp = SysCacheGetAttr(PIPELINEQUERYNAMESPACENAME, tup, Anum_pipeline_query_query, &isnull);

		Assert(!isnull);

		values[3] = CStringGetTextDatum(deparse_query_def((Query *) stringToNode(TextDatumGetCString(tmp))));

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	heap_close(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_streams
 */
Datum
pipeline_streams(PG_FUNCTION_ARGS)
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
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "schema", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "inferred", BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "queries", OIDARRAYOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "desc", BYTEAOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;

	if (funcctx->user_fctx == NULL)
	{
		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		data = palloc(sizeof(RelationScanData));
		data->rel = heap_open(PipelineStreamRelationId, AccessShareLock);
		data->scan = heap_beginscan_catalog(data->rel, 0, NULL);
		funcctx->user_fctx = data;

		MemoryContextSwitchTo(old);
	}
	else
		data = (RelationScanData *) funcctx->user_fctx;

	while ((tup = heap_getnext(data->scan, ForwardScanDirection)) != NULL)
	{
		Form_pipeline_stream row = (Form_pipeline_stream) GETSTRUCT(tup);
		Datum values[5];
		bool nulls[5];
		HeapTuple rtup;
		Datum result;
		Datum tmp;
		bool isnull;
		bytea *bytes;
		int nbytes;
		int nwords;
		Bitmapset *bms;
		int dims[1];
		int lbs[] = { 1 };
		Datum *queries;
		bool *qnulls;
		int x;
		int i;
		char *relname = get_rel_name(row->relid);
		char *namespace;

		if (!relname)
			continue;

		namespace = get_namespace_name(get_rel_namespace(row->relid));
		if (!namespace)
			continue;

		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(namespace);
		values[1] = CStringGetTextDatum(relname);
		values[2] = BoolGetDatum(row->inferred);

		tmp = SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

		if (isnull)
			nulls[3] = true;
		else
		{
			bytes = (bytea *) DatumGetPointer(PG_DETOAST_DATUM(tmp));
			nbytes = VARSIZE(bytes) - VARHDRSZ;
			nwords = nbytes / sizeof(bitmapword);

			bms = palloc0(BITMAPSET_SIZE(nwords));
			bms->nwords = nwords;

			memcpy(bms->words, VARDATA(bytes), nbytes);

			dims[0] = bms_num_members(bms);
			queries = palloc0(sizeof(Datum) * dims[0]);
			qnulls = palloc0(sizeof(bool) * dims[0]);

			MemSet(qnulls, 0, dims[0]);
			i = 0;

			while ((x = bms_first_member(bms)) >= 0)
			{
				queries[i] = ObjectIdGetDatum(x);
				i++;
			}

			values[3] = PointerGetDatum(construct_md_array(queries, qnulls, 1, dims, lbs, OIDOID, 4, true, 'i'));
		}

		tmp = SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_desc, &isnull);

		if (isnull)
			nulls[4] = true;
		else
			values[4] = tmp;

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	heap_close(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_version
 *
 * Returns the PipelineDB version string
 */
Datum
pipeline_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PIPELINE_VERSION_STR));
}
