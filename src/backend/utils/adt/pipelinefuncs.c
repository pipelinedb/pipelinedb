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
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pipeline_query.h"
#include "catalog/pipeline_stream.h"
#include "catalog/pipeline_stream_fn.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "pipeline/analyzer.h"
#include "pipeline/ipc/microbatch.h"
#include "pipeline/reaper.h"
#include "pipeline/stream.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/jsonapi.h"
#include "utils/lsyscache.h"
#include "utils/pipelinefuncs.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct JsonObjectIntSumState
{
	char *current_key;
	HTAB *kv;
} JsonObjectIntSumState;

typedef struct JsonObjectIntSumEntry
{
	char key[NAMEDATALEN];
	int64 value;
} JsonObjectIntSumEntry;

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
	PgStat_StatCQEntry *entry;
	HASH_SEQ_STATUS *iter;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(17, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "start_time", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "updated_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "tuples_ps", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "bytes_ps", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "time_pb", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "tuples_pb", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "memory", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 15, "executions", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 16, "errors", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 17, "exec_ms", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		stats = pgstat_fetch_cqstat_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (PgStat_StatCQEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[17];
		bool nulls[17];
		HeapTuple tup;
		Datum result;
		pid_t pid = GetStatCQEntryProcPid(entry->key);

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
			pgstat_send_cqpurge(0, pid, GetStatCQEntryProcType(entry->key));
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum((GetStatCQEntryProcType(entry->key) == Worker ? "worker" : "combiner"));
		values[1] = Int32GetDatum(pid);
		values[2] = TimestampTzGetDatum(entry->start_ts);
		values[3] = Int64GetDatum(entry->input_rows);
		values[4] = Int64GetDatum(entry->output_rows);
		values[5] = Int64GetDatum(entry->updated_rows);
		values[6] = Int64GetDatum(entry->input_bytes);
		values[7] = Int64GetDatum(entry->output_bytes);
		values[8] = Int64GetDatum(entry->updated_bytes);
		values[9] = Int64GetDatum(entry->tuples_ps);
		values[10] = Int64GetDatum(entry->bytes_ps);
		values[11] = Int64GetDatum(entry->time_pb);
		values[12] = Int64GetDatum(entry->tuples_pb);
		values[13] = Int64GetDatum(entry->memory);
		values[14] = Int64GetDatum(entry->executions);
		values[15] = Int64GetDatum(entry->errors);
		values[16] = Int64GetDatum(entry->exec_ms);

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
	PgStat_StatCQEntry *entry;
	HASH_SEQ_STATUS *iter;

	if (SRF_IS_FIRSTCALL())
	{

		TupleDesc	tupdesc;
		MemoryContext oldcontext;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		tupdesc = CreateTemplateTupleDesc(14, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "updates", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "tuples_ps", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "bytes_ps", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "time_pb", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "tuples_pb", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 13, "errors", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 14, "exec_ms", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		stats = pgstat_fetch_cqstat_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (PgStat_StatCQEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[14];
		bool nulls[14];
		HeapTuple tup;
		Datum result;
		Oid viewid = GetStatCQEntryViewId(entry->key);
		ContQuery *cv;
		char *viewname;

		/* keep scanning if it's a proc-level stats entry */
		if (!viewid)
			continue;

		/* just ignore stale stats entries */
		cv = GetContQueryForId(viewid);
		if (!cv)
		{
			/* stale query, purge it */
			pgstat_send_cqpurge(viewid, 0, GetStatCQEntryProcType(entry->key));
			continue;
		}

		viewname = get_rel_name(cv->relid);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(viewname);
		values[1] = CStringGetTextDatum((GetStatCQEntryProcType(entry->key) == Worker ? "worker" : "combiner"));
		values[2] = Int64GetDatum(entry->input_rows);
		values[3] = Int64GetDatum(entry->output_rows);
		values[4] = Int64GetDatum(entry->updated_rows);
		values[5] = Int64GetDatum(entry->input_bytes);
		values[6] = Int64GetDatum(entry->output_bytes);
		values[7] = Int64GetDatum(entry->updated_bytes);
		values[8] = Int64GetDatum(entry->tuples_ps);
		values[9] = Int64GetDatum(entry->bytes_ps);
		values[10] = Int64GetDatum(entry->time_pb);
		values[11] = Int64GetDatum(entry->tuples_pb);
		values[12] = Int64GetDatum(entry->errors);
		values[13] = Int64GetDatum(entry->exec_ms);

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
	PgStat_StatStreamEntry *entry;
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

		stats = pgstat_fetch_streamstat_all();
		if (!stats)
			SRF_RETURN_DONE(funcctx);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stats);

		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (PgStat_StatStreamEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[5];
		bool nulls[5];
		HeapTuple tup;
		Datum result;

		if (!IsStream(entry->relid))
		{
			pgstat_send_streampurge(entry->relid);
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
	PgStat_StatCQEntry *global;
	TupleDesc	tupdesc;
	MemoryContext oldcontext;
	Datum values[12];
	bool nulls[12];
	Datum result;
	HeapTuple tup;
	ContQueryProcType ptype;

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

	stats = pgstat_fetch_cqstat_all();
	ptype = funcctx->call_cntr == 0 ? Combiner : Worker;
	global = pgstat_fetch_stat_global_cqentry(stats, ptype);

	if (!global->start_ts)
		SRF_RETURN_DONE(funcctx);

	MemSet(values, 0, sizeof(values));
	MemSet(nulls, 0, sizeof(nulls));

	values[0] = CStringGetTextDatum(ptype == Combiner ? "combiner" : "worker");
	values[1] = TimestampTzGetDatum(global->start_ts);
	values[2] = Int64GetDatum(global->input_rows);
	values[3] = Int64GetDatum(global->output_rows);
	values[4] = Int64GetDatum(global->updated_rows);
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
 * pipeline_views
 */
Datum
pipeline_views(PG_FUNCTION_ARGS)
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
		Datum values[5];
		bool nulls[5];
		HeapTuple rtup;
		Datum result;
		Datum tmp;
		bool isnull;
		char *relname;
		Oid nsp;

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

		tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);

		Assert(!isnull);

		values[4] = CStringGetTextDatum(deparse_query_def((Query *) stringToNode(TextDatumGetCString(tmp))));

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	heap_close(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

static int
cstring_cmp(const void *a, const void *b)
{
	const char **ia = (const char **)a;
	const char **ib = (const char **)b;
	return strcmp(*ia, *ib);
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

		tmp = SysCacheGetAttr(PIPELINESTREAMRELID, tup, Anum_pipeline_stream_queries, &isnull);

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
 * pipeline_version
 *
 * Returns the PipelineDB version string
 */
Datum
pipeline_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(PIPELINE_VERSION_STR));
}

/*
 * handle_key_start
 */
static void
handle_key_start(void *_state, char *fname, bool isnull)
{
	JsonObjectIntSumState *state = (JsonObjectIntSumState *) _state;

	if (strlen(fname) > NAMEDATALEN)
		elog(ERROR, "json_object_sum requires keys to be no longer than %d characters", NAMEDATALEN);

	state->current_key = fname;
}

/*
 * handle_scalar
 */
static void
handle_scalar(void *_state, char *token, JsonTokenType tokentype)
{
	bool found;
	JsonObjectIntSumState *state = (JsonObjectIntSumState *) _state;
	JsonObjectIntSumEntry *entry = (JsonObjectIntSumEntry *) hash_search(state->kv, state->current_key, HASH_ENTER, &found);
	int64 result;

	if (!found)
		entry->value = 0;

	(void) scanint8(token, false, &result);
	entry->value += result;
}

/*
 * json_object_int_sum_startup
 */
static JsonObjectIntSumState *
json_object_int_sum_startup(FunctionCallInfo fcinfo)
{
	MemoryContext oldcontext;
	JsonSemAction *sem;
	JsonObjectIntSumState *state;
	HASHCTL ctl;

	oldcontext = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
	state = palloc0(sizeof(JsonObjectIntSumState));

	MemSet(&ctl, 0, sizeof(HASHCTL));
	ctl.keysize = NAMEDATALEN;
	ctl.entrysize = sizeof(JsonObjectIntSumEntry);
	ctl.hcxt = CurrentMemoryContext;
	state->kv = hash_create("json_object_sum", 32, &ctl, HASH_ELEM | HASH_CONTEXT);

	sem = palloc0(sizeof(JsonSemAction));
	sem->semstate = (void *) state;
	sem->object_field_start = handle_key_start;
	sem->scalar = handle_scalar;
	fcinfo->flinfo->fn_extra = (void *) sem;

	MemoryContextSwitchTo(oldcontext);

	return state;
}

/*
 * json_object_int_sum_transfn
 */
Datum
json_object_int_sum_transfn(PG_FUNCTION_ARGS)
{
	MemoryContext aggcontext;
	MemoryContext oldcontext;
	JsonObjectIntSumState *state;
	JsonLexContext *lex;
	JsonSemAction *sem;
	char *raw = TextDatumGetCString(PG_GETARG_TEXT_P(1));

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "json_object_sum_transfn called in non-aggregate context");
	}

	if (PG_ARGISNULL(0))
	{
		oldcontext = MemoryContextSwitchTo(aggcontext);
		state = json_object_int_sum_startup(fcinfo);
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		state = (JsonObjectIntSumState *) PG_GETARG_POINTER(0);
	}

	lex = makeJsonLexContextCstringLen(raw, strlen(raw), true);
	sem = (JsonSemAction *) fcinfo->flinfo->fn_extra;
	sem->semstate = (void *) state;
	pg_parse_json(lex, sem);

	PG_RETURN_POINTER(state);
}

/*
 * json_object_int_sum_transout
 */
Datum
json_object_int_sum_transout(PG_FUNCTION_ARGS)
{
	JsonObjectIntSumState *state;
	HASH_SEQ_STATUS seq;
	JsonObjectIntSumEntry *entry;
	StringInfoData buf;
	bool first = true;

	if (!IsContQueryProcess() && !AggCheckCallContext(fcinfo, NULL))
		PG_RETURN_TEXT_P(PG_GETARG_TEXT_P(0));

	state = (JsonObjectIntSumState *) PG_GETARG_POINTER(0);
	initStringInfo(&buf);
	appendStringInfoString(&buf, "{ ");

	hash_seq_init(&seq, state->kv);
	while ((entry = (JsonObjectIntSumEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (!first)
			appendStringInfo(&buf, ", \"%s\": %ld", entry->key, entry->value);
		else
			appendStringInfo(&buf, "\"%s\": %ld", entry->key, entry->value);
		first = false;
	}

	appendStringInfoString(&buf, " }");

	PG_RETURN_TEXT_P(cstring_to_text_with_len(buf.data, buf.len));
}

/*
 * pipeline_transforms
 */
Datum
pipeline_transforms(PG_FUNCTION_ARGS)
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
		Datum values[7];
		bool nulls[7];
		HeapTuple rtup;
		Datum result;
		Datum tmp;
		bool isnull;
		char *fn;
		char *relname;
		Oid nsp;

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

		if (OidIsValid(row->tgfn))
		{
			if (!FunctionIsVisible(row->tgfn))
				fn = quote_qualified_identifier(get_namespace_name(get_func_namespace(row->tgfn)), get_func_name(row->tgfn));
			else
				fn = quote_qualified_identifier(NULL, get_func_name(row->tgfn));

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

			val = DatumGetByteaP(SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_tgargs, &isnull));
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

		tmp = SysCacheGetAttr(PIPELINEQUERYRELID, tup, Anum_pipeline_query_query, &isnull);
		Assert(!isnull);
		values[6] = CStringGetTextDatum(deparse_query_def((Query *) stringToNode(TextDatumGetCString(tmp))));

		rtup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(rtup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	heap_endscan(data->scan);
	heap_close(data->rel, AccessShareLock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_flush
 */
Datum
pipeline_flush(PG_FUNCTION_ARGS)
{
	int i;
	ContQueryDatabaseMetadata *db_meta = GetMyContQueryDatabaseMetadata();
	uint64 start_generation = pg_atomic_read_u64(&db_meta->generation);
	microbatch_ack_t *ack = microbatch_ack_new(STREAM_INSERT_FLUSH);
	microbatch_t *mb = microbatch_new(FlushTuple, NULL, NULL);
	bool success;

	pzmq_init();

	microbatch_add_ack(mb, ack);

	for (i = 0; i < continuous_query_num_workers; i++)
		microbatch_send_to_worker(mb, i);

	microbatch_destroy(mb);

	microbatch_ack_increment_wtups(ack, continuous_query_num_workers);
	success = microbatch_ack_wait(ack, db_meta, start_generation);
	microbatch_ack_free(ack);

	PG_RETURN_BOOL(success);
}

#define UPDATE_TTL_TEMPLATE "UPDATE pipeline_query SET ttl = %d, ttl_attno = %d WHERE id = %d;"

/*
 * set_ttl
 *
 * Set a continuous view's TTL info
 */
Datum
set_ttl(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	HeapTuple tup;
	Datum result;
	RangeVar *cv_name;
	Datum values[2];
	bool nulls[2];
	TupleDesc	tupdesc;
	MemoryContext oldcontext;
	Interval *ttli = PG_ARGISNULL(1) ? NULL : PG_GETARG_INTERVAL_P(1);
	text *ttl_col = PG_ARGISNULL(2) ? NULL : PG_GETARG_TEXT_P(2);
	TupleDesc desc;
	ContQuery *cv;
	Relation matrel;
	int ttl = -1;
	char *ttl_colname = ttl_col ? TextDatumGetCString(ttl_col) : NULL;
	AttrNumber ttl_attno = InvalidAttrNumber;
	int i;
	StringInfoData buf;

	if (PG_ARGISNULL(0))
		elog(ERROR, "continuous view name is null");

	cv_name = makeRangeVarFromNameList(textToQualifiedNameList(PG_GETARG_TEXT_P(0)));

	if (!SRF_IS_FIRSTCALL())
	{
		funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
		SRF_RETURN_DONE(funcctx);
	}

	/* create a function context for cross-call persistence */
	funcctx = SRF_FIRSTCALL_INIT();

	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	/* build tupdesc for result tuples */
	tupdesc = CreateTemplateTupleDesc(2, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "ttl", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "ttl_attno", INT2OID, -1, 0);

	funcctx->tuple_desc = BlessTupleDesc(tupdesc);

	MemoryContextSwitchTo(oldcontext);

	cv = GetContQueryForView(cv_name);

	if (cv == NULL)
		elog(ERROR, "continuous view \"%s\" does not exist", cv_name->relname);

	if (IsSWContView(cv_name))
		elog(ERROR, "the ttl of a sliding-window continuous view cannot be changed");

	if (ttl_colname)
	{
		matrel = heap_openrv(cv->matrel, NoLock);

		/*
		 * Find which attribute number corresponds to the given column name
		 */
		desc = RelationGetDescr(matrel);
		for (i = 0; i < desc->natts; i++)
		{
			Form_pg_attribute att = desc->attrs[i];
			if (pg_strcasecmp(ttl_colname, NameStr(att->attname)) == 0)
			{
				ttl_attno = att->attnum;
				if (att->atttypid != TIMESTAMPOID && att->atttypid != TIMESTAMPTZOID)
					elog(ERROR, "ttl_column must refer to a timestamp or timestamptz column");
				break;
			}
		}
		heap_close(matrel, NoLock);

		if (!AttributeNumberIsValid(ttl_attno))
			elog(ERROR, "column \"%s\" does not exist", ttl_colname);
	}

	ttl = ttli ? IntervalToEpoch(ttli) : -1;

	initStringInfo(&buf);
	appendStringInfo(&buf, UPDATE_TTL_TEMPLATE, ttl, ttl_attno, cv->id);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI manager");

	if (SPI_execute(buf.data, false, 0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_execute failed: %s", buf.data);

	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	values[0] = Int32GetDatum(ttl);

	if (!AttributeNumberIsValid(ttl_attno))
		nulls[1] = true;

	values[1] = Int16GetDatum(ttl_attno);

	MemSet(nulls, false, sizeof(nulls));

	tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
	result = HeapTupleGetDatum(tup);
	SRF_RETURN_NEXT(funcctx, result);
}

/*
 * ttl_expire
 */
Datum
ttl_expire(PG_FUNCTION_ARGS)
{
	RangeVar *cv_name;
	RangeVar *matrel;
	int result;
	int save_batch_size = continuous_query_ttl_expiration_batch_size;

	if (PG_ARGISNULL(0))
		elog(ERROR, "continuous view name cannot be NULL");

	cv_name = makeRangeVarFromNameList(textToQualifiedNameList(PG_GETARG_TEXT_P(0)));
	matrel = GetMatRelName(cv_name);

	if (!IsTTLContView(cv_name))
		elog(ERROR, "continuous view \"%s\" does not have a TTL", cv_name->relname);

	/*
	 * DELETE everything
	 */
	continuous_query_ttl_expiration_batch_size = 0;
	result = DeleteTTLExpiredRows(cv_name, matrel);
	continuous_query_ttl_expiration_batch_size = save_batch_size;

	PG_RETURN_INT32(result);
}

static bool
set_cq_enabled(RangeVar *name, bool activate)
{
	bool changed = false;
	Oid query_id;
	Relation pipeline_query;

	pipeline_query = heap_open(PipelineQueryRelationId, ExclusiveLock);
	query_id = GetContQueryId(name);

	if (!OidIsValid(query_id))
		elog(ERROR, "\"%s\" does not exist", name->relname);

	changed = ContQuerySetActive(query_id, activate);
	if (changed)
		UpdatePipelineStreamCatalog();

	heap_close(pipeline_query, NoLock);

	return changed;
}
/*
 * activate
 *
 * Activate the given continuous view/transform
 */
Datum
activate(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	bool result = set_cq_enabled(rv, true);

	PG_RETURN_BOOL(result);
}

/*
 * deactivate
 *
 * Deactivate the given continuous view/transform
 */
Datum
deactivate(PG_FUNCTION_ARGS)
{
	text *relname = PG_GETARG_TEXT_P(0);
	RangeVar *rv = makeRangeVarFromNameList(textToQualifiedNameList(relname));
	bool result = set_cq_enabled(rv, false);

	PG_RETURN_BOOL(result);
}
