/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * cqstatfuncs.c
 *		Functions for retrieving CQ statistics
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/cqstatfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "pgstat.h"
#include "funcapi.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/cqstatfuncs.h"
#include "utils/rel.h"

/*
 * cq_stat_proc_get
 *
 * Look up CQ process-level stats in the stats hashtables
 * and return them in the form of rows so that we can use
 * this function as a data source in views
 */
Datum
cq_stat_proc_get(PG_FUNCTION_ARGS)
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
		tupdesc = CreateTemplateTupleDesc(12, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "start_time", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "output_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 7, "updated_rows", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 8, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 9, "output_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 10, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 11, "executions", INT8OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 12, "errors", INT8OID, -1, 0);

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
		Datum values[12];
		bool nulls[12];
		HeapTuple tup;
		Datum result;
		int pid = GetCQStatProcPid(entry->key);
		Oid viewoid = GetCQStatView(entry->key);
		Relation rel;
		char *viewname;

		/* keep scanning if it's a CQ-level stats entry */
		if (!pid)
			continue;

		/* ignore and purge stale stats entries */
		rel = try_relation_open(viewoid, NoLock);
		if (!rel)
		{
			cq_stat_send_purge(viewoid, pid, GetCQStatProcType(entry->key));
			continue;
		}

		viewname = RelationGetRelationName(rel);
		heap_close(rel, NoLock);

		/*
		 * If a pid doesn't exist, kill() with signal 0 will fail and set
		 * errno to ESRCH
		 */
		if (kill(pid, 0) == -1 && errno == ESRCH)
		{
			/* stale proc, purge it */
			cq_stat_send_purge(viewoid, pid, GetCQStatProcType(entry->key));
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(viewname);
		values[1] = CStringGetTextDatum((GetCQStatProcType(entry->key) == CQ_STAT_WORKER ? "worker" : "combiner"));
		values[2] = Int32GetDatum(pid);
		values[3] = TimestampTzGetDatum(entry->start_ts);
		values[4] = Int64GetDatum(entry->input_rows);
		values[5] = Int64GetDatum(entry->output_rows);
		values[6] = Int64GetDatum(entry->updates);
		values[7] = Int64GetDatum(entry->input_bytes);
		values[8] = Int64GetDatum(entry->output_bytes);
		values[9] = Int64GetDatum(entry->updated_bytes);
		values[10] = Int64GetDatum(entry->executions);
		values[11] = Int64GetDatum(entry->errors);

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
		int pid = GetCQStatProcPid(entry->key);
		Oid viewoid = GetCQStatView(entry->key);
		Relation rel;
		char *viewname;

		/* keep scanning if it's a proc-level stats entry */
		if (pid)
			continue;

		/* just ignore stale stats entries */
		rel = try_relation_open(viewoid, NoLock);
		if (!rel)
			continue;

		viewname = RelationGetRelationName(rel);
		heap_close(rel, NoLock);

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
