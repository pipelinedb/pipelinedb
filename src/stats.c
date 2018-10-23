/*-------------------------------------------------------------------------
 *
 * stats.c
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "scheduler.h"
#include "stats.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

static HTAB *proc_stats = NULL;
static HTAB *stream_stats = NULL;

ProcStatsEntry *MyProcStatCQEntry = NULL;

/*
 * StatsRequestLWLocks
 */
void
StatsRequestLWLocks(void)
{
	RequestNamedLWLockTranche("pipelinedb_proc_stats", 1);
	RequestNamedLWLockTranche("pipelinedb_stream_stats", 1);
}

/*
 * StatsShmemSize
 *
 * Start off with some rough estimations of each hashtable's size,
 * we'll grow them as necessary
 */
Size
StatsShmemSize(void)
{
	Size size = 0;

	/*
	 * proc_stats
	 */
	size = add_size(size, hash_estimate_size(4 * NUM_BG_WORKERS_PER_DB, sizeof(ProcStatsEntry)));

	/*
	 * query_stats
	 */
	size = add_size(size, hash_estimate_size(32, sizeof(ProcStatsEntry)));

	/*
	 * stream_stats
	 */
	size = add_size(size, hash_estimate_size(16, sizeof(ProcStatsEntry)));

	/*
	 * db_stats
	 */
	size = add_size(size, hash_estimate_size(4, sizeof(ProcStatsEntry)));

	return size;
}

/*
 * StatsShmemStartup
 */
void
StatsShmemInit(void)
{
	HASHCTL ctl;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(ProcStatsKey);
	ctl.entrysize = sizeof(ProcStatsEntry);

	proc_stats = ShmemInitHash("proc_stats", 4 * NUM_BG_WORKERS_PER_DB, 32 * 4 * NUM_BG_WORKERS_PER_DB, &ctl, HASH_ELEM | HASH_BLOBS);

	MemSet(&ctl, 0, sizeof(HASHCTL));

	ctl.keysize = sizeof(StreamStatsKey);
	ctl.entrysize = sizeof(StreamStatsEntry);

	stream_stats = ShmemInitHash("stream_stats", 32, 1024, &ctl, HASH_ELEM | HASH_BLOBS);

	LWLockRelease(AddinShmemInitLock);
}

/*
 * PurgeDeadProcStats
 */
void
PurgeDeadProcStats(Oid cqid)
{
	HASH_SEQ_STATUS iter;
	ProcStatsEntry *entry;
	List *to_delete = NIL;
	ListCell *lc;

	Assert(proc_stats);
	hash_seq_init(&iter, proc_stats);

	while ((entry = (ProcStatsEntry *) hash_seq_search(&iter)) != NULL)
	{
		if (entry->key.dbid != MyDatabaseId)
			continue;
		if (entry->key.cqid != cqid)
			continue;
		to_delete = lappend(to_delete, &entry->key);
	}

	foreach(lc, to_delete)
	{
		ProcStatsKey *key = (ProcStatsKey *) lfirst(lc);
		hash_search(proc_stats, key, HASH_REMOVE, NULL);
	}
}

/*
 * PurgeDeadStreamStats
 */
void
PurgeDeadStreamStats(Oid relid)
{
	StreamStatsKey key;

	key.dbid = MyDatabaseId;
	key.relid = relid;

	Assert(stream_stats);
	hash_search(stream_stats, &key, HASH_REMOVE, NULL);
}

/*
 * GetInstallationStats
 */
void
GetInstallationStats(uint64 *events_inp, uint64 *batches_inp, uint64 *bytes_inp, uint64 *errorsp, int *cqcountp, int *ccountp, int *wcountp)
{
	static LWLock *stream_lock = NULL;
	static LWLock *proc_lock = NULL;
	HASH_SEQ_STATUS iter;
	StreamStatsEntry *se;
	ProcStatsEntry *pe;
	uint64 events_in = 0;
	uint64 batches_in = 0;
	uint64 bytes_in = 0;
	uint64 errors = 0;
	Bitmapset *cqids = NULL;
	int ccount = 0;
	int wcount = 0;

	if (!stream_lock)
		stream_lock = &(GetNamedLWLockTranche("pipelinedb_stream_stats")->lock);
	if (!proc_lock)
		proc_lock = &(GetNamedLWLockTranche("pipelinedb_proc_stats")->lock);

	LWLockAcquire(stream_lock, LW_SHARED);

	hash_seq_init(&iter, stream_stats);
	while ((se = (StreamStatsEntry *) hash_seq_search(&iter)) != NULL)
	{
		events_in += pg_atomic_read_u64(&se->input_rows);
		batches_in += pg_atomic_read_u64(&se->input_batches);
		bytes_in += pg_atomic_read_u64(&se->input_bytes);
	}

	LWLockRelease(stream_lock);

	LWLockAcquire(proc_lock, LW_SHARED);

	hash_seq_init(&iter, proc_stats);
	while ((pe = (ProcStatsEntry *) hash_seq_search(&iter)) != NULL)
	{
		cqids = bms_add_member(cqids, pe->key.cqid);
		if (pe->proc_type == Combiner)
			ccount++;
		else if (pe->proc_type == Worker)
			wcount++;
		errors += pg_atomic_read_u64(&pe->errors);
	}

	*events_inp = events_in;
	*batches_inp = batches_in;
	*bytes_inp = bytes_in;
	*errorsp = errors;
	*cqcountp = bms_num_members(cqids);
	*ccountp = ccount;
	*wcountp = wcount;

	bms_free(cqids);

	LWLockRelease(proc_lock);
}

/*
 * get_proc_stats_entry
 */
static ProcStatsEntry *
get_proc_stats_entry(Oid cqid, pid_t pid, bool *found)
{
	bool f;
	ProcStatsEntry *entry = NULL;
	ProcStatsKey key;
	static LWLock *lock = NULL;

	if (!lock)
		lock = &(GetNamedLWLockTranche("pipelinedb_proc_stats")->lock);

	key.dbid = MyDatabaseId;
	key.pid = pid;
	key.cqid = cqid;

	if (!found)
		found = &f;

	Assert(lock);
	Assert(proc_stats);

	/*
	 * Since new entries are rarely created, we first look up the entry with a shared lock
	 */
	LWLockAcquire(lock, LW_SHARED);

	entry = (ProcStatsEntry *) hash_search(proc_stats, &key, HASH_FIND, NULL);

	if (!entry)
	{
		/*
		 * Entry doesn't exist yet, so now upgrade to an exclusive lock and create it
		 */
		LWLockRelease(lock);
		LWLockAcquire(lock, LW_EXCLUSIVE);
		entry = (ProcStatsEntry *) hash_search(proc_stats, &key, HASH_ENTER, found);

		if (!*found)
		{
			pg_atomic_write_u64(&entry->input_rows, 0);
			pg_atomic_write_u64(&entry->output_rows, 0);
			pg_atomic_write_u64(&entry->updated_rows, 0);
			pg_atomic_write_u64(&entry->input_bytes, 0);
			pg_atomic_write_u64(&entry->output_bytes, 0);
			pg_atomic_write_u64(&entry->updated_bytes, 0);
			pg_atomic_write_u64(&entry->executions, 0);
			pg_atomic_write_u64(&entry->errors, 0);
			pg_atomic_write_u64(&entry->exec_ms, 0);
		}
	}

	LWLockRelease(lock);
	Assert(entry);

	return entry;
}

/*
 * GetStreamStatsEntry
 */
StreamStatsEntry *
GetStreamStatsEntry(Oid relid)
{
	bool found;
	StreamStatsEntry *entry;
	StreamStatsKey key;
	static LWLock *lock = NULL;

	if (!lock)
		lock = &(GetNamedLWLockTranche("pipelinedb_stream_stats")->lock);

	Assert(lock);
	Assert(stream_stats);

	key.dbid = MyDatabaseId;
	key.relid = relid;

	/*
	 * Since new entries are rarely created, we first look up the entry with a shared lock
	 */
	LWLockAcquire(lock, LW_SHARED);
	entry = (StreamStatsEntry *) hash_search(stream_stats, &key, HASH_FIND, NULL);

	if (!entry)
	{
		/*
		 * Entry doesn't exist yet, so now upgrade to an exclusive lock and create it
		 */
		LWLockRelease(lock);
		LWLockAcquire(lock, LW_EXCLUSIVE);
		entry = (StreamStatsEntry *) hash_search(stream_stats, &key, HASH_ENTER, &found);

		if (!found)
		{
			pg_atomic_write_u64(&entry->input_rows, 0);
			pg_atomic_write_u64(&entry->input_batches, 0);
			pg_atomic_write_u64(&entry->input_bytes, 0);
		}
	}

	LWLockRelease(lock);
	Assert(entry);

	return entry;
}

/*
 * delete_my_proc_stats_entry
 */
static void
delete_proc_stats_entry(pid_t pid)
{
	ProcStatsKey key;

	key.dbid = MyDatabaseId;
	key.pid = pid;
	key.cqid = 0;

	hash_search(proc_stats, &key, HASH_REMOVE, NULL);
}

/*
 * ProcStatsInit
 */
ProcStatsEntry *
ProcStatsInit(Oid cqid, pid_t pid)
{
	ProcStatsEntry *entry;
	bool found;

	if (!MyContQueryProc)
		return NULL;

	entry = get_proc_stats_entry(cqid, pid, &found);
	Assert(entry);

	entry->start_time = GetCurrentTimestamp();
	entry->proc_type = MyContQueryProc->type;

	return entry;
}

/*
 * StreamStatsInit
 */
StreamStatsEntry *
StreamStatsInit(Oid relid)
{
	StreamStatsEntry *entry;

	entry = GetStreamStatsEntry(relid);
	Assert(entry);

	return entry;
}

/*
 * get_proc_type_str
 */
static char *
get_proc_type_str(ContQueryProcType type)
{
	switch (type)
	{
		case Combiner:
			return "combiner";
			break;
		case Worker:
			return "worker";
			break;
		case Queue:
			return "queue";
			break;
		case Reaper:
			return "reaper";
			break;
		case Scheduler:
			return "scheduler";
			break;
	}

	return NULL;
}

/*
 * pipeline_get_proc_query_stats
 */
PG_FUNCTION_INFO_V1(pipeline_get_proc_query_stats);
Datum
pipeline_get_proc_query_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	ProcStatsEntry *entry;
	Datum result;
	HASH_SEQ_STATUS *iter;
	LWLock *lock = &(GetNamedLWLockTranche("pipelinedb_proc_stats")->lock);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc desc;
		MemoryContext old;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		desc = CreateTemplateTupleDesc(13, false);
		TupleDescInitEntry(desc, (AttrNumber) 1, "type", TEXTOID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 2, "pid", INT4OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 3, "start_time", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 4, "query_id", INT4OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 5, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 6, "inserted_rows", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 7, "updated_rows", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 8, "input_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 9, "inserted_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 10, "updated_bytes", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 11, "executions", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 12, "errors", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 13, "exec_ms", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(desc);

		LWLockAcquire(lock, LW_SHARED);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, proc_stats);
		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(old);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (ProcStatsEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[13];
		bool nulls[13];
		HeapTuple tup;
		pid_t pid = entry->key.pid;

		/*
		 * If a pid doesn't exist, kill() with signal 0 will fail and set
		 * errno to ESRCH
		 */
		if (kill(pid, 0) == -1 && errno == ESRCH)
		{
			/* stale proc, purge it */
			delete_proc_stats_entry(pid);
			continue;
		}

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(get_proc_type_str(entry->proc_type));
		values[1] = Int32GetDatum(pid);
		values[2] = TimestampTzGetDatum(entry->start_time);
		values[3] = Int32GetDatum(entry->key.cqid);
		values[4] = Int64GetDatum(pg_atomic_read_u64(&entry->input_rows));
		values[5] = Int64GetDatum(pg_atomic_read_u64(&entry->output_rows));
		values[6] = Int64GetDatum(pg_atomic_read_u64(&entry->updated_rows));
		values[7] = Int64GetDatum(pg_atomic_read_u64(&entry->input_bytes));
		values[8] = Int64GetDatum(pg_atomic_read_u64(&entry->output_bytes));
		values[9] = Int64GetDatum(pg_atomic_read_u64(&entry->updated_bytes));
		values[10] = Int64GetDatum(pg_atomic_read_u64(&entry->executions));
		values[11] = Int64GetDatum(pg_atomic_read_u64(&entry->errors));
		values[12] = Int64GetDatum(pg_atomic_read_u64(&entry->exec_ms));

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	LWLockRelease(lock);

	SRF_RETURN_DONE(funcctx);
}

/*
 * pipeline_get_stream_stats
 */
PG_FUNCTION_INFO_V1(pipeline_get_stream_stats);
Datum
pipeline_get_stream_stats(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	StreamStatsEntry *entry;
	Datum result;
	HASH_SEQ_STATUS *iter;
	LWLock *lock = &(GetNamedLWLockTranche("pipelinedb_stream_stats")->lock);

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc desc;
		MemoryContext old;

		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		old = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* build tupdesc for result tuples */
		desc = CreateTemplateTupleDesc(4, false);
		TupleDescInitEntry(desc, (AttrNumber) 1, "relid", OIDOID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 2, "input_rows", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 3, "input_batches", INT8OID, -1, 0);
		TupleDescInitEntry(desc, (AttrNumber) 4, "input_bytes", INT8OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(desc);

		LWLockAcquire(lock, LW_SHARED);

		iter = palloc0(sizeof(HASH_SEQ_STATUS));
		hash_seq_init(iter, stream_stats);
		funcctx->user_fctx = (void *) iter;

		MemoryContextSwitchTo(old);
	}

	funcctx = (FuncCallContext *) fcinfo->flinfo->fn_extra;
	iter = (HASH_SEQ_STATUS *) funcctx->user_fctx;

	while ((entry = (StreamStatsEntry *) hash_seq_search(iter)) != NULL)
	{
		Datum values[4];
		bool nulls[4];
		HeapTuple tup;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ObjectIdGetDatum(entry->key.relid);
		values[1] = Int64GetDatum(pg_atomic_read_u64(&entry->input_rows));
		values[2] = Int64GetDatum(pg_atomic_read_u64(&entry->input_batches));
		values[3] = Int64GetDatum(pg_atomic_read_u64(&entry->input_bytes));

		tup = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tup);
		SRF_RETURN_NEXT(funcctx, result);
	}

	LWLockRelease(lock);

	SRF_RETURN_DONE(funcctx);
}
