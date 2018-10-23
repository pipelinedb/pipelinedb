/*-------------------------------------------------------------------------
 *
 * stats.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_CORE_STATS_H
#define PIPELINE_CORE_STATS_H

#include "postgres.h"

#include "utils/timestamp.h"

typedef struct ProcStatsKey
{
	Oid dbid;
	pid_t pid;
	Oid cqid;
} ProcStatsKey;

typedef struct ProcStatsEntry
{
	ProcStatsKey key;
	ContQueryProcType proc_type;
	TimestampTz start_time;
	pg_atomic_uint64 input_rows;
	pg_atomic_uint64 output_rows;
	pg_atomic_uint64 updated_rows;
	pg_atomic_uint64 input_bytes;
	pg_atomic_uint64 output_bytes;
	pg_atomic_uint64 updated_bytes;
	pg_atomic_uint64 executions;
	pg_atomic_uint64 errors;
	pg_atomic_uint64 exec_ms;
} ProcStatsEntry;

typedef struct StreamStatsKey
{
	Oid dbid;
	Oid relid;
} StreamStatsKey;

typedef struct StreamStatsEntry
{
	StreamStatsKey key;
	pg_atomic_uint64 input_rows;
	pg_atomic_uint64 input_batches;
	pg_atomic_uint64 input_bytes;
} StreamStatsEntry;

/*
 * If we're a CQ process, this tracks our various runtime stats
 */
extern ProcStatsEntry *MyProcStatCQEntry;

extern Size StatsShmemSize(void);
extern void StatsRequestLWLocks(void);
extern void StatsShmemInit(void);
extern void PurgeDeadProcStats(Oid cqid);
extern void PurgeDeadStreamStats(Oid relid);
extern void GetInstallationStats(uint64 *events_inp, uint64 *batches_inp, uint64 *bytes_inp, uint64 *errorsp, int *cqcountp, int *ccountp, int *wcountp);
extern StreamStatsEntry *GetStreamStatsEntry(Oid relid);

#define StatsIncrementStreamInsert(relid, ntups, nbatches, nbytes) \
	do { \
		StreamStatsEntry *__stats = GetStreamStatsEntry((relid)); \
		pg_atomic_fetch_add_u64(&__stats->input_rows, (ntups)); \
		pg_atomic_fetch_add_u64(&__stats->input_batches, (nbatches)); \
		pg_atomic_fetch_add_u64(&__stats->input_bytes, (nbytes)); \
	} \
	while(0)

#define StatsIncrementCQRead(nrows, nbytes) \
	do { \
		if (MyProcStatCQEntry) \
		{ \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->input_rows, (nrows)); \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->input_bytes, (nbytes)); \
		} \
	} \
	while(0)

#define StatsIncrementCQWrite(nrows, nbytes) \
	do { \
		if (MyProcStatCQEntry) \
		{ \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->output_rows, (nrows)); \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->output_bytes, (nbytes)); \
		} \
	} \
	while(0)

#define StatsIncrementCQUpdate(nrows, nbytes) \
	do { \
		if (MyProcStatCQEntry) \
		{ \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->updated_rows, (nrows)); \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->updated_bytes, (nbytes)); \
		} \
	} \
	while(0)

#define StatsIncrementCQExec(count) \
	do { \
		if (MyProcStatCQEntry) \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->executions, (count)); \
	} \
	while(0)

#define StatsIncrementCQError(count) \
	do { \
		if (MyProcStatCQEntry) \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->errors, (count)); \
	} \
	while(0)

#define StatsIncrementCQExecMs(ms) \
	do { \
		if (MyProcStatCQEntry) \
			pg_atomic_fetch_add_u64(&MyProcStatCQEntry->exec_ms, (ms)); \
	} \
	while(0)

extern ProcStatsEntry *ProcStatsInit(Oid cqid, pid_t pid);
extern StreamStatsEntry *StreamStatsInit(Oid relid);

#endif
