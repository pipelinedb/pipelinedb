/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 * Functions for handling event streams
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAM_H
#define STREAM_H

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

#define QueryIsStreaming(query) ((query)->isContinuous)
#define QueryIsCombine(query) ((query)->isCombine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* returns true if the given PreparedStreamInsertStmt has pending inserts */
#define HasPendingInserts(pstmt) (pstmt && (pstmt)->inserts)

typedef struct PreparedStreamInsertStmt
{
	char name[NAMEDATALEN];
	Oid relid; /* stream relid */
	List *cols; /* column names for INSERTs */
	List *inserts; /* list of ParamListInfoData for the INSERT */
} PreparedStreamInsertStmt;

/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool synchronous_stream_insert;
extern char *stream_targets;

extern PreparedStreamInsertStmt *StorePreparedStreamInsert(const char *name, RangeVar *stream, List *cols);
extern void AddPreparedStreamInsert(PreparedStreamInsertStmt *stmt, ParamListInfoData *params);
extern PreparedStreamInsertStmt *FetchPreparedStreamInsert(const char *name);
extern void DropPreparedStreamInsert(const char *name);
extern int InsertIntoStreamPrepared(PreparedStreamInsertStmt *pstmt);
extern int InsertIntoStream(InsertStmt *ins, List *params);
extern uint64 CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples);

/* Represents a single batch of inserts made into a stream. */
typedef struct InsertBatch {
	int id;
	/* Number of acks from workers */
	int num_wacks;
	/* Number of acks from combiners */
	int num_cacks;
	/* Total number of tuples sent to workers */
	int num_wtups;
	/* Total number of tuples sent to combiners */
	int num_ctups;
	slock_t mutex;
} InsertBatch;

/* Represents the number of tuples processed for the stream batch. */
typedef struct InsertBatchAck {
	int batch_id;
	InsertBatch *batch;
	int count;
} InsertBatchAck;

extern InsertBatch *InsertBatchCreate(void);
extern void InsertBatchWaitAndRemove(InsertBatch *batch, int num_tuples);
extern void InsertBatchIncrementNumCTuples(InsertBatch *batch);
extern void InsertBatchMarkAcked(InsertBatchAck *ack);


#endif
