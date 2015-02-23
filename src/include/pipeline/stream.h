/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 * Functions for handling event streams
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
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#define EventStreamNeedsOpen(stream) (stream->state != STREAM_STATE_OPEN)
#define QueryIsStreaming(query) ((query)->is_continuous)
#define QueryIsCombine(query) ((query)->is_combine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous || false)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* returns true if the given PreparedStreamInsertStmt has pending inserts */
#define HasPendingInserts(pstmt) (pstmt && (pstmt)->inserts)

typedef struct PreparedStreamInsertStmt
{
	char name[NAMEDATALEN];
	/* destination stream for INSERTs */
	char *stream;
	/* column names for INSERTs */
	List *cols;
	/* List of ParamListInfoData for the INSERT */
	List *inserts;
	/* TupleDesc for these INSERTs */
	TupleDesc desc;
} PreparedStreamInsertStmt;

/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool DebugSyncStreamInsert;

extern PreparedStreamInsertStmt *StorePreparedStreamInsert(const char *name, const char *stream, List *cols);
extern void AddPreparedStreamInsert(PreparedStreamInsertStmt *stmt, ParamListInfoData *params);
extern PreparedStreamInsertStmt *FetchPreparedStreamInsert(const char *name);
extern void DropPreparedStreamInsert(const char *name);
extern bool InsertTargetIsStream(InsertStmt *ins);
extern int InsertIntoStreamPrepared(PreparedStreamInsertStmt *pstmt);
extern int InsertIntoStream(InsertStmt *ins, List *values);
extern bool IsInputStream(const char *stream);

#endif
