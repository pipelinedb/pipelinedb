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
#include "nodes/parsenodes.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

#define EventStreamNeedsOpen(stream) (stream->state != STREAM_STATE_OPEN)
#define QueryIsStreaming(query) ((query)->is_continuous)
#define QueryIsCombine(query) ((query)->is_combine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous || false)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool DebugSyncStreamInsert;

extern bool InsertTargetIsStream(InsertStmt *ins);
extern int InsertIntoStream(InsertStmt *ins);
extern bool IsInputStream(const char *stream);

#endif
