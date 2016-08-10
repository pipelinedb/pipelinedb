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

#include "pipeline/cont_execute.h"
#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

typedef enum
{
	STREAM_INSERT_ASYNCHRONOUS,
	STREAM_INSERT_SYNCHRONOUS_RECEIVE,
	STREAM_INSERT_SYNCHRONOUS_COMMIT,
	STREAM_INSERT_FLUSH /* internal */
} StreamInsertLevel;

/*
 * Hooks that can feed raw data to COPY when we need more flexibility than a simple file descriptor.
 */
extern int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread);
extern void *copy_iter_arg;

#define QueryIsStreaming(query) ((query)->isContinuous)
#define QueryIsCombine(query) ((query)->isCombine)
#define PlanIsStreaming(stmt) ((stmt)->isContinuous)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* Whether or not to wait on the inserted event to be consumed by the CV */
extern int stream_insert_level;
extern char *stream_targets;

extern void CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples);

extern Datum pipeline_stream_insert(PG_FUNCTION_ARGS);

#endif
