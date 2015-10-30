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

#include "nodes/bitmapset.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "storage/spin.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"
#include "utils/timestamp.h"

/*
 * Hooks that can feed raw data to COPY when we need more flexibility than a simple file descriptor.
 */
extern int (*copy_iter_hook) (void *arg, void *buf, int minread, int maxread);
extern void *copy_iter_arg;

#define QueryIsStreaming(query) ((query)->isContinuous)
#define QueryIsCombine(query) ((query)->isCombine)
#define PlanIsStreaming(stmt) ((stmt)->is_continuous)
#define ARRIVAL_TIMESTAMP "arrival_timestamp"

/* Whether or not to wait on the inserted event to be consumed by the CV*/
extern bool synchronous_stream_insert;
extern char *stream_targets;

extern uint64 CopyIntoStream(Relation stream, TupleDesc desc, HeapTuple *tuples, int ntuples);

typedef struct AdhocQuery
{
	int cq_id;
	int *active_flag;
	int count; 
} AdhocQuery;

typedef struct AdhocData
{
	int num_adhoc;
	AdhocQuery *queries;

} AdhocData;

extern Bitmapset *GetStreamReaders(Oid relid);

extern int SendTupleToAdhoc(AdhocData *data,
							HeapTuple tup,
							TupleDesc desc,
							Size *bytes);

#endif
