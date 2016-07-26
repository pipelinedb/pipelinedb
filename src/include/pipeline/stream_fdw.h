/*-------------------------------------------------------------------------
 *
 * stream_fdw.h
 *		 FDW for PipelineDB streams
 *
 * Portions Copyright (c) 2015, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAM_FDW_H
#define STREAM_FDW_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "pipeline/cont_adhoc.h"
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/rel.h"

#define REENTRANT_STREAM_INSERT 0x1

typedef struct StreamProjectionInfo StreamProjectionInfo;

typedef struct StreamScanState
{
	ContExecutor *cont_executor;
	AdhocExecutor *adhoc_executor;
	StreamProjectionInfo *pi;
	Size nbytes;
	int ntuples;
} StreamScanState;

typedef struct StreamInsertState
{
	Bitmapset *targets;

	long count;
	long bytes;
	int num_batches;

	InsertBatch *batch;
	InsertBatchAck *ack;

	TupleDesc desc;
	bytea *packed_desc;

	ipc_queue *worker_queue;
	AdhocInsertState *adhoc_state;

	int flags;
} StreamInsertState;

extern Datum stream_fdw_handler(PG_FUNCTION_ARGS);

extern void GetStreamSize(PlannerInfo *root, RelOptInfo *baserel, Oid streamid);
extern void GetStreamPaths(PlannerInfo *root, RelOptInfo *baserel, Oid streamid);
extern ForeignScan *GetStreamScanPlan(PlannerInfo *root, RelOptInfo *baserel,
		Oid streamid, ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan);
extern void BeginStreamScan(ForeignScanState *node, int eflags);
extern List *PlanStreamModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
extern TupleTableSlot *IterateStreamScan(ForeignScanState *node);
extern void ReScanStreamScan(ForeignScanState *node);
extern void EndStreamScan(ForeignScanState *node);

extern void BeginStreamModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
						   List *fdw_private, int subplan_index, int eflags);
extern TupleTableSlot *ExecStreamInsert(EState *estate, ResultRelInfo *resultRelInfo,
						  TupleTableSlot *slot, TupleTableSlot *planSlot);
extern void EndStreamModify(EState *estate, ResultRelInfo *resultRelInfo);

#endif
