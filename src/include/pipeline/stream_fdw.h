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
#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/rel.h"

extern Datum stream_fdw_handler(PG_FUNCTION_ARGS);

extern void GetStreamSize(PlannerInfo *root, RelOptInfo *baserel, Oid streamid);
extern void GetStreamPaths(PlannerInfo *root, RelOptInfo *baserel, Oid streamid);
extern ForeignScan *GetStreamScanPlan(PlannerInfo *root, RelOptInfo *baserel,
		Oid streamid, ForeignPath *best_path, List *tlist, List *scan_clauses);
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
