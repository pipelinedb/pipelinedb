/*-------------------------------------------------------------------------
 *
 * nodeStreamTablescan.h
 *
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeStreamTablescan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESTREAMTABLEJOINSCAN_H
#define NODESTREAMTABLEJOINSCAN_H

#include "nodes/execnodes.h"

extern StreamTableScanState *ExecInitStreamTableScan(StreamTableScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecStreamTableScan(StreamTableScanState *node);
extern void ExecEndStreamTableScan(StreamTableScanState *node);
extern void ExecStreamTableMarkPos(StreamTableScanState *node);
extern void ExecStreamTableRestrPos(StreamTableScanState *node);
extern void ExecReScanStreamTableScan(StreamTableScanState *node);
extern void ExecBeginBatchStreamTableScan(StreamTableScanState *node);


#endif   /* NODESTREAMTABLEJOINSCAN_H */
