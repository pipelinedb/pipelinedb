/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.h
 *
 * Support for scanning the stream buffer
 *
 * src/include/executor/nodeStreamscan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESTREAMSCAN_H
#define NODESTREAMSCAN_H

#include "nodes/execnodes.h"

extern StreamScanState *ExecInitStreamScan(StreamScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecStreamScan(StreamScanState *node);
extern void ExecEndStreamScan(StreamScanState *node);
extern void CloseHeapScan(StreamTableScanState *node);
extern void ExecReScanStreamScan(StreamScanState *node);
extern bool IsScanningJoinCache();
extern void ClearStreamJoinCache();

#endif   /* NODESTREAMSCAN_H */
