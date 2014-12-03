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

extern HeapTuple ExecStreamProject(StreamEvent event, StreamScanState *node);
extern StreamScanState *ExecInitStreamScan(StreamScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecStreamScan(StreamScanState *node);
extern void ExecEndStreamScan(StreamScanState *node);
extern void ExecEndStreamScanBatch(StreamScanState *node);

#endif   /* NODESTREAMSCAN_H */
