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

#endif   /* NODESTREAMSCAN_H */
