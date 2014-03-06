/*-------------------------------------------------------------------------
 *
 * nodeTuplestoreScan.h
 *
 * src/include/executor/nodeTuplestoreScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQSCAN_H
#define NODESEQSCAN_H

#include "nodes/execnodes.h"

extern SeqScanState *ExecInitTuplestoreScan(TuplestoreScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecTuplestoreScan(TuplestoreScan *node);
extern void ExecEndTuplestoreScan(TuplestoreScan *node);

#endif   /* NODESEQSCAN_H */
