/*-------------------------------------------------------------------------
 *
 * nodeTuplestoreScan.h
 *		Support routines for sequential scans of tuple stores.
 *
 * src/include/executor/nodeTuplestoreScan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODETUPLESTORESCAN_H
#define NODETUPLESTORESCAN_H

#include "nodes/execnodes.h"

extern TuplestoreScanState *ExecInitTuplestoreScan(TuplestoreScan *node, EState *estate, int eflags);
extern TupleTableSlot *ExecTuplestoreScan(TuplestoreScanState *node);
extern void ExecEndTuplestoreScan(TuplestoreScanState *node);

#endif   /* NODETUPLESTORESCAN_HH */
