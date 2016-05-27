/*-------------------------------------------------------------------------
 *
 * nodeContinuousUnique.h
 *
 * src/include/executor/nodeCQUnique.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODECONTINUOUSUNIQUE_H
#define NODECONTINUOUSUNIQUE_H

#include "nodes/execnodes.h"

extern ContinuousUniqueState *ExecInitContinuousUnique(ContinuousUnique *node, EState *estate, int eflags);
extern TupleTableSlot *ExecContinuousUnique(ContinuousUniqueState *node);
extern void ExecEndContinuousUnique(ContinuousUniqueState *node);
extern void ExecReScanContinuousUnique(ContinuousUniqueState *node);

#endif   /* NODECONTINUOUSUNIQUE_H */
