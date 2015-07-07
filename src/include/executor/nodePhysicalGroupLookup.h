/*
 * nodePhysicalGroupLookup.h
 *
 *	Interface for nodePhysicalGroupLookup.c
 *
 *	src/include/executor/nodePhysicalGroupLookup.h
 */
#ifndef NODEPHYSICALGROUPLOOKUP_H
#define NODEPHYSICALGROUPLOOKUP_H

#include "nodes/execnodes.h"

extern TupleTableSlot *ExecPhysicalGroupLookup(PhysicalGroupLookupState *node);
extern PhysicalGroupLookupState *ExecInitPhysicalGroupLookup(PhysicalGroupLookup *node, EState *estate, int eflags);
extern void ExecEndPhysicalGroupLookup(PhysicalGroupLookupState *node);

#endif /* NODEPHYSICALGROUPLOOKUP_H */
