/*-------------------------------------------------------------------------
 *
 * orderedsetaggs.h
 *	  Interface for ordered and hypothetical set aggregate operations
 *
 * src/include/utils/orderedsetaggs.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ORDEREDSETAGGS_H
#define ORDEREDSETAGGS_H

#include "fmgr.h"

typedef struct CQHSPerQueryState
{
	/* slot for inserting elements into the HLL */
	TupleTableSlot *curslot;
	/* slot containing the tuple representing the direct args to this HS agg */
	TupleTableSlot *directslot;
	SortSupport sort;
	/* expression type of HLL input element, used to choose hash function */
	Oid exprtype;
} CQHSPerQueryState;

int CompareSlots(TupleTableSlot *s0, TupleTableSlot *s1, SortSupport sort);
CQHSPerQueryState *CQHSPerQueryStartup(PG_FUNCTION_ARGS);

#endif
