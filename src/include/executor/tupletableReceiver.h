/*-------------------------------------------------------------------------
 *
 * tupletableReceiver.h
 *	  An implementation of DestReceiver that stores the result HeapTuples in
 *	  a TupleHashTable.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/include/executor/tupletableReceiver.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef TUPLETABLE_RECEIVER_H
#define TUPLETABLE_RECEIVER_H

#include "tcop/dest.h"
#include "nodes/execnodes.h"

typedef struct HeapTupleEntryData *HeapTupleEntry;

typedef struct HeapTupleEntryData
{
	TupleHashEntryData shared;	/* common header for hash table entries */
	HeapTuple tuple;	/* physical tuple belonging to this entry */
	char flags;
}	HeapTupleEntryData;

extern DestReceiver *CreateTupleTableDestReceiver(void);
extern void SetTupleTableDestReceiverParams(DestReceiver *self, TupleHashTable htab,
		MemoryContext tContext, bool detoast);

#endif   /* TUPLETABLE_RECEIVER_H */
