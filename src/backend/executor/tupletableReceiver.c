/*-------------------------------------------------------------------------
 *
 * tupletableReceiver.c
 *	  An implementation of DestReceiver that stores the result HeapTuples in
 *	  a TupleHashTable.
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * IDENTIFICATION
 *	  src/backend/executor/tupletableReceiver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "nodes/execnodes.h"
#include "executor/executor.h"

#include "access/tuptoaster.h"
#include "executor/tupletableReceiver.h"



typedef struct
{
	DestReceiver pub;
	/* parameters: */
	TupleHashTable htab;	/* where to store the HeapTuples */
	MemoryContext cxt;			/* context containing htab */
	bool		detoast;		/* were we told to detoast? */
	/* workspace: */
	Datum	   *outvalues;		/* values array for result tuple */
	Datum	   *tofree;			/* temp values to be pfree'd */
} TupleTableReceiverState;


static void tupletable_receive_notoast(TupleTableSlot *slot, DestReceiver *self);
static void tupletable_receive_detoast(TupleTableSlot *slot, DestReceiver *self);


/*
 * Prepare to receive tuples from executor.
 */
static void
tupletable_startup_receiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	TupleTableReceiverState *myState = (TupleTableReceiverState *) self;
	bool		needtoast = false;
	Form_pg_attribute *attrs = typeinfo->attrs;
	int			natts = typeinfo->natts;
	int			i;

	/* Check if any columns require detoast work */
	if (myState->detoast)
	{
		for (i = 0; i < natts; i++)
		{
			if (attrs[i]->attisdropped)
				continue;
			if (attrs[i]->attlen == -1)
			{
				needtoast = true;
				break;
			}
		}
	}

	/* Set up appropriate callback */
	if (needtoast)
	{
		myState->pub.receiveSlot = tupletable_receive_detoast;
		/* Create workspace */
		myState->outvalues = (Datum *)
			MemoryContextAlloc(myState->cxt, natts * sizeof(Datum));
		myState->tofree = (Datum *)
			MemoryContextAlloc(myState->cxt, natts * sizeof(Datum));
	}
	else
	{
		myState->pub.receiveSlot = tupletable_receive_notoast;
		myState->outvalues = NULL;
		myState->tofree = NULL;
	}
}

static void
put_heap_tuple(TupleTableSlot *slot, TupleTableReceiverState *self)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(self->cxt);
	bool isnew;

	HeapTupleEntry entry = (HeapTupleEntry) LookupTupleHashEntry(self->htab, slot, &isnew);
	entry->tuple = ExecCopySlotTuple(slot);

	MemoryContextSwitchTo(oldcxt);
}


/*
 * Receive a tuple from the executor and store it in the tuplestore.
 * This is for the easy case where we don't have to detoast.
 */
static void
tupletable_receive_notoast(TupleTableSlot *slot, DestReceiver *self)
{
	TupleTableReceiverState *myState = (TupleTableReceiverState *) self;

	put_heap_tuple(slot, myState);
}

/*
 * Receive a tuple from the executor and store it in the tuplestore.
 * This is for the case where we have to detoast any toasted values.
 */
static void
tupletable_receive_detoast(TupleTableSlot *slot, DestReceiver *self)
{
	TupleTableReceiverState *myState = (TupleTableReceiverState *) self;
	TupleDesc	typeinfo = slot->tts_tupleDescriptor;
	Form_pg_attribute *attrs = typeinfo->attrs;
	int			natts = typeinfo->natts;
	int			nfree;
	int			i;

	/* Make sure the tuple is fully deconstructed */
	slot_getallattrs(slot);

	/*
	 * Fetch back any out-of-line datums.  We build the new datums array in
	 * myState->outvalues[] (but we can re-use the slot's isnull array). Also,
	 * remember the fetched values to free afterwards.
	 */
	nfree = 0;
	for (i = 0; i < natts; i++)
	{
		Datum		val = slot->tts_values[i];

		if (!attrs[i]->attisdropped &&
			attrs[i]->attlen == -1 &&
			!slot->tts_isnull[i])
		{
			if (VARATT_IS_EXTERNAL(DatumGetPointer(val)))
			{
				val = PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)
													  DatumGetPointer(val)));
				myState->tofree[nfree++] = val;
				slot->tts_values[i] = val;
			}
		}

		myState->outvalues[i] = val;
	}

	/*
	 * Store the physical tuple in a TupleHashTable
	 */

	put_heap_tuple(slot, myState);

	/* And release any temporary detoasted values */
	for (i = 0; i < nfree; i++)
		pfree(DatumGetPointer(myState->tofree[i]));
}

/*
 * Clean up at end of an executor run
 */
static void
tupletable_shutdown_receiver(DestReceiver *self)
{
	TupleTableReceiverState *myState = (TupleTableReceiverState *) self;

	/* Release workspace if any */
	if (myState->outvalues)
		pfree(myState->outvalues);
	myState->outvalues = NULL;
	if (myState->tofree)
		pfree(myState->tofree);
	myState->tofree = NULL;
}

/*
 * Destroy receiver when done with it
 */
static void
tupletable_destroy_receiver(DestReceiver *self)
{
	pfree(self);
}

/*
 * Initially create a DestReceiver object.
 */
DestReceiver *
CreateTupleTableDestReceiver(void)
{
	TupleTableReceiverState *self = (TupleTableReceiverState *) palloc0(sizeof(TupleTableReceiverState));

	self->pub.receiveSlot = tupletable_receive_notoast;	/* might change */
	self->pub.rStartup = tupletable_startup_receiver;
	self->pub.rShutdown = tupletable_shutdown_receiver;
	self->pub.rDestroy = tupletable_destroy_receiver;
	self->pub.mydest = DestTuplestore;

	return (DestReceiver *) self;
}

/*
 * Set parameters for a TupleTableDestReceiver
 */
void
SetTupleTableDestReceiverParams(DestReceiver *self, TupleHashTable htab,
								MemoryContext tContext, bool detoast)
{
	TupleTableReceiverState *myState = (TupleTableReceiverState *) self;

	Assert(myState->pub.mydest == DestTuplestore);
	myState->htab = htab;
	myState->cxt = tContext;
	myState->detoast = detoast;
}
