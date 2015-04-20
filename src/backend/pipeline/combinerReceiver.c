/*-------------------------------------------------------------------------
 *
 * combinerReceiver.c
 *	  An implementation of DestReceiver that that allows combiners to receive
 *	  tuples from worker processes.
 *
 * IDENTIFICATION
 *	  src/backend/pipeline/combinerReceiver.c
 *
 */
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "postgres.h"
#include "pgstat.h"

#include "access/printtup.h"
#include "pipeline/combinerReceiver.h"
#include "pipeline/tuplebuf.h"
#include "miscadmin.h"
#include "utils/memutils.h"

typedef struct
{
	DestReceiver pub;
	Bitmapset *readers;
} CombinerState;

static void
combiner_shutdown(DestReceiver *self)
{

}

static void
combiner_startup(DestReceiver *self, int operation,
		TupleDesc typeinfo)
{

}

static void
combiner_receive(TupleTableSlot *slot, DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	MemoryContext old = MemoryContextSwitchTo(CQExecutionContext);
	TupleBufferSlot *tbs;
	Tuple *tup;
	StreamBatch *batches = NULL;

	if (sync_stream_insert)
	{
		ListCell *lc;
		int i = 0;

		batches = (StreamBatch *) palloc(sizeof(StreamBatch) * list_length(MyBatches));

		foreach(lc, MyBatches)
		{
			StreamBatch *batch = lfirst(lc);

			StreamBatchEntryIncrementTotalCAcks(batch);

			batches[i].id = batch->id;
			batches[i].count = 1;
			i++;
		}
	}

	tup = MakeTuple(ExecMaterializeSlot(slot), NULL, list_length(MyBatches), batches);
	tbs = TupleBufferInsert(CombinerTupleBuffer, tup, c->readers);
	IncrementCQWrite(1, tbs->size);

	pfree(batches);

	MemoryContextSwitchTo(old);
}

static void combiner_destroy(DestReceiver *self)
{
	CombinerState *c = (CombinerState *) self;
	bms_free(c->readers);
	pfree(c);
}

DestReceiver *
CreateCombinerDestReceiver(void)
{
	CombinerState *self = (CombinerState *) palloc0(sizeof(CombinerState));

	self->pub.receiveSlot = combiner_receive; /* might get changed later */
	self->pub.rStartup = combiner_startup;
	self->pub.rShutdown = combiner_shutdown;
	self->pub.rDestroy = combiner_destroy;
	self->pub.mydest = DestCombiner;

	return (DestReceiver *) self;
}

/*
 * SetCombinerDestReceiverParams
 *
 * Set parameters for a CombinerDestReceiver
 */
void
SetCombinerDestReceiverParams(DestReceiver *self, int32_t cq_id)
{
	CombinerState *c = (CombinerState *) self;
	c->readers = bms_add_member(c->readers, cq_id);
}
