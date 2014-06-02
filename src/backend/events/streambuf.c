/*-------------------------------------------------------------------------
 *
 * stream.c
 *
 *	  Interface for interacting with the stream buffer
 *
 * src/backend/events/streambuf.c
 *
 *-------------------------------------------------------------------------
 */
#include "events/streambuf.h"

/* Global stream buffer that lives in shared memory. All stream events are appended to this */
StreamBuffer *GlobalStreamBuffer;

/* Maximum size in blocks of the global stream buffer */
long GlobalStreamBufferSize = 1000;

/*
 * AppendStreamEvent
 *
 * Appends a decoded event to the given stream buffer
 */
extern void
AppendStreamEvent(StreamBuffer *buf, HeapTuple event)
{
	// needs to wait for free space in shmem queue
	// - actual size of event since it needs to physically live in shmem

	// increase total memory consumed by buf

	// list of queryids needs to be in shared mem
	HeapTuple shared;
	StreamBufferSlot *sbs;

	shared = (HeapTuple) ShmemAlloc(HEAPTUPLESIZE + event->t_len);
	shared->t_len = event->t_len;
	shared->t_self = event->t_self;
	shared->t_tableOid = event->t_tableOid;
	shared->t_xc_node_id = event->t_xc_node_id;
	shared->t_data = (HeapTupleHeader) ((char *) shared + HEAPTUPLESIZE);
	memcpy((char *) shared->t_data, (char *) event->t_data, event->t_len);

	sbs = (StreamBufferSlot *) ShmemAlloc(sizeof(StreamBufferSlot));
	sbs->event = shared;
	SHMQueueElemInit(&(sbs->link));
	SHMQueueInsertBefore(&(GlobalStreamBuffer->buf), &(sbs->link));
}

/*
 * InitGlobalStreamBuffer
 *
 * Initialize global shared-memory buffer that all decoded events are appended to
 */
extern void InitGlobalStreamBuffer(void)
{
	// all slots on freelist

	bool found;
	GlobalStreamBuffer = ShmemInitStruct("GlobalStreamBuffer", GlobalStreamBufferSize, &found);
	if (!found)
		SHMQueueInit(&(GlobalStreamBuffer->buf));
}

/*
 * CreateStreamBuffer
 *
 * Creates a new stream buffer
 */
extern StreamBuffer *
CreateStreamBuffer(void)
{
	// create queue in shmem
	return NULL;
}

/*
 * OpenStreamBufferReader
 *
 * Opens a reader into the given stream buffer for a given continuous query
 */
extern StreamBufferReader *
OpenStreamBufferReader(StreamBuffer *buf, int queryid)
{
	return NULL;
}

/*
 * NextStreamEvent
 *
 * Returns the next event for the given reader, or NULL if there aren't any new events.
 * If this is the last reader that needs to see a given event, the event is deleted
 * from the stream buffer.
 */
extern HeapTuple
NextStreamEvent(StreamBufferReader *reader)
{
	// put deleted on freelist

	// decrement size consumed
	return NULL;
}
