/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 *	  Interface for interacting with the stream buffer
 *
 * src/include/events/streambuf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAMBUF_H
#define STREAMBUF_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "events/stream.h"
#include "nodes/bitmapset.h"
#include "storage/shmem.h"

/* Wraps a physical event and the queries that still need to read it */
typedef struct StreamBufferSlot
{
	SHM_QUEUE	link;
	/* decoded event */
	HeapTuple event;
	/*
	 * Each continuous query maps to exactly one bit in this bitmap.
	 * After a CQ has read the event, its bit is set to 1.
	 */
	// needed a shared memory implementation or fixed-size
//	Bitmapset *readby;
} StreamBufferSlot;

/* Circular buffer containing physical events to be read by continuous queries */
typedef struct StreamBuffer
{
	/* total capacity in bytes of this buffer */
	long capacity;
	/* queue of events, which are wrapped by StreamBufferSlots */
	SHM_QUEUE	buf;
	/* pointer to location in shared memory that should be consumed by the next append */
	char *pos;
	/* pointer to the beginning of the shared memory segment consumed by this buffer */
	char *start;
	/*
	 * Pointer to the next slot to be clobbered. Consider the following scenario to
	 * understand why this is useful to keep track of:
	 *
	 * There are two 100-byte slots at the head of a full buffer.
	 *
	 * An incoming 101-byte event arrives. We must clobber entire slots at a time,
	 * because we need to read them first to verify that they've been read by
	 * all relevant CQs, so this 101-byte event will need to clobber both 100-byte
	 * slots to fit, but it will only consume 101 bytes of the buffer
	 *
	 * If another event arrives after that, it's safe to start writing it directly
	 * after the 101st byte, but we need to know where the next event starts so that
	 * it can properly be read before being clobbered.
	 */
	StreamBufferSlot *nextvictim;
} StreamBuffer;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct StreamBufferReader
{
	int queryid;
	StreamBuffer *buf;
} StreamBufferReader;

extern StreamBuffer *GlobalStreamBuffer;

extern void AppendStreamEvent(StreamBuffer *buf, HeapTuple event);
extern void InitGlobalStreamBuffer(void);

extern StreamBufferReader *OpenStreamBufferReader(StreamBuffer *buf, int queryid);
extern HeapTuple NextStreamEvent(StreamBufferReader *reader);

#endif
