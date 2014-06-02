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
#include "events/stream.h"
#include "storage/shmem.h"


/* Buffer containing physical events to be read by continuous queries */
typedef struct StreamBuffer
{
	long size;
	SHM_QUEUE	buf;
} StreamBuffer;

/* Wraps a physical event and the queries that still need to read it */
typedef struct StreamBufferSlot
{
	SHM_QUEUE	link;
	/* decoded event */
	HeapTuple event;
	/* list of 16-bit query ids that still need to see this event */
//	List *queryids;
	// should probably be a bitmapset for which every query maps to precisely one bit
} StreamBufferSlot;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct StreamBufferReader
{
	int queryid;
	StreamBuffer *buf;
} StreamBufferReader;

extern StreamBuffer *GlobalStreamBuffer;

extern void AppendStreamEvent(StreamBuffer *buf, HeapTuple event);
extern void InitGlobalStreamBuffer(void);

extern StreamBuffer *CreateStreamBuffer(void);
extern StreamBufferReader *OpenStreamBufferReader(StreamBuffer *buf, int queryid);
extern HeapTuple NextStreamEvent(StreamBufferReader *reader);

#endif
