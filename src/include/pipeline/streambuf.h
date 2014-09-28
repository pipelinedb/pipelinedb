/*-------------------------------------------------------------------------
 *
 * stream.h
 *
 *	  Interface for interacting with the stream buffer
 *
 * src/include/pipeline/streambuf.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STREAMBUF_H
#define STREAMBUF_H

#include "postgres.h"
#include "nodes/bitmapset.h"
#include "pipeline/stream.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"

#define BufferEnd(buf) ((buf)->start + (buf)->capacity)
#define BufferOffset(buf, ptr) ((int) ((char *) (ptr) - (buf)->start))
#define SlotSize(slot) ((slot)->len)
#define SlotEnd(slot) ((char *) (slot) + SlotSize(slot))

extern bool DebugPrintStreamBuffer;

extern int StreamBufferBlocks;

/* Wraps a physical event and the queries that still need to read it */
typedef struct StreamBufferSlot
{
	SHM_QUEUE	link;
	/* decoded event */
	StreamEvent event;
	/*
	 * Each continuous query maps to exactly one bit in this bitmap.
	 * After a CQ has read the event, its bit is set to 1.
	 */
	Bitmapset *readby;
	char *stream;
	char *encoding;
	int len;
	int nextoffset;
	slock_t mutex;
} StreamBufferSlot;

/* Circular buffer containing physical events to be read by continuous queries */
typedef struct StreamBuffer
{
	/* total capacity in bytes of this buffer */
	long capacity;
	/* queue of events, which are wrapped by StreamBufferSlots */
	SHM_QUEUE	buf;
	/* pointer to location in shared memory that should be consumed by the next append */
	char **pos;
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
	/* mapping from streams to the continuous views that read from them */
//	StreamTargets *targets;
	StreamBufferSlot **prev;
	StreamBufferSlot **tail;
	char **last;
	int writers;
	slock_t mutex;
} StreamBuffer;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct StreamBufferReader
{
	int queryid;
	char *pos;
	StreamBuffer *buf;
	bool reading;
} StreamBufferReader;

extern StreamBuffer *GlobalStreamBuffer;

extern StreamBufferSlot *AppendStreamEvent(const char *stream, const char *encoding, StreamBuffer *buf, StreamEvent event);
extern Size StreamBufferShmemSize(void);
extern void InitGlobalStreamBuffer(void);
extern bool IsInputStream(const char *stream);

extern StreamBufferReader *OpenStreamBufferReader(StreamBuffer *buf, int queryid);
extern void CloseStreamBufferReader(StreamBufferReader *reader);
extern StreamBufferSlot *PinNextStreamEvent(StreamBufferReader *reader);
extern void UnpinStreamEvent(StreamBufferReader *reader, StreamBufferSlot *slot);
extern void ReadAndPrintStreamBuffer(StreamBuffer *buf, int32 queryid, int intervalms);
extern void PrintStreamBuffer(StreamBuffer *buf);

#endif
