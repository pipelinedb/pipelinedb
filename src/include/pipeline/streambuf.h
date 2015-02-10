/* Copyright (c) 2013-2015 PipelineDB */
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
#include "storage/latch.h"

extern bool DebugPrintStreamBuffer;

extern int StreamBufferBlocks;

int EmptyStreamBufferWaitTime;

/* Wraps a physical event and the queries that still need to read it */
typedef struct StreamBufferSlot
{
	int32_t magic;
	Size size;
	StreamEvent *event;
	Bitmapset *readby;
	char *stream;
	slock_t mutex;
} StreamBufferSlot;

/* Circular buffer containing physical events to be read by continuous queries */
typedef struct StreamBuffer
{
	char *start;
	Size size;
	StreamBufferSlot *head;
	StreamBufferSlot *tail;
	int64_t nonce;
	slock_t mutex;
	Bitmapset *waiters;
	Latch **latches;
} StreamBuffer;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct StreamBufferReader
{
	int32_t cq_id;
	int8_t worker_id;
	int8_t num_workers;
	int64_t nonce;
	bool retry_slot;
	StreamBufferSlot *slot;
} StreamBufferReader;

extern StreamBuffer *GlobalStreamBuffer;

extern void StreamBufferInit(void);
extern Size StreamBufferShmemSize(void);
extern StreamBufferSlot *StreamBufferInsert(const char *stream, StreamEvent *event);
extern bool StreamBufferIsEmpty(void);
extern void StreamBufferWait(int32_t cq_id, int8_t worker_id);
extern void StreamBufferNotifyAndClearWaiters(void);
extern void StreamBufferResetNotify(int32_t cq_id, int8_t worker_id);
extern void StreamBufferNotify(int32_t cq_id);

extern StreamBufferReader *StreamBufferOpenReader(int32_t cq_id, int8_t worker_id);
extern void StreamBufferCloseReader(StreamBufferReader *reader);
extern StreamBufferSlot *StreamBufferPinNextSlot(StreamBufferReader *reader);
extern void StreamBufferUnpinSlot(StreamBufferReader *reader, StreamBufferSlot *slot);
extern void StreamBufferWaitOnSlot(StreamBufferSlot *slot, int sleepms);

#endif
