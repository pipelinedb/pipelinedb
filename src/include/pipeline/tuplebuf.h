/*-------------------------------------------------------------------------
 *
 * tuplebuf.h
 *
 *	  Interface for interacting with tuple buffers
 *
 * src/include/pipeline/tuplebuf.h
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
#include "storage/lwlock.h"

extern bool DebugPrintTupleBuffer;
extern int TupleBufferBlocks;
extern int EmptyTupleBufferWaitTime;

typedef struct Tuple
{
	/* append-time values */
	HeapTuple heaptup;
	/* arrival time of the event */
	TimestampTz arrivaltime;
	/* descriptor for this event and possibly some that follow it */
	TupleDesc desc;
} Tuple;

/* Wraps a physical event and the queries that still need to read it */
typedef struct TupleBufferSlot
{
	int32_t magic;
	struct TupleBuffer *buf;
	Size size;
	Tuple *tuple;
	Bitmapset *readby;
	char *stream;
	slock_t mutex;
} TupleBufferSlot;

/* Circular buffer containing physical events to be read by continuous queries */
typedef struct TupleBuffer
{
	char *name;
	LWLock *head_lock;
	LWLock *tail_lock;
	int8_t max_readers;
	Size size;
	char *start;
	TupleBufferSlot *head;
	TupleBufferSlot *tail;
	int64_t nonce;
	slock_t mutex;
	Bitmapset *waiters;
	Latch **latches;
} TupleBuffer;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct TupleBufferReader
{
	TupleBuffer *buf;
	int32_t cq_id;
	int8_t reader_id;
	int8_t num_readers;
	int64_t nonce;
	bool retry_slot;
	TupleBufferSlot *slot;
} TupleBufferReader;

extern TupleBuffer *WorkerTupleBuffer;
extern TupleBuffer *CombinerTupleBuffer;

extern Tuple *MakeTuple(HeapTuple heaptup, TupleDesc desc);

extern void TupleBuffersInit(void);

extern TupleBuffer *TupleBufferInit(char *name, Size size, LWLock *head_lock, LWLock *tail_lock, int8_t max_readers);
extern Size TupleBufferShmemSize(void);
extern TupleBufferSlot *TupleBufferInsert(TupleBuffer *buf, const char *stream, Tuple *event);
extern bool TupleBufferIsEmpty(TupleBuffer *buf);
extern void TupleBufferWait(TupleBuffer *buf, int32_t cq_id, int8_t reader_id);
extern void TupleBufferNotifyAndClearWaiters(TupleBuffer *buf);
extern void TupleBufferResetNotify(TupleBuffer *buf, int32_t cq_id, int8_t reader_id);
extern void TupleBufferNotify(TupleBuffer *buf, int32_t cq_id);

extern TupleBufferReader *TupleBufferOpenReader(TupleBuffer *buf, int32_t cq_id, int8_t reader_id, int8_t num_readers);
extern void TupleBufferCloseReader(TupleBufferReader *reader);
extern TupleBufferSlot *TupleBufferPinNextSlot(TupleBufferReader *reader);
extern void TupleBufferUnpinSlot(TupleBufferReader *reader, TupleBufferSlot *slot);
extern void TupleBufferWaitOnSlot(TupleBufferSlot *slot, int sleepms);

extern void TupleBufferUnpinAllPinnedSlots(void);
extern void TupleBufferClearPinnedSlots(void);

#endif
