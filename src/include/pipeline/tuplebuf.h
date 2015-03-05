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
	bytea *desc;
	/* append-time values */
	HeapTuple heaptup;
	/* arrival time of the event */
	TimestampTz arrivaltime;
} Tuple;

/* Wraps a physical event and the queries that still need to read it */
typedef struct TupleBufferSlot
{
	uint32_t magic;
	uint64_t id;
	struct TupleBufferSlot *next;
	struct TupleBuffer *buf;
	Size size;
	Tuple *tuple;
	Bitmapset *readby;
	slock_t mutex;
} TupleBufferSlot;

/* Circular buffer containing physical events to be read by continuous queries */
typedef struct TupleBuffer
{
	char *name;
	LWLock *head_lock;
	LWLock *tail_lock;
	uint8_t max_readers;
	Size size;
	char *start;
	TupleBufferSlot *head;
	TupleBufferSlot *tail;
	uint64_t head_id;
	uint64_t tail_id;
	slock_t mutex;
	uint16_t max_cqs;
	Bitmapset *waiters;
	Latch **latches;
	Latch writer_latch;
} TupleBuffer;

/* Pointer into a stream buffer from the perspective of a continuous query */
typedef struct TupleBufferReader
{
	TupleBuffer *buf;
	uint32_t cq_id;
	uint8_t reader_id;
	uint8_t num_readers;
	uint64_t slot_id;
	TupleBufferSlot *slot;
} TupleBufferReader;

extern TupleBuffer *WorkerTupleBuffer;
extern TupleBuffer *CombinerTupleBuffer;

extern Tuple *MakeTuple(HeapTuple heaptup, TupleDesc desc);

extern void TupleBuffersInit(void);

extern TupleBuffer *TupleBufferInit(char *name, Size size, LWLock *head_lock, LWLock *tail_lock, uint8_t max_readers);
extern Size TupleBuffersShmemSize(void);
extern TupleBufferSlot *TupleBufferInsert(TupleBuffer *buf, Tuple *event, Bitmapset *readers);
extern bool TupleBufferIsEmpty(TupleBuffer *buf);

extern void TupleBufferInitLatch(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, Latch *proclatch);
extern void TupleBufferWait(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id);
extern void TupleBufferNotifyAndClearWaiters(TupleBuffer *buf);
extern void TupleBufferResetNotify(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id);
extern void TupleBufferExpandLatchArray(TupleBuffer *buf, uint32_t cq_id);
extern void TupleBufferNotify(TupleBuffer *buf, uint32_t cq_id);

extern TupleBufferReader *TupleBufferOpenReader(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, uint8_t num_readers);
extern void TupleBufferCloseReader(TupleBufferReader *reader);
extern TupleBufferSlot *TupleBufferPinNextSlot(TupleBufferReader *reader);
extern void TupleBufferUnpinSlot(TupleBufferReader *reader, TupleBufferSlot *slot);
extern void TupleBufferWaitOnSlot(TupleBufferSlot *slot, int sleepms);
extern void TupleBufferDrain(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, uint8_t num_readers);

extern void TupleBufferUnpinAllPinnedSlots(void);
extern void TupleBufferClearPinnedSlots(void);
extern bool TupleBufferHasUnreadSlots(void);
extern void TupleBufferClearReaders(void);

extern TupleDesc TupleHeaderUnpack(char *raw);
extern char *TupleHeaderPack(TupleDesc desc);

#endif
