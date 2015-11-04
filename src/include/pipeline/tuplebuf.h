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
#include "cont_execute.h"
#include "pipeline/cont_scheduler.h"
#include "pipeline/stream.h"
#include "storage/s_lock.h"
#include "storage/shmem.h"
#include "storage/latch.h"
#include "storage/lwlock.h"

/* GUC parameters */
extern int tuple_buffer_blocks;

typedef struct StreamTuple
{
	Oid db_oid;
	bytea *desc; /* tuple descriptor */
	HeapTuple heaptup;
	TimestampTz arrival_time; /* arrival time of the event */
	int num_acks; /* length of the acks array */
	InsertBatchAck *acks; /* the acks this tuple is responsible for */
	int num_record_descs; /* number of tuple descriptors for RECORD types */
	RecordTupleDesc *record_descs; /* RECORD type cached tuple descriptors */
	int64 group_hash; /* hash of this tuple's group columns */
} StreamTuple;

typedef struct TupleBufferSlot
{
	uint32_t magic;
	Oid db_oid;
	uint64_t id;
	sig_atomic_t unread;
	struct TupleBufferSlot *next;
	struct TupleBuffer *buf;
	Size size;
	StreamTuple *tuple;
	Bitmapset *queries;
} TupleBufferSlot;

typedef struct TupleBufferReader TupleBufferReader;

typedef struct TupleBuffer
{
	Size size;
	char *start;
	LWLock *head_lock;
	LWLock *tail_lock;
	TupleBufferSlot *head;
	TupleBufferSlot *tail;
	uint64_t head_id;
	uint64_t tail_id;
	slock_t mutex;
	Latch *writer_latch;
	Bitmapset *waiters;
	TupleBufferReader *readers[1]; /* readers[i] points to reader for continuous query proc with id i */
} TupleBuffer;


typedef bool (*TupleBufferShouldReadFunc) (TupleBufferReader *rdr, TupleBufferSlot *slot);

typedef bool (*TupleBufferShouldDrainFunc) (TupleBufferSlot *slot, void *ctx);

/* Pointer into a stream buffer from the perspective of a continuous query */
struct TupleBufferReader
{
	TupleBuffer *buf;
	ContQueryProc *proc;
	TupleBufferShouldReadFunc should_read_fn;
	uint64_t slot_id;
	TupleBufferSlot *slot;
	List *pinned;
	List *acks;
	MemoryContext context;
};

typedef struct TupleBufferBatchReader
{
	TupleBufferReader *rdr;
	Oid cq_id;
	TimestampTz start_time;
	bool started;
	bool depleted;
	bool batch_done;
	ListCell *current;
	ContQueryRunParams *params;
	Bitmapset *queries_seen;
	List *yielded;
} TupleBufferBatchReader;

extern TupleBuffer *WorkerTupleBuffer;
extern TupleBuffer *CombinerTupleBuffer;
extern TupleBuffer *AdhocTupleBuffer;

/* Handler for writing to the combiner's TupleBuffer  */
typedef TupleBufferSlot *(*CombinerWriteFunc) (StreamTuple *tuple, Bitmapset *queries);
extern CombinerWriteFunc CombinerWriteHook;

/* Shim for assigning any metadata to a TupleBufferSlot just before it is read */
typedef void (*PrepareTupleBufferSlotShimFunc) (TupleBufferSlot *slot);
extern PrepareTupleBufferSlotShimFunc PrepareTupleBufferSlotShim;

extern StreamTuple *MakeStreamTuple(HeapTuple heaptup, TupleDesc desc, int num_acks, InsertBatchAck *acks);

extern void TupleBuffersShmemInit(void);

extern TupleBuffer *TupleBufferInit(char *name, Size size, LWLock *head_lock, LWLock *tail_lock);
extern Size TupleBuffersShmemSize(void);
extern TupleBufferSlot *TupleBufferInsert(TupleBuffer *buf, StreamTuple *event, Bitmapset *queries);
extern bool TupleBufferIsEmpty(TupleBuffer *buf);

extern void TupleBufferInitLatch(TupleBuffer *buf, uint32_t cq_id, uint8_t reader_id, Latch *proclatch);
extern void TupleBufferTryWait(TupleBufferReader *reader);
extern void TupleBufferTryWaitTimeout(TupleBufferReader *reader, int timeout);
extern void TupleBufferNotifyAndClearWaiters(TupleBuffer *buf);

/* low level API for reading/writing to a TupleBuffer */
extern TupleBufferReader *TupleBufferOpenReader(TupleBuffer *buf, TupleBufferShouldReadFunc read_func, MemoryContext context);
extern void TupleBufferCloseReader(TupleBufferReader *reader);
extern TupleBufferSlot *TupleBufferPinNextSlot(TupleBufferReader *reader);
extern void TupleBufferUnpinSlot(TupleBufferSlot *slot);
extern void TupleBufferWaitOnSlot(TupleBuffer *buf, TupleBufferSlot *slot);
extern void TupleBufferUnpinAllPinnedSlots(TupleBufferReader *reader);
extern bool TupleBufferHasUnreadSlots(TupleBufferReader *reader);

/* high level API for reading/writing to a TupleBuffer */
extern TupleBufferBatchReader *TupleBufferOpenBatchReader(TupleBuffer *buf, TupleBufferShouldReadFunc read_func, MemoryContext context);
extern void TupleBufferCloseBatchReader(TupleBufferBatchReader *reader);
extern void TupleBufferBatchReaderSetCQId(TupleBufferBatchReader *reader, Oid cq_id);
extern bool TupleBufferBatchReaderHasTuplesForCQId(TupleBufferBatchReader *reader, Oid cq_id);
extern TupleBufferSlot *TupleBufferBatchReaderNext(TupleBufferBatchReader *reader);
extern void TupleBufferBatchReaderRewind(TupleBufferBatchReader *reader);
extern void TupleBufferBatchReaderReset(TupleBufferBatchReader *reader);
extern void TupleBufferBatchReaderTrySleep(TupleBufferBatchReader *reader, TimestampTz last_processed);
extern void TupleBufferBatchReaderTrySleepTimeout(TupleBufferBatchReader *reader, TimestampTz last_processed, int timeout);

extern void TupleBufferDrain(TupleBuffer *buf, Oid db_oid);
extern void TupleBufferDrainGeneric(TupleBuffer *buf, TupleBufferShouldDrainFunc fn, void *ctx);

#endif
