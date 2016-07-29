/*-------------------------------------------------------------------------
 *
 * reader.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef READER_H
#define READER_H

#include "postgres.h"

#include "pipeline/ipc/pzmq.h"

typedef struct ipc_tuple
{
	TupleDesc desc;
	List *record_descs;
	HeapTuple tup;
	uint64 hash;
} ipc_tuple;

typedef struct ipc_tuple_reader_batch
{
	Bitmapset *queries;
	List *acks;
	int ntups;
	Size nbytes;
} ipc_tuple_reader_batch;

extern void ipc_tuple_reader_init(uint64 id);
extern void ipc_tuple_reader_destroy(void);

#define ipc_tuple_reader_poll(timeout) (pzmq_poll(timeout))
extern ipc_tuple_reader_batch *ipc_tuple_reader_pull(void);
extern void ipc_tuple_reader_reset(void);
extern void ipc_tuple_reader_ack(void);

extern ipc_tuple *ipc_tuple_reader_next(Oid query_id);
extern void ipc_tuple_reader_rewind(void);

#endif
