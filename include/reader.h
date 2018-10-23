/*-------------------------------------------------------------------------
 *
 * reader.h
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef READER_H
#define READER_H

#include "postgres.h"

#include "access/htup.h"
#include "access/tupdesc.h"
#include "nodes/bitmapset.h"

#include "pzmq.h"

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
	bool has_acks;
	List *sync_acks; /* only valid at when the iteration is complete */
	List *flush_acks;
	int ntups;
	Size nbytes;
} ipc_tuple_reader_batch;

extern void ipc_tuple_reader_init(void);
extern void ipc_tuple_reader_destroy(void);

#define ipc_tuple_reader_poll(timeout) (pzmq_poll(timeout))
extern ipc_tuple_reader_batch *ipc_tuple_reader_pull(void);
extern void ipc_tuple_reader_reset(void);
extern void ipc_tuple_reader_ack(void);

extern ipc_tuple *ipc_tuple_reader_next(Oid query_id);
extern void ipc_tuple_reader_rewind(void);

#endif
