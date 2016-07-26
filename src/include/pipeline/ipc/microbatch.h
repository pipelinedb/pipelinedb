/*-------------------------------------------------------------------------
 *
 * microbatch.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_MICROBATCH_H
#define IPC_MICROBATCH_H

#include "postgres.h"

/* guc */
extern int continuous_query_num_batch;
extern int continuous_query_batch_size;

typedef enum microbatch_type_t
{
	Worker = 1,
	Combiner
} microbatch_type_t;

typedef struct microbatch_t
{
	microbatch_type_t type;
	int num_tups;
	int size;
	Bitmapset *record_descs;
	Bitmapset *queries;
	List *acks;
	List *tups;
} microbatch_t;

extern void microbatch_init(microbatch_type_t type, Bitmapset *queries, );
extern void microbatch_destroy(void);

extern bool microbatch_push(HeapTuple tup);


#endif
