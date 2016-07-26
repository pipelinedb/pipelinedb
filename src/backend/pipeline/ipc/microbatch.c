/*-------------------------------------------------------------------------
 *
 * microbatch.c
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pipeline/ipc/microbatch.h"

int continuous_query_num_batch;
int continuous_query_batch_size;
