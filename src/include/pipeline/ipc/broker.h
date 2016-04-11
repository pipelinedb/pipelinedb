/*-------------------------------------------------------------------------
 *
 * broker.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_BROKER_H
#define IPC_BROKER_H

#include "postgres.h"

#include "pipeline/ipc/queue.h"

/* guc */
extern int continuous_query_ipc_shared_mem;

extern void IPCMessageBrokerShmemInit(void);

extern pid_t StartIPCMessageBroker(void);
extern bool IsIPCMessageBrokerProcess(void);

extern ipc_queue *acquire_my_ipc_consumer_queue(void);
extern void release_my_ipc_consumer_queue(void);

extern ipc_queue *get_worker_producer_queue_with_lock(void);
extern ipc_queue *get_combiner_producer_queue_with_lock(int idx);

#endif
