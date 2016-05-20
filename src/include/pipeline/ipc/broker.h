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

extern Size IPCMessageBrokerShmemSize(void);
extern void IPCMessageBrokerShmemInit(void);

extern pid_t StartIPCMessageBroker(void);
extern bool IsIPCMessageBrokerProcess(void);

extern void signal_ipc_broker_process(void);

extern ipc_queue *acquire_my_ipc_queue(void);
extern void release_my_ipc_queue(void);
extern ipc_queue *acquire_my_broker_ipc_queue(void);

extern ipc_queue *get_worker_queue_with_lock(void);
extern ipc_queue *get_combiner_queue_with_lock(int idx);

#endif
