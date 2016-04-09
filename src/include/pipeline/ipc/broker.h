/*-------------------------------------------------------------------------
 *
 * broker.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef BROKER_H
#define BROKER_H

#include "postgres.h"

/* guc */
extern int continuous_query_ipc_shared_mem;

/* functions to start the ipc broker process */
extern pid_t StartIPCMessageBroker(void);
extern bool IsIPCMessageBrokerProcess(void);

#endif
