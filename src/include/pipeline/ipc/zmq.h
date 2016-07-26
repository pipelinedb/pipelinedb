/*-------------------------------------------------------------------------
 *
 * zmq.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef IPC_ZMQ_H
#define IPC_ZMQ_H

#include "postgres.h"

extern void pzmq_init(void);
extern void pzmq_destroy(void);

extern void pzmq_bind(uint64 id);
extern void pzmq_connect(uint64 id, int hwm);

extern char *pzmq_recv(int *len, bool wait);
extern bool pzmq_send(uint64 id, char *buf, int len, bool wait);

#endif
