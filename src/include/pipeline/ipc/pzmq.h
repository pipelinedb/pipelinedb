/*-------------------------------------------------------------------------
 *
 * zmq.h
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef PZMQ_H
#define PZMQ_H

#include "postgres.h"

extern void pzmq_init(void);
extern void pzmq_destroy(void);

extern void pzmq_bind(uint64 id);
extern void pzmq_connect(uint64 id);

extern bool pzmq_pollin(int timeout);
extern char *pzmq_recv(int *len, int timeout);

extern bool pzmq_pollout(void);
extern bool pzmq_send(uint64 id, char *buf, int len, bool wait);

extern void pzmq_purge_sock_files(void);

#endif
