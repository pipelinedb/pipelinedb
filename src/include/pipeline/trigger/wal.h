/*-------------------------------------------------------------------------
 *
 * wal.h
 *	  Interface for wal
 *
 * Copyright (c) 2013-2016, PipelineDB
 *
 *-------------------------------------------------------------------------
 */
#ifndef TRIGGER_WAL_H
#define TRIGGER_WAL_H

#include "postgres.h"
#include "trigger.h"

extern WalStream* create_wal_stream(void *pdata);
extern void wal_stream_read(WalStream *stream, bool *did_read);
extern void destroy_wal_stream(WalStream *stream);
extern void wal_init(void);

#endif
