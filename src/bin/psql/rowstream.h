/*-------------------------------------------------------------------------
 *
 * rowstream.h
 *    Interface for streaming rows into the adhoc client
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/rowstream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ROWSTREAM_H
#define ROWSTREAM_H

#include <stdbool.h>

#include "rowmap.h"

#include "postgres_fe.h"
#include "pqexpbuffer.h"

/*
 * This module is responsible for row i/o and parsing.
 *
 * When a row has been received and parsed, a callback will be fired, 
 * supplying the row type and contents.
 *
 * Row data is allocated here, but expected to be freed elsewhere.
 * Currently the RowMap is responsible for that.
 */

/* row callback - valid types are h,k,i,u,d */
typedef void (*RowFunc) (void *ctx, int type, Row *row);

typedef struct RowStream
{
	int              fd;
	char             buf[4096];
	PQExpBufferData  buffer;
	RowFunc          callback;
	void             *cb_ctx;
} RowStream;

extern RowStream *RowStreamInit(RowFunc fn, void *ctx);
extern void RowStreamDestroy(RowStream *s);

/* to be used with select/poll */
extern int RowStreamFd(RowStream *s);

/* 
 * To be called when the fd is ready. Returns true if the stream has hit EOF
 */
extern bool RowStreamHandleInput(RowStream *s);

#endif
