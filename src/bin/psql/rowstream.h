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

typedef struct RowMessage
{
	int type; /*  i, u, d, k */
	Row row;
} RowMessage;

typedef void (*RowFunc) (void *ctx, int type, Row *row);

typedef struct RowStream
{
	int              fd;
	char             buf[4096];
	PQExpBufferData  flex;
	RowFunc          callback;
	void             *cb_ctx;
} RowStream;

extern RowStream *RowStreamInit(RowFunc fn, void *ctx);
extern void RowStreamDestroy(RowStream *s);
extern int RowStreamFd(RowStream *s);
extern bool RowStreamHandleInput(RowStream *s);

#endif
