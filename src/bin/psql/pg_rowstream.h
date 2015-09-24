/*-------------------------------------------------------------------------
 *
 * rowstream.h
 *    Interface for streaming postgres rows into the adhoc client
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/bin/psql/pg_rowstream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_ROWSTREAM_H
#define PG_ROWSTREAM_H

#include "rowmap.h"

#include "postgres_fe.h"
#include "pqexpbuffer.h"
#include "libpq-fe.h"

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

typedef struct PGRowStream
{
	PGconn *conn;

	RowFunc          callback;
	void             *cb_ctx;
	char 			 *initial;	 	

} PGRowStream;

extern PGRowStream *PGRowStreamInit(const char* sql, RowFunc fn, void *ctx);
extern bool PGRowStreamPostInit(PGRowStream *s);

extern void PGRowStreamDestroy(PGRowStream *s);

/* to be used with select/poll */
extern int PGRowStreamFd(PGRowStream *s);

/*
 * To be called when the fd is ready. Returns true if the stream has hit EOF
 */
extern bool PGRowStreamHandleInput(PGRowStream *s);

#endif
