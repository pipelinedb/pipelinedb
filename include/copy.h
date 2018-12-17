/*-------------------------------------------------------------------------
 *
 * copy.h
 *	  COPY for streams
 *
 * Portions Copyright (c) 2018, PipelineDB, Inc.
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_COPY_H
#define PIPELINE_COPY_H

#include "commands/copy.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"

extern copy_data_source_cb stream_copy_hook;

extern void DoStreamCopy(ParseState *state, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed);

extern CopyState BeginCopyStreamFrom(ParseState *pstate, Relation rel, const char *filename,
			  bool is_program, copy_data_source_cb data_source_cb, List *attnamelist, List *options);

extern uint64 CopyStreamFrom(CopyState cstate);

#endif
