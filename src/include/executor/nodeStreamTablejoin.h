/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * nodeStreamTableJoin.h
 *	Interface for stream-table join support
 *
 * src/include/executor/nodeStreamTableJoin.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODE_STREAM_TABLE_JOIN_H
#define NODE_STREAM_TABLE_JOIN_H

#include "nodes/execnodes.h"

extern StreamTableJoinState *ExecInitStreamTableJoin(StreamTableJoin *node, EState *estate, int eflags);
extern TupleTableSlot *ExecStreamTableJoin(StreamTableJoinState *node);
extern void ExecEndStreamTableJoin(StreamTableJoinState *node);
extern void ExecReScanStreamTableJoin(StreamTableJoinState *node);

#endif   /* NODE_STREAM_TABLE_JOIN_H */
