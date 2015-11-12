/*-------------------------------------------------------------------------
 *
 * cont_adhoc.h
 *
 * Functions for adhoc queries
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/pipeline/cont_adhoc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CONT_ADHOC_H
#define CONT_ADHOC_H

#include "nodes/parsenodes.h"
#include "pipeline/cont_execute.h"

/* 
 * This module is for performing adhoc in-memory continuous queries. 
 * Rows are not persisted in a matrel, but are streamed down to the client
 * in adhoc format
 */
extern void ExecAdhocQuery(Node *select);

/* returns true iff this parsetree is an adhoc query */
extern bool IsAdhocQuery(Node *node);

extern void CleanupAdhocContinuousView(ContinuousView *view);

/* placeholder so noone creates this function */
extern Datum pipeline_exec_adhoc_query(PG_FUNCTION_ARGS);

typedef struct AdhocExecutor AdhocExecutor;
extern StreamTupleState *AdhocExecutorYieldItem(AdhocExecutor *exec, int *len);

typedef struct AdhocQueryState
{
	int volatile *active;
	dsm_segment *segment;
	dsm_cqueue *cqueue;
} AdhocQueryState;

typedef struct AdhocInsertState
{
	int nqueries;
	AdhocQueryState queries[1];
} AdhocInsertState;

extern AdhocInsertState *AdhocInsertStateCreate(Bitmapset *queries);
extern void AdhocInsertStateSend(AdhocInsertState *astate, StreamTupleState *sts, int len);
extern void AdhocInsertStateDestroy(AdhocInsertState *astate);

#endif
