/*-------------------------------------------------------------------------
 *
 * pipeline_query_fn.h
 *	 prototypes for functions in catalog/pipeline_query.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline_query_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERY_FN_H
#define PIPELINE_QUERY_FN_H

#include "datatype/timestamp.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

typedef struct ContinuousView
{
	Oid id;
	Oid namespace;
	NameData name;
	RangeVar *matrel;
	Oid seqrel;
	char *query;
	int sw_step_factor;
	Oid oid; /* OID in pipeline_query table */
} ContinuousView;

extern Oid DefineContinuousView(RangeVar *name, Query *query, Oid matrel, Oid seqrel, bool gc, bool adhoc, Oid *pq_id);
extern HeapTuple GetPipelineQueryTuple(RangeVar *name);
extern SelectStmt *GetContSelectStmt(RangeVar *rv);
extern Relation OpenCVRelFromMatRel(Relation matrel, LOCKMODE lockmode);
extern bool IsAContinuousView(RangeVar *name);
extern bool ContainsSlidingWindowContinuousView(List *nodes);
extern bool IsAMatRel(RangeVar *name, RangeVar **cvname);
extern bool RelIdIsAMatRel(Oid relid, RangeVar **cvname);
extern bool GetGCFlag(RangeVar *name);
extern RangeVar *GetMatRelationName(RangeVar *cv);
extern RangeVar *GetCVNameFromMatRelName(RangeVar *matrel);
extern void RemoveContinuousViewById(Oid oid);

extern Oid GetContViewId(RangeVar *name);
extern ContinuousView *GetContinuousView(Oid id);
extern ContinuousView *RangeVarGetContinuousView(RangeVar *cv_name);
extern Bitmapset *GetAllContinuousViewIds(void);
extern Bitmapset *GetAdhocContinuousViewIds(void);

#endif
