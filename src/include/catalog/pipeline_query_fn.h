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

#include "access/htup.h"
#include "datatype/timestamp.h"
#include "nodes/bitmapset.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/lock.h"
#include "utils/relcache.h"

typedef enum ContQueryType
{
	CONT_VIEW,
	CONT_TRANSFORM
} ContQueryType;

typedef struct ContQuery
{
	Oid id;
	Oid oid; /* OID in pipeline_query table */
	bool active;
	ContQueryType type;

	/* meta */
	Oid namespace;
	NameData name;
	char *sql;
	Oid matrelid;

	/* for view */
	RangeVar *matrel;
	Oid seqrel;
	int sw_step_factor;

	/* for transform */
	Oid tgfn;
	int tgnargs;
	char **tgargs;
} ContQuery;

extern HeapTuple GetPipelineQueryTuple(RangeVar *name);
extern void RemovePipelineQueryById(Oid oid);

extern Oid DefineContinuousView(RangeVar *name, Query *query, Oid matrel, Oid seqrel, bool gc, bool adhoc, Oid *pq_id);
extern Oid DefineContinuousTransform(RangeVar *name, Query *query, Oid typoid, Oid fnoid, List *args);

extern Relation OpenCVRelFromMatRel(Relation matrel, LOCKMODE lockmode);
extern bool IsAContinuousView(RangeVar *name);
extern RangeVar *GetSWContinuousViewRangeVar(List *nodes);
extern bool IsAMatRel(RangeVar *name, RangeVar **cvname);
extern bool RelIdIsForMatRel(Oid relid, RangeVar **cvname);
extern bool GetGCFlag(RangeVar *name);
extern RangeVar *GetMatRelName(RangeVar *cv);
extern RangeVar *GetCVNameFromMatRelName(RangeVar *matrel);

extern Bitmapset *GetContinuousQueryIds(void);
extern Bitmapset *GetContinuousViewIds(void);
extern Bitmapset *GetAdhocContinuousViewIds(void);
extern Bitmapset *GetContinuousTransformIds(void);

extern Oid GetContQueryId(RangeVar *name);

extern ContQuery *GetContQueryForId(Oid id);
extern ContQuery *GetContQueryForView(RangeVar *cv_name);
extern ContQuery *GetContQueryForViewId(Oid id);
extern ContQuery *GetContQueryForTransformId(Oid id);

extern bool ContQuerySetActive(Oid id, bool active);

#endif
