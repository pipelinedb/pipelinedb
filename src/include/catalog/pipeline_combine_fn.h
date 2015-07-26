/*-------------------------------------------------------------------------
 *
 * pipeline_combine_fn.h
 *	 prototypes for functions in catalog/pipeline_combine.c
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline_combine_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_COMBINE_FN_H
#define PIPELINE_COMBINE_FN_H

#include "catalog/pipeline_combine.h"

extern Oid GetCombineStateType(Oid aggfnoid);
extern void GetCombineInfo(Oid aggfnoid, Oid *combinefn, Oid *combineinfn, Oid *statetype);
extern Oid DefineCombiner(Oid aggoid, List *name, List *args, bool oldstyle, List *parameters);
extern void RemovePipelineCombineById(Oid oid);

#endif
