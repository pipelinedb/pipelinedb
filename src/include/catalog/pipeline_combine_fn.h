/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipeline_combine_fn.h
 *	 prototypes for functions in catalog/pipeline_combine.c
 *
 *
 * src/include/catalog/pipeline_combine_fn.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_COMBINE_FN_H
#define PIPELINE_COMBINE_FN_H

#include "catalog/pipeline_combine.h"

Oid GetCombineStateType(Oid aggfnoid);
void GetCombineInfo(Oid aggfnoid, Oid *combinefn, Oid *combineinfn, Oid *statetype);

#endif
