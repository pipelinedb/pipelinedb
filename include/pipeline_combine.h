/*-------------------------------------------------------------------------
 *
 * pipeline_combine.h
 * 		Interface for pipelinedb.combine catalog
 *
 * Copyright (c) 2018, PipelineDB, Inc.
 *
 * -------------------------------------------------------------------------
 */
#ifndef PIPELINE_COMBINE_H
#define PIPELINE_COMBINE_H

#include "postgres.h"

typedef struct FormData_pipeline_combine
{
	regproc aggfn;
	regproc combineaggfn;
} FormData_pipeline_combine;

typedef FormData_pipeline_combine *Form_pipeline_combine;

#define Natts_pipeline_combine                2
#define Anum_pipeline_combine_aggfn        1
#define Anum_pipeline_combine_combineaggfn 2

extern Oid PipelineQueryCombineOid;

#endif
