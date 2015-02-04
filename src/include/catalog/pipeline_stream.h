/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipeline_stream.h
 *		Definition of the pipeline_stream catalog table
 *
 * src/include/catalog/pipeline_stream.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_STREAM_H
#define PIPELINE_STREAM_H

#include "catalog/genbki.h"

#define PipelineStreamRelationId  4249

/* ----------------------------------------------------------------
 * ----------------------------------------------------------------
 */
CATALOG(pipeline_stream,4249) BKI_WITHOUT_OIDS
{
	NameData name;
#ifdef CATALOG_VARLEN
	bytea targets;
#endif
} FormData_pipeline_stream;

typedef FormData_pipeline_stream *Form_pipeline_stream;

#define Natts_pipeline_stream						2
#define Anum_pipeline_stream_name					1
#define Anum_pipeline_stream_targets 				2

#endif
