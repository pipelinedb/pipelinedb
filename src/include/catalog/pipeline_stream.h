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
	Oid namespace;
	NameData name;
#ifdef CATALOG_VARLEN
	bytea queries;
	bytea desc;
#endif
} FormData_pipeline_stream;

typedef FormData_pipeline_stream *Form_pipeline_stream;

#define Natts_pipeline_stream						3
#define Anum_pipeline_stream_name					1
#define Anum_pipeline_stream_queries 				2
#define Anum_pipeline_stream_desc					3

#endif
