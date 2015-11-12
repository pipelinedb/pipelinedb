/*-------------------------------------------------------------------------
 *
 * pipeline_query.h
 *	  definition of continuous queries that have been registered
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline_query.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERY_H
#define PIPELINE_QUERY_H

#include "catalog/genbki.h"
#include "datatype/timestamp.h"

/* ----------------------------------------------------------------
 *		pipeline_query definition.
 *
 *		cpp turns this into typedef struct FormData_pipeline_query
 * ----------------------------------------------------------------
 */
#define PipelineQueryRelationId  4242

CATALOG(pipeline_query,4242)
{
	int32		id;
	Oid			namespace;
	NameData	name;
	Oid			matrel;
	bool		gc;
	bool 		adhoc;
#ifdef CATALOG_VARLEN
	text		query;
#endif
} FormData_pipeline_query;

/* ----------------
 *		Form_pipeline_query corresponds to a pointer to a tuple with
 *		the format of the pipeline_query relation.
 * ----------------
 */
typedef FormData_pipeline_query *Form_pipeline_query;

/* ----------------
 *		compiler constants for pipeline_query
 * ----------------
 */
#define Natts_pipeline_query			7
#define Anum_pipeline_query_id			1
#define Anum_pipeline_query_namespace	2
#define Anum_pipeline_query_name		3
#define Anum_pipeline_query_matrel		4
#define Anum_pipeline_query_gc			5
#define Anum_pipeline_query_adhoc		6
#define Anum_pipeline_query_query 		7

#endif   /* PIPELINE_QUERIES_H */
