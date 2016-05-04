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
	char 		type;
	Oid			namespace;
	NameData	name;
	bool 		active;

	/* valid for views only */
	Oid			matrel;
	Oid         seqrel;
	bool		gc;
	bool 		adhoc;
	int8 		step_factor;

	/* valid for transforms only */
	Oid			tgfn;
	int16		tgnargs;

#ifdef CATALOG_VARLEN
	bytea       tgargs;
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
#define Natts_pipeline_query			14
#define Anum_pipeline_query_id			1
#define Anum_pipeline_query_type		2
#define Anum_pipeline_query_namespace	3
#define Anum_pipeline_query_name		4
#define Anum_pipeline_query_active		5
#define Anum_pipeline_query_matrel		6
#define Anum_pipeline_query_seqrel		7
#define Anum_pipeline_query_gc			8
#define Anum_pipeline_query_adhoc		9
#define Anum_pipeline_query_step_factor 10
#define Anum_pipeline_query_tgfn		11
#define Anum_pipeline_query_tgnargs		12
#define Anum_pipeline_query_tgargs		13
#define Anum_pipeline_query_query 		14

#define PIPELINE_QUERY_VIEW 		'v'
#define PIPELINE_QUERY_TRANSFORM 	't'

#endif   /* PIPELINE_QUERIES_H */
