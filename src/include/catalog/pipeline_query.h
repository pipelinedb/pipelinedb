/*-------------------------------------------------------------------------
 *
 * pipeline_query.h
 *	  definition of continuous queries that have been registered
 *
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

CATALOG(pipeline_query,4242) BKI_WITHOUT_OIDS
{
	int32		id;
	NameData	name;
	NameData	matrelname;
	bool		gc;
	bool		needs_xact;
	int32		hash;
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
#define Anum_pipeline_query_name		2
#define Anum_pipeline_query_matrelname	3
#define Anum_pipeline_query_gc			4
#define Anum_pipeline_query_needs_xact	5
#define Anum_pipeline_query_hash		6
#define Anum_pipeline_query_query 		7

#endif   /* PIPELINE_QUERIES_H */
