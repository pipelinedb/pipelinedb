/*-------------------------------------------------------------------------
 *
 * pipeline_queries.h
 *	  definition of continuous queries that have been registered
 *
 *
 * src/include/catalog/pipeline_queries.h
 *
 * NOTES
 *	  the genbki.pl script reads this file and generates .bki
 *	  information from the DATA() statements.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_QUERIES_H
#define PIPELINE_QUERIES_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *		pipeline_queries definition.
 *
 *		cpp turns this into typedef struct FormData_pipeline_queries
 * ----------------------------------------------------------------
 */
#define QueriesRelationId  4242

CATALOG(pipeline_queries,4242)
{
	Oid				id;
	text			query;
} FormData_pipeline_queries;

/* ----------------
 *		Form_pipeline_queries corresponds to a pointer to a tuple with
 *		the format of the pipeline_queries relation.
 * ----------------
 */
typedef FormData_pipeline_queries *Form_pipeline_queries;

/* ----------------
 *		compiler constants for pipeline_queries
 * ----------------
 */
#define Natts_pipeline_queries				2
#define Anum_pipeline_queries_id			1
#define Anum_pipeline_queries_query 	2


#endif   /* PIPELINE_QUERIES_H */
