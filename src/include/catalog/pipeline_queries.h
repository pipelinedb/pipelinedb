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
#define PipelineQueriesRelationId  4242

CATALOG(pipeline_queries,4242) BKI_WITHOUT_OIDS
{
	int32			id;
	NameData	name;
	char			state;
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
#define Natts_pipeline_queries				4
#define Anum_pipeline_queries_id			1
#define Anum_pipeline_queries_name		2
#define Anum_pipeline_queries_state 	3
#define Anum_pipeline_queries_query 	4


/* ----------------
 *		query states
 * ----------------
 */
#define PIPELINE_QUERY_STATE_ACTIVE 		'a'
#define PIPELINE_QUERY_STATE_INACTIVE 	'i'

/* ----------------
 *		microbatching tuning params
 * ----------------
 */
#define PIPELINE_BATCH_SIZE 1000
#define PIPELINE_SLEEP_MS 10
#define PIPELINE_BATCH_TIMEOUT_MS 0

#endif   /* PIPELINE_QUERIES_H */
