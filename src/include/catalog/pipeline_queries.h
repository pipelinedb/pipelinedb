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

CATALOG(pipeline_queries,4242) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	int32		id;
	NameData	name;
	char		state;
	int8 		batchsize;
	int32 		maxwaitms;
	int32		emptysleepms;
	int16		parallelism;

#ifdef CATALOG_VARLEN
	text		query;
#endif
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
#define Natts_pipeline_queries				8
#define Anum_pipeline_queries_id			1
#define Anum_pipeline_queries_name			2
#define Anum_pipeline_queries_state 		3
#define Anum_pipeline_queries_batchsize		4
#define Anum_pipeline_queries_maxwaitms 	5
#define Anum_pipeline_queries_emptysleepms 	6
#define Anum_pipeline_queries_parallelism	7
#define Anum_pipeline_queries_query 		8

/* ----------------
 *		query states
 * ----------------
 */
#define PIPELINE_QUERY_STATE_ACTIVE 	'a'
#define PIPELINE_QUERY_STATE_INACTIVE 	'i'

/* ----------------
 *		microbatching tuning params
 * ----------------
 */
#define CQ_BATCH_SIZE_KEY 	"batchsize"
#define CQ_SLEEP_MS_KEY			"emptysleepms"
#define CQ_WAIT_MS_KEY			"maxwaitms"
#define CQ_PARALLELISM_KEY	"parallelism"

#define CQ_DEFAULT_BATCH_SIZE 	1000
#define CQ_DEFAULT_SLEEP_MS 	10
#define CQ_DEFAULT_WAIT_MS 		0
#define CQ_DEFAULT_PARALLELISM 	1

#endif   /* PIPELINE_QUERIES_H */
