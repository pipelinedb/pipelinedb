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
	char		state;
	NameData	matrelname;
	bool		gc;
	bool		long_xact;
	int32 		batchsize;
	int32 		maxwaitms;
	int32		emptysleepms;
	int16		parallelism;
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
#define Natts_pipeline_query			11
#define Anum_pipeline_query_id			1
#define Anum_pipeline_query_name		2
#define Anum_pipeline_query_state 		3
#define Anum_pipeline_query_matrelname	4
#define Anum_pipeline_query_gc			5
#define Anum_pipeline_query_long_xact		6
#define Anum_pipeline_query_batchsize	7
#define Anum_pipeline_query_maxwaitms 	8
#define Anum_pipeline_query_emptysleepms 9
#define Anum_pipeline_query_parallelism	10
#define Anum_pipeline_query_query 		11

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
#define CQ_SLEEP_MS_KEY		"emptysleepms"
#define CQ_WAIT_MS_KEY		"maxwaitms"
#define CQ_PARALLELISM_KEY	"parallelism"

#define CQ_DEFAULT_BATCH_SIZE 		10000
#define CQ_DEFAULT_EMPTY_SLEEP_MS 	10
#define CQ_DEFAULT_MAX_WAIT_MS		5
#define CQ_DEFAULT_PARALLELISM		1

#define MAX_PARALLELISM 16

#endif   /* PIPELINE_QUERIES_H */
