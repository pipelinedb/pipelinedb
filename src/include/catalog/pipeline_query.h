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
	Oid			relid;
	bool 		active;
	Oid 		osrelid;

	/* valid for views only */
	Oid			matrelid;
	Oid  	    seqrelid;
	Oid  	    pkidxid;
	Oid 	    lookupidxid;
	int16			sw_attno;
	bool 		adhoc;
	int16 		step_factor;
	int32				ttl;
	int16			ttl_attno;

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
#define Natts_pipeline_query             18
#define Anum_pipeline_query_id           1
#define Anum_pipeline_query_type         2
#define Anum_pipeline_query_relid	     3
#define Anum_pipeline_query_active       4
#define Anum_pipeline_query_osrelid	     5
#define Anum_pipeline_query_matrelid     6
#define Anum_pipeline_query_seqrelid     7
#define Anum_pipeline_query_pkidxid      8
#define Anum_pipeline_query_lookupidxid  9
#define Anum_pipeline_query_sw_attno      10
#define Anum_pipeline_query_adhoc        11
#define Anum_pipeline_query_step_factor  12
#define Anum_pipeline_query_ttl  					13
#define Anum_pipeline_query_ttl_attno  		14
#define Anum_pipeline_query_tgfn         15
#define Anum_pipeline_query_tgnargs	     16
#define Anum_pipeline_query_tgargs       17
#define Anum_pipeline_query_query        18

#define PIPELINE_QUERY_VIEW 		'v'
#define PIPELINE_QUERY_TRANSFORM 	't'

#endif   /* PIPELINE_QUERIES_H */
