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

extern Oid PipelineQueryRelationOid;

CATALOG(pipeline_query,4242)
{
	int32		id;
	char 		type;
	Oid			relid;
	bool 		active;
	Oid 		osrelid;

	/* valid for views only */
	Oid			matrelid;
	Oid  	  seqrelid;
	Oid  	  pkidxid;
	Oid 	  lookupidxid;
	int16	step_factor;
	int32		ttl;
	int16		ttl_attno;

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
#define Natts_pipeline_query             16
#define Anum_pipeline_query_id           1
#define Anum_pipeline_query_type         2
#define Anum_pipeline_query_relid	     3
#define Anum_pipeline_query_active       4
#define Anum_pipeline_query_osrelid	     5
#define Anum_pipeline_query_matrelid     6
#define Anum_pipeline_query_seqrelid     7
#define Anum_pipeline_query_pkidxid      8
#define Anum_pipeline_query_lookupidxid  9
#define Anum_pipeline_query_step_factor  10
#define Anum_pipeline_query_ttl  		 11
#define Anum_pipeline_query_ttl_attno  	 12
#define Anum_pipeline_query_tgfn         13
#define Anum_pipeline_query_tgnargs	     14
#define Anum_pipeline_query_tgargs       15
#define Anum_pipeline_query_query        16

#define PIPELINE_QUERY_VIEW 		'v'
#define PIPELINE_QUERY_TRANSFORM 	't'

// consolidate these with other CV options we already have #define'd
#define OPTION_ACTION "action"
#define OPTION_OUTPUTFUNC "outputfunc"
#define OPTION_MATRELID "matrelid"
#define OPTION_OSRELID "osrelid"
#define OPTION_OSRELTYPE "osreltype"
#define OPTION_OSRELATYPE "osrelatype"
#define OPTION_TABLESPACE "tablespace"
#define OPTION_TGFNID "tgfnid"
#define OPTION_TGFN "tgfn"
#define OPTION_TGNARGS "tgnargs"
#define OPTION_TGARGS "tgargs"

#define OPTION_MATRELID "matrelid"
#define OPTION_MATRELTYPE "matreltype"
#define OPTION_MATRELATYPE "matrelatype"
#define OPTION_MATRELTOASTRELID "matreltoastrelid"
#define OPTION_MATRELTOASTTYPE "matreltoasttype"
#define OPTION_MATRELTOASTINDID "matreltoastindid"

#define OPTION_SEQRELID "seqrelid"
#define OPTION_SEQRELTYPE "seqreltype"

#define OPTION_PKINDID "pkindid"
#define OPTION_LOOKUPINDID "lookupindid"

#define ACTION_TRANSFORM "transform"
#define ACTION_MATERIALIZE "materialize"

#endif   /* PIPELINE_QUERIES_H */
