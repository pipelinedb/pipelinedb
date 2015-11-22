/*-------------------------------------------------------------------------
 *
 * pipeline_database.h
 *
 * Copyright (c) 2013-2015, PipelineDB
 *
 * src/include/catalog/pipeline_database.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_DATABASE_H
#define PIPELINE_DATABASE_H

#include "catalog/genbki.h"

/* ----------------
 *		pipeline_database definition.  cpp turns this into
 *		typedef struct FormData_pg_database
 * ----------------
 */
#define PipelineDatabaseRelationId	4467

CATALOG(pipeline_database,4467) BKI_SHARED_RELATION BKI_WITHOUT_OIDS
{
	Oid	 dbid;
	bool cont_queries_enabled;
#ifdef CATALOG_VARLEN
	text meta;
#endif
} FormData_pipeline_database;

/* ----------------
 *		Form_pipeline_database corresponds to a pointer to a tuple with
 *		the format of pipeline_database relation.
 * ----------------
 */
typedef FormData_pipeline_database *Form_pipeline_database;

/* ----------------
 *		compiler constants for pipeline_database
 * ----------------
 */
#define Natts_pipeline_database		1
#define Anum_pg_database_dboid		1

#endif   /* PIPELINE_DATABASE_H */
