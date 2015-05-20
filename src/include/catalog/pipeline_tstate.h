/*-------------------------------------------------------------------------
 *
 * pipeline_tstate.h
 *		Definition of the pipeline_tstate catalog table
 *
 * src/include/catalog/pipeline_tstate.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_TSTATE_H
#define PIPELINE_TSTATE_H

#include "catalog/genbki.h"

#define PipelineTStateRelationId  4251

/* ----------------------------------------------------------------
 * ----------------------------------------------------------------
 */
CATALOG(pipeline_tstate,4251) BKI_WITHOUT_OIDS
{
	int32 id;
#ifdef CATALOG_VARLEN
	bloom distinct;
#endif
} FormData_pipeline_tstate;

typedef FormData_pipeline_tstate *Form_pipeline_tstate;

#define Natts_pipeline_tstate			2
#define Anum_pipeline_tstate_id			1
#define Anum_pipeline_tstate_distinct	2

#endif
