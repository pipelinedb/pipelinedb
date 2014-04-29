/*-------------------------------------------------------------------------
 *
 * pipeline_encoding.h
 *	  catalog for raw event encodings
 *
 *
 * src/include/catalog/pipeline_encoding.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_ENCODING_H
#define PIPELINE_ENCODING_H

#include "catalog/genbki.h"

/* ----------------------------------------------------------------
 *		pipeline_encoding definition.
 * ----------------------------------------------------------------
 */
#define PipelineEncodingRelationId  4244

CATALOG(pipeline_encoding,4244) BKI_WITHOUT_OIDS
{
	NameData	name;
	Oid 			decodedby;
	text 			decodedbyargnames[1];
	text			decodedbyargvalues[1];
} FormData_pipeline_encoding;

/* ----------------
 *		FormData_pipeline_encodings corresponds to a pointer to a tuple with
 *		the format of the pipeline_encodings relation.
 * ----------------
 */
typedef FormData_pipeline_encoding *Form_pipeline_encoding;

/* ----------------
 *		compiler constants for pipeline_encoding
 * ----------------
 */
#define Natts_pipeline_encoding											4
#define Anum_pipeline_encoding_name 								1
#define Anum_pipeline_encoding_decodedby						2
#define Anum_pipeline_encoding_decodedbyargnames		3
#define Anum_pipeline_encoding_decodedbyargvalues		4

#endif   /* PIPELINE_ENCODING_H */
