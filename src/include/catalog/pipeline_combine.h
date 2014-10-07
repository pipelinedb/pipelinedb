/*-------------------------------------------------------------------------
 *
 * pipeline_combine.h
 *		Definition of the pipeline_combine catalog table, which contains
 *		information about how to combine partial aggregation results
 *
 * src/include/catalog/pipeline_combine.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PIPELINE_COMBINE_H
#define PIPELINE_COMBINE_H

#include "catalog/genbki.h"

#define PipelineCombineRelationId  4247

/* ----------------------------------------------------------------
 *
 * transfn				transition function whose output is the combiner's input
 *
 * transoutfn			function for serializing transition states before sending
 * 								them to the combiner
 *
 * combineinfn		function for deserializing transition states before using
 * 								them in the combiner
 *
 * combinefn			function that combines multiple transition states into
 * 								a single, final result
 *
 * transouttype		type returned by transition out function
 *
 * ----------------------------------------------------------------
 */
CATALOG(pipeline_combine,4247) BKI_WITHOUT_OIDS
{
	regproc transfn;
	regproc transoutfn;
	regproc combineinfn;
	regproc combinefn;
	Oid transouttype;
} FormData_pipeline_combine;

typedef FormData_pipeline_combine *Form_pipeline_combine;

#define Natts_pipeline_combine							5
#define Anum_pipeline_combine_transfn 			1
#define Anum_pipeline_combine_transoutfn 		2
#define Anum_pipeline_combine_combineinfn 	3
#define Anum_pipeline_combine_combinefn 		4
#define Anum_pipeline_combine_transouttype 	5

DATA(insert (numeric_avg_accum naggstatesend naggstaterecv numeric_avg_combine 17));

#endif
