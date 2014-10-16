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
 * The pipeline_combine catalog table specifies functions that are chained
 * together to form an aggregation combination pipeline. "combine" in this
 * context means taking multiple transition states and combining them into
 * a single state. For example, if we're combining for COUNT(*), the
 * transition function is increment(), but the combine function is sum().
 * This catalog stores such associations.
 *
 * Additionally, some transition states are represented by internal data
 * structures. For these, workers need to serialize them as data that
 * can be sent in a tuple and combiners need to deserialize them back into
 * the internal transition state. pipeline_combine also provides a way to
 * specify these serialization/deserialization functions.
 *
 * The basic flow of a combination pipeline from workers to combiners is
 * as follows,
 *
 * worker:
 *
 * 	 advance transition state
 * 	 		-> apply transition output function (serialize transition state)
 * 	 			-> send transition state to combiner
 *
 * combiner:
 *
 *   receive from worker
 *   		-> apply combine input function (deserialize transition state)
 *   			-> combine function
 *
 * aggfinalfn			final output function for the aggregate we're combining for
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
	regproc aggfinalfn;
	regproc transfn;
	regproc transoutfn;
	regproc combineinfn;
	regproc combinefn;
	regproc statestorefn;
	regproc stateloadfn;
	bool storestate;
	Oid transouttype;
} FormData_pipeline_combine;

typedef FormData_pipeline_combine *Form_pipeline_combine;

#define Natts_pipeline_combine							9
#define Anum_pipeline_combine_aggfinalfn		1
#define Anum_pipeline_combine_transfn 			2
#define Anum_pipeline_combine_transoutfn 		3
#define Anum_pipeline_combine_combineinfn 	4
#define Anum_pipeline_combine_combinefn 		5
#define Anum_pipeline_combine_statestorefn 	6
#define Anum_pipeline_combine_stateloadfn 	7
#define Anum_pipeline_combine_storestate		8
#define Anum_pipeline_combine_transouttype 	9

/* avg */
DATA(insert (numeric_avg numeric_avg_accum naggstatesend naggstaterecv numeric_combine 0 0 t 17));
DATA(insert (numeric_avg int8_avg_accum naggstatesend naggstaterecv numeric_combine 0 0 t 17));
DATA(insert (int8_avg int4_avg_accum 0 0 int_avg_combine 0 0 t 1231));
DATA(insert (int8_avg int2_avg_accum 0 0 int_avg_combine 0 0 t 1231));
DATA(insert (float8_avg float4_accum 0 0 float8_combine 0 0 t 1022));
DATA(insert (float8_avg float8_accum 0 0 float8_combine 0 0 t 1022));
DATA(insert (interval_avg interval_accum 0 0 interval_combine 0 0 t 1231));

/* sum */
DATA(insert (numeric_sum int8_avg_accum naggstatesend naggstaterecv numeric_combine 0 0 t 17));
DATA(insert (numeric_sum numeric_avg_accum naggstatesend naggstaterecv numeric_combine 0 0 t 17));

/* count */
DATA(insert (0 int8inc 0 0 int8_sum_to_int8 0 0 f 20));
DATA(insert (0 int8inc_any 0 0 int8_sum_to_int8 0 0 f 20));

/* array */
DATA(insert (array_agg_finalfn array_agg_transfn arrayaggstatesend arrayaggstaterecv array_agg_combine 0 0 f 2277));

/* text */
DATA(insert (string_agg_finalfn string_agg_transfn bytea_string_agg_finalfn byteatostringinfo 0 0 0 f 17));

/* json */
DATA(insert (json_agg_finalfn json_agg_transfn bytea_string_agg_finalfn byteatostringinfo 0 0 0 f 17));
DATA(insert (json_object_agg_finalfn json_object_agg_transfn bytea_string_agg_finalfn byteatostringinfo 0 0 0 f 17));

#endif
