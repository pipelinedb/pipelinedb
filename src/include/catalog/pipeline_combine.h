/* Copyright (c) 2013-2015 PipelineDB */
/*-------------------------------------------------------------------------
 *
 * pipeline_combine.h
 *		Definition of the pipeline_combine catalog table, which contains
 *		information about how to combine partial aggregation results.
 *
 *		Note: combine information for aggregates defined in extensions is
 *		not known at compile time and thus must be determined at bootstrap
 *		time. See src/gis/postgis/pipeline_gis.sql.in for examples.
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
 * storestate		should the state be stored in a hidden column?
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
	bool storestate;
	Oid transouttype;
} FormData_pipeline_combine;

typedef FormData_pipeline_combine *Form_pipeline_combine;

#define COMBINE_OID 4300
#define IS_COMBINE_AGG(oid) (oid == COMBINE_OID)

#define Natts_pipeline_combine							7
#define Anum_pipeline_combine_aggfinalfn		1
#define Anum_pipeline_combine_transfn 			2
#define Anum_pipeline_combine_transoutfn 		3
#define Anum_pipeline_combine_combineinfn 	4
#define Anum_pipeline_combine_combinefn 		5
#define Anum_pipeline_combine_storestate		6
#define Anum_pipeline_combine_transouttype 	7

/* avg */
DATA(insert (numeric_avg numeric_avg_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_avg int8_avg_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (int8_avg int4_avg_accum 0 0 int_avg_combine t 1016));
DATA(insert (int8_avg int2_avg_accum 0 0 int_avg_combine t 1016));
DATA(insert (float8_avg float4_accum 0 0 float8_combine t 1022));
DATA(insert (float8_avg float8_accum 0 0 float8_combine t 1022));
DATA(insert (interval_avg interval_accum 0 0 interval_combine t 1187));

/* sum */
DATA(insert (numeric_sum int8_avg_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_sum numeric_avg_accum naggstatesend naggstaterecv numeric_combine t 17));

/* count */
DATA(insert (0 int8inc 0 0 int8_sum_to_int8 f 20));
DATA(insert (0 int8inc_any 0 0 int8_sum_to_int8 f 20));

/* array */
DATA(insert (array_agg_finalfn array_agg_transfn arrayaggstatesend arrayaggstaterecv array_agg_combine t 2277));

/* text */
DATA(insert (string_agg_finalfn string_agg_transfn stringaggstatesend stringaggstaterecv string_agg_combine t 17));
DATA(insert (bytea_string_agg_finalfn bytea_string_agg_transfn stringaggstatesend stringaggstaterecv string_agg_combine t 17));

/* json */
DATA(insert (json_agg_finalfn json_agg_transfn bytearecv byteatostringinfo json_agg_combine t 17));
DATA(insert (json_object_agg_finalfn json_object_agg_transfn bytearecv byteatostringinfo json_object_agg_combine t 17));

/* binary regression aggregates */
DATA(insert (0 int8inc_float8_float8 0 0 int8_sum_to_int8 f 20));
DATA(insert (float8_regr_sxx float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_syy float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_sxy float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_avgx float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_avgy float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_r2 float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_slope float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_regr_intercept float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_covar_pop float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_covar_samp float8_regr_accum 0 0 float8_regr_combine t 1022));
DATA(insert (float8_corr float8_regr_accum 0 0 float8_regr_combine t 1022));

/* var_pop */
DATA(insert (numeric_var_pop int8_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_pop int4_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_pop int2_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_pop numeric_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (float8_var_pop float8_accum 0 0 float8_combine t 1022));
DATA(insert (float8_var_pop float4_accum 0 0 float8_combine t 1022));

/* var_samp / variance (same thing) */
DATA(insert (numeric_var_samp int8_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_samp int4_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_samp int2_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_var_samp numeric_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (float8_var_samp float8_accum 0 0 float8_combine t 1022));
DATA(insert (float8_var_samp float4_accum 0 0 float8_combine t 1022));

/* stddev_pop */
DATA(insert (numeric_stddev_pop int8_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_pop int4_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_pop int2_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_pop numeric_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (float8_stddev_pop float8_accum 0 0 float8_combine t 1022));
DATA(insert (float8_stddev_pop float4_accum 0 0 float8_combine t 1022));

/* stddev_samp / stddev (same thing) */
DATA(insert (numeric_stddev_samp int8_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_samp int4_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_samp int2_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (numeric_stddev_samp numeric_accum naggstatesend naggstaterecv numeric_combine t 17));
DATA(insert (float8_stddev_samp float8_accum 0 0 float8_combine t 1022));
DATA(insert (float8_stddev_samp float4_accum 0 0 float8_combine t 1022));

/* ordered-set rank */
DATA(insert (cq_percentile_cont_float8_final cq_percentile_cont_float8_transition cqosastatesend cqosastaterecv cq_percentile_cont_float8_combine t 17));
DATA(insert (cq_percentile_cont_float8_final cq_percentile_cont_float8_transition_multi cqosastatesend cqosastaterecv cq_percentile_cont_float8_combine t 17));

/* hypothetical-set rank */
DATA(insert (cq_rank_final cq_hypothetical_set_transition_multi 0 0 cq_hypothetical_set_combine_multi t 1016));
DATA(insert (cq_percent_rank_final cq_hypothetical_set_transition_multi 0 0 cq_hypothetical_set_combine_multi t 1016));
DATA(insert (cq_cume_dist_final cq_hypothetical_set_transition_multi 0 0 cq_hypothetical_set_combine_multi t 1016));
DATA(insert (hll_dense_rank_final hll_hypothetical_set_transition_multi 0 0 hll_union_agg_trans t 3998));

/* HyperLogLog count distinct */
DATA(insert (hll_count_distinct_final hll_count_distinct_transition 0 0 hll_union_agg_trans t 3998));

/* hll_agg */
DATA(insert (0 hll_agg_trans 0 0 hll_union_agg_trans t 3998));
DATA(insert (0 hll_agg_transp 0 0 hll_union_agg_trans t 3998));

#endif
