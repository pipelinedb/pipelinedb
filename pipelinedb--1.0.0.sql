-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pipelinedb" to load this file. \quit

CREATE SCHEMA pipelinedb;

CREATE TABLE pipelinedb._exec_lock ();

CREATE FUNCTION stream_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER stream_fdw HANDLER stream_fdw_handler;
CREATE SERVER pipelinedb FOREIGN DATA WRAPPER stream_fdw;

CREATE TABLE pipelinedb.cont_query (
  id int4 NOT NULL,
  type "char",
  relid oid NOT NULL,
  defrelid oid NOT NULL,
  active bool,
  osrelid oid,
  streamrelid oid,

  -- Valid for views only
  matrelid oid NOT NULL,
  seqrelid oid,
  pkidxid oid,
  lookupidxid oid,
  step_factor int2,
  ttl int4,
  ttl_attno int2,

  -- Valid for transforms only
  tgnargs int2,
  tgargs bytea
) WITH OIDS;

CREATE UNIQUE INDEX pipeline_cont_query_relid_index ON pipelinedb.cont_query (relid);
CREATE UNIQUE INDEX pipeline_cont_query_defrelid_index ON pipelinedb.cont_query (defrelid);
CREATE UNIQUE INDEX pipeline_cont_query_id_index ON pipelinedb.cont_query (id);
CREATE UNIQUE INDEX pipeline_cont_query_oid_index ON pipelinedb.cont_query (oid);
CREATE UNIQUE INDEX pipeline_cont_query_matrelid_index ON pipelinedb.cont_query (matrelid);
CREATE UNIQUE INDEX pipeline_cont_query_osrelid_index ON pipelinedb.cont_query (osrelid);

-- These aren't UNIQUE because they can be 0 for transforms
CREATE INDEX pipeline_cont_query_seqrelid_index ON pipelinedb.cont_query (seqrelid);
CREATE INDEX pipeline_cont_query_pkidxid_index ON pipelinedb.cont_query (pkidxid);
CREATE INDEX pipeline_cont_query_lookupidxid_index ON pipelinedb.cont_query (lookupidxid);

CREATE TABLE pipelinedb.stream (
  relid oid NOT NULL,
  queries bytea
) WITH OIDS;

CREATE UNIQUE INDEX pipeline_stream_relid_index ON pipelinedb.stream (relid);
CREATE UNIQUE INDEX pipeline_stream_oid_index ON pipelinedb.stream (oid);

CREATE TABLE pipelinedb.combine (
  aggfn oid,
  combineaggfn oid
);

CREATE UNIQUE INDEX pipeline_combine_aggfn_index ON pipelinedb.combine (aggfn);

CREATE OR REPLACE FUNCTION pipelinedb.get_streams(OUT schema text, OUT name text, OUT queries text[])
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pipeline_get_streams'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION pipelinedb.get_views(
  OUT id oid, OUT schema text, OUT name text, OUT active bool, OUT query text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pipeline_get_views'
LANGUAGE C IMMUTABLE;

CREATE VIEW pipelinedb.views AS
 SELECT * FROM pipelinedb.get_views();

CREATE OR REPLACE FUNCTION pipelinedb.get_transforms(
  OUT id oid, OUT schema text, OUT name text, OUT active bool, OUT tgfunc text, OUT tgargs text[], OUT query text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pipeline_get_transforms'
LANGUAGE C IMMUTABLE;

CREATE VIEW pipelinedb.transforms AS
 SELECT * FROM pipelinedb.get_transforms();


CREATE OR REPLACE FUNCTION pipelinedb.hash_group(VARIADIC "any")
RETURNS integer
AS 'MODULE_PATHNAME', 'hash_group'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pipelinedb.ls_hash_group(VARIADIC "any")
RETURNS bigint
AS 'MODULE_PATHNAME', 'ls_hash_group'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION pipelinedb.insert_into_stream()
RETURNS trigger
AS 'MODULE_PATHNAME', 'insert_into_stream'
LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION date_round(timestamp with time zone, interval)
RETURNS timestamp with time zone
AS 'MODULE_PATHNAME', 'timestamptz_round'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combine_trans_dummy(anyelement, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'combine_trans_dummy'
LANGUAGE C IMMUTABLE;

/*
 * Dummy combine aggregate for user combines
 *
 * User combines will always take a finalized aggregate value as input
 * and return a combined aggregate of the same type, so this dummy aggregate
 * ensures we make it through the analyzer with correct types everywhere.
 */
CREATE AGGREGATE combine(anyelement) (
  sfunc = combine_trans_dummy,
  stype = anyelement,
  initcond = 'combine_dummy',
  parallel = safe
);

/*
 * Dummy combine aggregate used in SW overlay queries
 *
 * It's convenient to differentiate between these and user combines in the planner,
 * so we indicate SW overlay combines using this aggregate
 */
CREATE AGGREGATE sw_combine(anyelement) (
  sfunc = combine_trans_dummy,
  stype = anyelement,
  initcond = 'sw_combine_dummy',
  parallel = safe
);

CREATE AGGREGATE combine_numeric_avg(internal) (
  sfunc = numeric_avg_combine,
  stype = internal,
  finalfunc = numeric_avg,
  combinefunc = numeric_avg_combine,
  serialfunc = numeric_avg_serialize,
  deserialfunc = numeric_avg_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_avg(internal) (
  sfunc = numeric_avg_combine,
  stype = internal,
  finalfunc = numeric_avg_serialize,
  combinefunc = numeric_avg_combine,
  serialfunc = numeric_avg_serialize,
  deserialfunc = numeric_avg_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_int8_avg(internal) (
  sfunc = int8_avg_combine,
  stype = internal,
  finalfunc = numeric_poly_avg,
  combinefunc = int8_avg_combine,
  serialfunc = int8_avg_serialize,
  deserialfunc = int8_avg_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_int8_avg(internal) (
  sfunc = int8_avg_combine,
  stype = internal,
  finalfunc = int8_avg_serialize,
  combinefunc = int8_avg_combine,
  serialfunc = int8_avg_serialize,
  deserialfunc = int8_avg_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_int4_avg(bigint[]) (
  sfunc = int4_avg_combine,
  stype = bigint[],
  finalfunc = int8_avg,
  combinefunc = int4_avg_combine,
  parallel = safe
);
CREATE AGGREGATE partial_combine_int4_avg(bigint[]) (
  sfunc = int4_avg_combine,
  stype = bigint[],
  combinefunc = int4_avg_combine,
  parallel = safe
);

CREATE AGGREGATE combine_float8_avg(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  finalfunc = float8_avg,
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_float8_avg(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE combine_interval_avg(interval[]) (
  sfunc = interval_combine,
  stype = interval[],
  finalfunc = interval_avg,
  combinefunc = interval_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_interval_avg(interval[]) (
  sfunc = interval_combine,
  stype = interval[],
  combinefunc = interval_combine,
  parallel = safe
);

CREATE AGGREGATE combine_int8_sum(internal) (
  sfunc = int8_avg_combine,
  stype = internal,
  finalfunc = numeric_poly_sum,
  combinefunc = int8_avg_combine,
  deserialfunc = int8_avg_deserialize,
  serialfunc = int8_avg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_int8_sum(internal) (
  sfunc = int8_avg_combine,
  stype = internal,
  finalfunc = int8_avg_serialize,
  combinefunc = int8_avg_combine,
  deserialfunc = int8_avg_deserialize,
  serialfunc = int8_avg_serialize,
  parallel = safe
);

CREATE AGGREGATE combine_numeric_sum(internal) (
  sfunc = numeric_avg_combine,
  stype = internal,
  finalfunc = numeric_sum,
  combinefunc = numeric_avg_combine,
  deserialfunc = numeric_avg_deserialize,
  serialfunc = numeric_avg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_sum(internal) (
  sfunc = numeric_avg_combine,
  stype = internal,
  finalfunc = numeric_avg_serialize,
  combinefunc = numeric_avg_combine,
  deserialfunc = numeric_avg_deserialize,
  serialfunc = numeric_avg_serialize,
  parallel = safe
);

-- corr
CREATE AGGREGATE combine_corr(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_corr,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_corr(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- covar_pop
CREATE AGGREGATE combine_covar_pop(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_covar_pop,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_covar_pop(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- covar_samp
CREATE AGGREGATE combine_covar_samp(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_covar_samp,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_covar_samp(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_avgx
CREATE AGGREGATE combine_regr_avgx(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_avgx,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_avgx(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_avgy
CREATE AGGREGATE combine_regr_avgy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_avgy,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_avgy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_count
CREATE AGGREGATE combine_regr_count(int8) (
  sfunc = int8pl,
  stype = int8,
  combinefunc = int8pl,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_count(int8) (
  sfunc = int8pl,
  stype = int8,
  combinefunc = int8pl,
  parallel = safe
);

-- regr_intercept
CREATE AGGREGATE combine_regr_intercept(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_intercept,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_intercept(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_r2
CREATE AGGREGATE combine_regr_r2(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_r2,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_r2(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_slope
CREATE AGGREGATE combine_regr_slope(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_slope,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_slope(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_sxx
CREATE AGGREGATE combine_regr_sxx(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_sxx,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_sxx(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_sxy
CREATE AGGREGATE combine_regr_sxy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_sxy,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_sxy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- regr_syy
CREATE AGGREGATE combine_regr_syy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  finalfunc = float8_regr_syy,
  combinefunc = float8_regr_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_regr_syy(float8[]) (
  sfunc = float8_regr_combine,
  stype = float8[],
  combinefunc = float8_regr_combine,
  parallel = safe
);

-- stddev (float8_combine -> _float8)
CREATE AGGREGATE combine_float8_stddev(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  finalfunc = float8_stddev_samp,
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_float8_stddev(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  combinefunc = float8_combine,
  parallel = safe
);

-- stddev (numeric_poly_combine -> internal)
CREATE AGGREGATE combine_numeric_poly_stddev(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_stddev_samp,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_poly_stddev(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_serialize,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

-- stddev (numeric_combine -> internal)
CREATE AGGREGATE combine_numeric_stddev(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_stddev_samp,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_stddev(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_serialize,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

-- stddev_pop (float8_combine -> _float8)
CREATE AGGREGATE combine_float8_stddev_pop(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  finalfunc = float8_stddev_pop,
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_float8_stddev_pop(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  combinefunc = float8_combine,
  parallel = safe
);

-- stddev_pop (numeric_poly_combine -> internal)
CREATE AGGREGATE combine_numeric_poly_stddev_pop(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_stddev_pop,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_poly_stddev_pop(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_serialize,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

-- stddev_pop  (numeric_combine -> internal)
CREATE AGGREGATE combine_numeric_stddev_pop(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_stddev_pop,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_stddev_pop(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_serialize,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

-- var_pop (float8_combine -> _float8)
CREATE AGGREGATE combine_float8_var_pop(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  finalfunc = float8_var_pop,
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_float8_var_pop(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  combinefunc = float8_combine,
  parallel = safe
);

-- var_pop (numeric_poly_combine -> internal)
CREATE AGGREGATE combine_numeric_poly_var_pop(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_var_pop,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_poly_var_pop(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_serialize,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

-- var_pop (numeric_combine -> internal)
CREATE AGGREGATE combine_numeric_var_pop(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_var_pop,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_var_pop(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_serialize,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

-- var_samp (float8_combine -> _float8)
CREATE AGGREGATE combine_float8_var_samp(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  finalfunc = float8_var_samp,
  combinefunc = float8_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_float8_var_samp(float8[]) (
  sfunc = float8_combine,
  stype = float8[],
  combinefunc = float8_combine,
  parallel = safe
);

-- var_samp (numeric_poly_combine -> internal)
CREATE AGGREGATE combine_numeric_poly_var_samp(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_var_samp,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_poly_var_samp(internal) (
  sfunc = numeric_poly_combine,
  stype = internal,
  finalfunc = numeric_poly_serialize,
  combinefunc = numeric_poly_combine,
  serialfunc = numeric_poly_serialize,
  deserialfunc = numeric_poly_deserialize,
  parallel = safe
);

-- var_samp (numeric_combine -> internal)
CREATE AGGREGATE combine_numeric_var_samp(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_var_samp,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_numeric_var_samp(internal) (
  sfunc = numeric_combine,
  stype = internal,
  finalfunc = numeric_serialize,
  combinefunc = numeric_combine,
  serialfunc = numeric_serialize,
  deserialfunc = numeric_deserialize,
  parallel = safe
);

-- combinable_json_agg_transfn
CREATE FUNCTION combinable_json_agg_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_json_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- combinable_json_agg_finalfn
CREATE FUNCTION combinable_json_agg_finalfn(internal)
RETURNS json
AS 'MODULE_PATHNAME', 'combinable_json_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- json_agg_serialize
CREATE FUNCTION json_agg_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'json_agg_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- json_agg_deserialize
CREATE FUNCTION json_agg_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_agg_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- json_agg_combine
CREATE FUNCTION json_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Cobineable json_object_agg aggregate
CREATE AGGREGATE combinable_json_agg(anyelement) (
  sfunc = combinable_json_agg_transfn,
  stype = internal,
  finalfunc = combinable_json_agg_finalfn,
  combinefunc = json_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE combine_json_agg(internal) (
  sfunc = json_agg_combine,
  stype = internal,
  finalfunc = combinable_json_agg_finalfn,
  combinefunc = json_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_json_agg(internal) (
  sfunc = json_agg_combine,
  stype = internal,
  finalfunc = json_agg_serialize,
  combinefunc = json_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

-- json_object_agg support functions
CREATE FUNCTION combinable_json_object_agg_transfn(internal, "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_json_object_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_json_object_agg_finalfn(internal)
RETURNS json
AS 'MODULE_PATHNAME', 'combinable_json_object_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION json_object_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_object_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- combinable json_object_agg aggregate
CREATE AGGREGATE combinable_json_object_agg("any", "any") (
  sfunc = combinable_json_object_agg_transfn,
  stype = internal,
  finalfunc = combinable_json_object_agg_finalfn,
  combinefunc = json_object_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE combine_json_object_agg(internal) (
  sfunc = json_object_agg_combine,
  stype = internal,
  finalfunc = combinable_json_object_agg_finalfn,
  combinefunc = json_object_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_json_object_agg(internal) (
  sfunc = json_object_agg_combine,
  stype = internal,
  finalfunc = json_agg_serialize,
  combinefunc = json_object_agg_combine,
  deserialfunc = json_agg_deserialize,
  serialfunc = json_agg_serialize,
  parallel = safe
);

-- jsonb_agg support functions
-- combinable_jsonb_agg_transfn
CREATE FUNCTION combinable_jsonb_agg_transfn(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_jsonb_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- combinable_jsonb_agg_finalfn
CREATE FUNCTION combinable_jsonb_agg_finalfn(internal)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'combinable_jsonb_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- jsonb_agg_serialize
CREATE FUNCTION jsonb_agg_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'jsonb_agg_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- jsonb_agg_deserialize
CREATE FUNCTION jsonb_agg_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'jsonb_agg_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- jsonb_agg_combine
CREATE FUNCTION jsonb_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'jsonb_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- Cobineable json_object_agg aggregate
CREATE AGGREGATE combinable_jsonb_agg(anyelement) (
  sfunc = combinable_jsonb_agg_transfn,
  stype = internal,
  finalfunc = combinable_jsonb_agg_finalfn,
  combinefunc = jsonb_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE combine_jsonb_agg(internal) (
  sfunc = jsonb_agg_combine,
  stype = internal,
  finalfunc = combinable_jsonb_agg_finalfn,
  combinefunc = jsonb_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_jsonb_agg(internal) (
  sfunc = jsonb_agg_combine,
  stype = internal,
  finalfunc = jsonb_agg_serialize,
  combinefunc = jsonb_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

-- jsonb_object_agg support functions
CREATE FUNCTION combinable_jsonb_object_agg_transfn(internal, "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_jsonb_object_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_jsonb_object_agg_finalfn(internal)
RETURNS jsonb
AS 'MODULE_PATHNAME', 'combinable_jsonb_object_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION jsonb_object_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'jsonb_object_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- combinable json_object_agg aggregate
CREATE AGGREGATE combinable_jsonb_object_agg("any", "any") (
  sfunc = combinable_jsonb_object_agg_transfn,
  stype = internal,
  finalfunc = combinable_jsonb_object_agg_finalfn,
  combinefunc = jsonb_object_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE combine_jsonb_object_agg(internal) (
  sfunc = jsonb_object_agg_combine,
  stype = internal,
  finalfunc = combinable_jsonb_object_agg_finalfn,
  combinefunc = jsonb_object_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_jsonb_object_agg(internal) (
  sfunc = jsonb_object_agg_combine,
  stype = internal,
  finalfunc = jsonb_agg_serialize,
  combinefunc = jsonb_object_agg_combine,
  deserialfunc = jsonb_agg_deserialize,
  serialfunc = jsonb_agg_serialize,
  parallel = safe
);

-- combinable array_agg
CREATE FUNCTION array_agg_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'array_agg_serialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION array_agg_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'array_agg_deserialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION array_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'array_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_array_agg_finalfn(internal, anynonarray)
RETURNS anyarray
AS 'MODULE_PATHNAME', 'combinable_array_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_array_agg(anynonarray) (
  sfunc = array_agg_transfn,
  stype = internal,
  finalfunc = combinable_array_agg_finalfn,
  finalfunc_extra,
  combinefunc = array_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

-- combinable array_agg_array
CREATE FUNCTION array_agg_array_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'array_agg_array_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION array_agg_array_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'array_agg_array_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION array_agg_array_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'array_agg_array_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_array_agg_array_finalfn(internal, anyarray)
RETURNS anyarray
AS 'MODULE_PATHNAME', 'combinable_array_agg_array_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_array_agg(anyarray) (
  sfunc = array_agg_array_transfn,
  stype = internal,
  finalfunc = combinable_array_agg_array_finalfn,
  finalfunc_extra,
  combinefunc = array_agg_array_combine,
  deserialfunc = array_agg_array_deserialize,
  serialfunc = array_agg_array_serialize,
  parallel = safe
);

-- set_agg supporting functions
CREATE FUNCTION set_agg_trans(internal, anynonarray)
RETURNS internal
AS 'MODULE_PATHNAME', 'set_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION set_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'set_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION set_cardinality(internal, anynonarray)
RETURNS integer
AS 'MODULE_PATHNAME', 'set_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- set_agg aggregate
CREATE AGGREGATE set_agg(anynonarray) (
  sfunc = set_agg_trans,
  stype = internal,
  finalfunc_extra,
  finalfunc = combinable_array_agg_finalfn,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

CREATE FUNCTION combinable_array_agg_finalfn2(internal, "any")
RETURNS "any"
AS 'MODULE_PATHNAME', 'combinable_array_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combine_set_agg(internal) (
  sfunc = set_agg_combine,
  stype = internal,
  finalfunc_extra,
  finalfunc = combinable_array_agg_finalfn2,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_set_agg(internal) (
  sfunc = set_agg_combine,
  stype = internal,
  finalfunc = array_agg_serialize,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

-- exact_count_distinct aggregate
CREATE AGGREGATE exact_count_distinct(anynonarray) (
  sfunc = set_agg_trans,
  stype = internal,
  finalfunc_extra,
  finalfunc = set_cardinality,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

-- exact_count_distinct aggregate
CREATE AGGREGATE combine_exact_count_distinct(internal) (
  sfunc = set_agg_combine,
  stype = internal,
  finalfunc_extra,
  finalfunc = set_cardinality,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_exact_count_distinct(internal) (
  sfunc = set_agg_combine,
  stype = internal,
  finalfunc = array_agg_serialize,
  combinefunc = set_agg_combine,
  deserialfunc = array_agg_deserialize,
  serialfunc = array_agg_serialize,
  parallel = safe
);

/*
 * HyperLogLog aggregates and supporting functionality
 */

-- We need to create the shell type first so that we can use hyperloglog in the support function signatures
CREATE TYPE hyperloglog;

CREATE FUNCTION hll_in(cstring)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_out(hyperloglog)
RETURNS cstring
AS 'MODULE_PATHNAME', 'hll_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_send(hyperloglog)
RETURNS bytea
AS 'MODULE_PATHNAME', 'hll_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_recv(internal)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE hyperloglog (
  input = hll_in,
  output = hll_out,
  receive = hll_recv,
  send = hll_send,
  alignment = int4,
  storage = extended
);

CREATE FUNCTION hll_print(hyperloglog)
RETURNS text
AS 'MODULE_PATHNAME', 'hll_print'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_agg_trans(internal, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_agg_transp(internal, anyelement, int4)
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_union_agg_trans(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_union_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_union_agg_trans_hll(internal, hyperloglog)
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_union_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_cardinality(hyperloglog)
RETURNS int8
AS 'MODULE_PATHNAME', 'hll_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_cache_cardinality(hyperloglog)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_cache_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_cache_cardinality_internal(internal)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_cache_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_empty()
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_empty(int4)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_add(hyperloglog, anyelement)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_add(hyperloglog, text)
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_count_distinct_transition(internal, "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_count_distinct_transition'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_count_distinct_final(internal)
RETURNS int8
AS 'MODULE_PATHNAME', 'hll_count_distinct_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'hll_serialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_deserialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_union(variadic "any")
RETURNS hyperloglog
AS 'MODULE_PATHNAME', 'hll_union'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE hll_count_distinct("any") (
  sfunc = hll_count_distinct_transition,
  stype = internal,
  finalfunc = hll_count_distinct_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE combine_hll_count_distinct(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_count_distinct_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE partial_combine_hll_count_distinct(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_serialize,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE hll_agg(anyelement) (
  sfunc = hll_agg_trans,
  stype = internal,
  finalfunc = hll_cache_cardinality_internal,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE hll_agg(anyelement, int4) (
  sfunc = hll_agg_transp,
  stype = internal,
  finalfunc = hll_cache_cardinality_internal,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE hll_union_agg(hyperloglog) (
  sfunc = hll_union_agg_trans_hll,
  stype = internal,
  finalfunc = hll_cache_cardinality_internal,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE combine_hll_agg(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_cache_cardinality_internal,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE partial_combine_hll_agg(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_serialize,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

/*
 * Streamining ordered and hypothetical set aggregates
 *
 * OS/HS aggregates are not combinable, so we create our own standard aggregates
 * for use within CQs. Since we can't use WITHIN GROUP (ORDER BY ...)
 * 
 * The interface is of the form:
 * 
 *   hll_dense_rank(1, 2, x, y)
 *
 * We then use the initial constant expressions as if they were direct arguments 
 * in an OS/HS aggregate, with the remaining variable expressions as the ORDER BY clause.
 * When PG supports combinable OS aggs, we can update these to use the WITHIN GROUP clause.
 */
CREATE FUNCTION hll_hypothetical_set_transition_multi1(internal, "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hll_dense_rank_final(internal)
RETURNS int8
AS 'MODULE_PATHNAME', 'hll_hypothetical_dense_rank_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_dense_rank("any", "any") (
  sfunc = hll_hypothetical_set_transition_multi1,
  stype = internal,
  finalfunc = hll_dense_rank_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE combine_dense_rank(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_dense_rank_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE partial_combine_dense_rank(internal) (
  sfunc = hll_union_agg_trans,
  stype = internal,
  finalfunc = hll_serialize,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

-- Combine functions and serialization/deserialization functions don't work well
-- with variadic args, so we have to add explicit signatures for the number of arguments
-- we want to support :/
CREATE FUNCTION hll_hypothetical_set_transition_multi2(internal, "any", "any", "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_dense_rank("any", "any", "any", "any") (
  sfunc = hll_hypothetical_set_transition_multi2,
  stype = internal,
  finalfunc = hll_dense_rank_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

CREATE FUNCTION hll_hypothetical_set_transition_multi3(internal, "any", "any", "any", "any", "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'hll_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_dense_rank("any", "any", "any", "any", "any", "any") (
  sfunc = hll_hypothetical_set_transition_multi3,
  stype = internal,
  finalfunc = hll_dense_rank_final,
  serialfunc = hll_serialize,
  deserialfunc = hll_deserialize,
  combinefunc = hll_union_agg_trans,
  parallel = safe
);

-- pipelinedb.rank
CREATE FUNCTION cq_rank_final(int8[])
RETURNS int8
AS 'MODULE_PATHNAME', 'cq_hypothetical_rank_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_hypothetical_set_transition_multi1(int8[], "any", "any")
RETURNS int8[]
AS 'MODULE_PATHNAME', 'cq_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_hypothetical_set_transition_multi2(int8[], "any", "any", "any", "any")
RETURNS int8[]
AS 'MODULE_PATHNAME', 'cq_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_hypothetical_set_transition_multi3(int8[], "any", "any", "any", "any", "any", "any")
RETURNS int8[]
AS 'MODULE_PATHNAME', 'cq_hypothetical_set_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_hypothetical_set_combine_multi(int8[], int8[])
RETURNS int8[]
AS 'MODULE_PATHNAME', 'cq_hypothetical_set_combine_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_rank("any", "any") (
  sfunc = cq_hypothetical_set_transition_multi1,
  stype = int8[],
  finalfunc = cq_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_rank("any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi2,
  stype = int8[],
  finalfunc = cq_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_rank("any", "any", "any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi3,
  stype = int8[],
  finalfunc = cq_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combine_rank(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  finalfunc = cq_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE partial_combine_rank(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

-- percent_rank
CREATE FUNCTION cq_hypothetical_percent_rank_final(int8[])
RETURNS float8
AS 'MODULE_PATHNAME', 'cq_hypothetical_percent_rank_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_percent_rank("any", "any") (
  sfunc = cq_hypothetical_set_transition_multi1,
  stype = int8[],
  finalfunc = cq_hypothetical_percent_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_percent_rank("any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi2,
  stype = int8[],
  finalfunc = cq_hypothetical_percent_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_percent_rank("any", "any", "any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi3,
  stype = int8[],
  finalfunc = cq_hypothetical_percent_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combine_percent_rank(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  finalfunc = cq_hypothetical_percent_rank_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE partial_combine_percent_rank(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

-- cume_dist
CREATE FUNCTION cq_hypothetical_cume_dist_final(int8[])
RETURNS float8
AS 'MODULE_PATHNAME', 'cq_hypothetical_cume_dist_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_cume_dist("any", "any") (
  sfunc = cq_hypothetical_set_transition_multi1,
  stype = int8[],
  finalfunc = cq_hypothetical_cume_dist_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_cume_dist("any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi2,
  stype = int8[],
  finalfunc = cq_hypothetical_cume_dist_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combinable_cume_dist("any", "any", "any", "any", "any", "any") (
  sfunc = cq_hypothetical_set_transition_multi3,
  stype = int8[],
  finalfunc = cq_hypothetical_cume_dist_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE combine_cume_dist(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  finalfunc = cq_hypothetical_cume_dist_final,
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

CREATE AGGREGATE partial_combine_cume_dist(int8[]) (
  sfunc = cq_hypothetical_set_combine_multi,
  stype = int8[],
  combinefunc = cq_hypothetical_set_combine_multi,
  parallel = safe
);

-- percentile_cont
CREATE FUNCTION cq_percentile_cont_float8_final(internal)
RETURNS float8
AS 'MODULE_PATHNAME', 'cq_percentile_cont_float8_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_percentile_cont_float8_final_multi(internal)
RETURNS float8[]
AS 'MODULE_PATHNAME', 'cq_percentile_cont_float8_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_percentile_cont_float8_transition(internal, float8, float8)
RETURNS internal
AS 'MODULE_PATHNAME', 'cq_percentile_cont_float8_transition'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_percentile_cont_float8_transition_multi(internal, float8[], float8)
RETURNS internal
AS 'MODULE_PATHNAME', 'cq_percentile_cont_float8_transition_multi'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_percentile_cont_float8_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'cq_percentile_cont_float8_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_os_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'cq_os_serialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cq_os_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'cq_os_deserialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_percentile_cont(float8, float8) (
  sfunc = cq_percentile_cont_float8_transition,
  stype = internal,
  finalfunc = cq_percentile_cont_float8_final,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_percentile_cont(internal) (
  sfunc = cq_percentile_cont_float8_combine,
  stype = internal,
  finalfunc = cq_percentile_cont_float8_final,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_percentile_cont(internal) (
  sfunc = cq_percentile_cont_float8_combine,
  stype = internal,
  finalfunc = cq_os_serialize,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

-- percentile_cont
CREATE AGGREGATE combinable_percentile_cont(float8[], float8) (
  sfunc = cq_percentile_cont_float8_transition_multi,
  stype = internal,
  finalfunc = cq_percentile_cont_float8_final_multi,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_percentile_cont_multi(internal) (
  sfunc = cq_percentile_cont_float8_combine,
  stype = internal,
  finalfunc = cq_percentile_cont_float8_final_multi,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_percentile_cont_multi(internal) (
  sfunc = cq_percentile_cont_float8_combine,
  stype = internal,
  finalfunc = cq_os_serialize,
  combinefunc = cq_percentile_cont_float8_combine,
  serialfunc = cq_os_serialize,
  deserialfunc = cq_os_deserialize,
  parallel = safe
);

/*
 * Bloom filter aggregates and supporting functionality
 */

-- We need to create the shell type first so that we can use bloom in the support function signatures
CREATE TYPE bloom;

CREATE FUNCTION bloom_in(cstring)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_out(bloom)
RETURNS cstring
AS 'MODULE_PATHNAME', 'bloom_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_send(bloom)
RETURNS bytea
AS 'MODULE_PATHNAME', 'bloom_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_recv(internal)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE bloom (
  input = bloom_in,
  output = bloom_out,
  receive = bloom_recv,
  send = bloom_send,
  alignment = int4,
  storage = extended
);

CREATE FUNCTION bloom_empty()
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_empty(float8, int8)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_add(bloom, anyelement)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_add(bloom, text)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_cardinality(bloom)
RETURNS int8
AS 'MODULE_PATHNAME', 'bloom_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_contains(bloom, anyelement)
RETURNS bool
AS 'MODULE_PATHNAME', 'bloom_contains'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_print(bloom)
RETURNS text
AS 'MODULE_PATHNAME', 'bloom_print'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_agg_trans(bloom, anyelement)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_agg_trans(bloom, anyelement, float8, int8)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_union_agg_trans(bloom, bloom)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_union_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE bloom_agg(anyelement) (
  sfunc = bloom_agg_trans,
  stype = bloom,
  combinefunc = bloom_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE bloom_agg(anyelement, float8, int8) (
  sfunc = bloom_agg_trans,
  stype = bloom,
  combinefunc = bloom_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE combine_bloom_agg(bloom) (
  sfunc = bloom_union_agg_trans,
  stype = bloom,
  combinefunc = bloom_union_agg_trans,
  parallel = safe
);

CREATE AGGREGATE partial_combine_bloom_agg(bloom) (
  sfunc = bloom_union_agg_trans,
  stype = bloom,
  combinefunc = bloom_union_agg_trans,
  parallel = safe
);

CREATE FUNCTION bloom_union(variadic "any")
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_union'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE bloom_union_agg(bloom) (
  sfunc = bloom_union_agg_trans,
  stype = bloom,
  combinefunc = bloom_union_agg_trans,
  parallel = safe
);

CREATE FUNCTION bloom_intersection(variadic "any")
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_intersection'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bloom_intersection_agg_trans(bloom, bloom)
RETURNS bloom
AS 'MODULE_PATHNAME', 'bloom_intersection_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE bloom_intersection_agg(bloom) (
  sfunc = bloom_intersection_agg_trans,
  stype = bloom,
  combinefunc = bloom_intersection_agg_trans,
  parallel = safe
);

/*
 * Filtered space-saving top K aggregates and associated functionality
 */

-- We need to create the shell type first so that we can use topk in the support function signatures
CREATE TYPE topk;

CREATE FUNCTION topk_in(cstring)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_out(topk)
RETURNS cstring
AS 'MODULE_PATHNAME', 'topk_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_send(topk)
RETURNS bytea
AS 'MODULE_PATHNAME', 'topk_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_recv(internal)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE topk (
  input = topk_in,
  output = topk_out,
  receive = topk_recv,
  send = topk_send,
  alignment = int4,
  storage = extended
);

CREATE FUNCTION topk_empty(regtype, integer)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_empty(regtype, integer, integer, integer)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_print(topk)
RETURNS text
AS 'MODULE_PATHNAME', 'topk_print'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_agg_trans(topk, anyelement, integer)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_agg_trans(topk, anyelement, integer, integer, integer)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_merge_agg_trans(topk, topk)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_merge_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_increment(topk, anyelement)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_increment'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE topk_agg(anyelement, integer) (
  sfunc = topk_agg_trans,
  stype = topk,
  combinefunc = topk_merge_agg_trans,
  parallel = safe
);

CREATE AGGREGATE topk_agg(anyelement, integer, integer, integer) (
  sfunc = topk_agg_trans,
  stype = topk,
  combinefunc = topk_merge_agg_trans,
  parallel = safe
);

CREATE AGGREGATE combine_topk_agg(topk) (
  sfunc = topk_merge_agg_trans,
  stype = topk,
  combinefunc = topk_merge_agg_trans,
  parallel = safe
);

CREATE AGGREGATE partial_combine_topk_agg(topk) (
  sfunc = topk_merge_agg_trans,
  stype = topk,
  combinefunc = topk_merge_agg_trans,
  parallel = safe
);

CREATE FUNCTION topk_increment(topk, anyelement, int8)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_increment_weighted'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_agg_weighted_trans(topk, anyelement, integer, int8)
RETURNS topk
AS 'MODULE_PATHNAME', 'topk_agg_weighted_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE topk_agg(anyelement, integer, int8) (
  sfunc = topk_agg_weighted_trans,
  stype = topk,
  combinefunc = topk_merge_agg_trans,
  parallel = safe
);

CREATE FUNCTION hashed_topk_final(internal)
RETURNS topk
AS 'MODULE_PATHNAME', 'hashed_topk_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hashed_topk_merge_agg_trans(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'hashed_topk_merge_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hashed_topk_agg_weighted_trans(internal, anyelement, integer, int8)
RETURNS internal
AS 'MODULE_PATHNAME', 'hashed_topk_agg_weighted_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hashed_topk_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'hashed_topk_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hashed_topk_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'hashed_topk_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk(topk)
RETURNS setof record
AS 'MODULE_PATHNAME', 'topk'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_values(topk, anyarray default null)
RETURNS anyarray
AS 'MODULE_PATHNAME', 'topk_values'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION topk_freqs(topk)
RETURNS int8[]
AS 'MODULE_PATHNAME', 'topk_freqs'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

/*
 * T-Digest aggregates and associated functionality
 */

-- We need to create the shell type first so that we can use tdigest in the support function signatures
CREATE TYPE tdigest;

-- do we need to make this strict?
CREATE FUNCTION tdigest_in(cstring)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_out(tdigest)
RETURNS cstring
AS 'MODULE_PATHNAME', 'tdigest_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_send(tdigest)
RETURNS bytea
AS 'MODULE_PATHNAME', 'tdigest_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_recv(internal)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE tdigest (
  input = tdigest_in,
  output = tdigest_out,
  receive = tdigest_recv,
  send = tdigest_send,
  alignment = int4,
  storage = extended
);

CREATE FUNCTION tdigest_empty()
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_empty(integer)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_compress(tdigest)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'tdigest_compress'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_add(tdigest, float8)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_add(tdigest, float8, integer)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_addn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_cdf(tdigest, float8)
RETURNS float8
AS 'MODULE_PATHNAME', 'dist_cdf'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_quantile(tdigest, float8)
RETURNS float8
AS 'MODULE_PATHNAME', 'dist_quantile'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_trans(internal, float8)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_trans(internal, float8, integer)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'dist_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION dist_agg_final(internal)
RETURNS tdigest
AS 'MODULE_PATHNAME', 'dist_agg_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'tdigest_serialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION tdigest_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'tdigest_deserialize'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE dist_agg(float8) (
  sfunc = dist_agg_trans,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE dist_agg(float8, integer) (
  sfunc = dist_agg_trans,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE combine_dist_agg(internal) (
  sfunc = dist_combine,
  stype = internal,
  finalfunc = dist_agg_final,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_dist_agg(internal) (
  sfunc = dist_combine,
  stype = internal,
  finalfunc = tdigest_serialize,
  serialfunc = tdigest_serialize,
  deserialfunc = tdigest_deserialize,
  combinefunc = dist_combine,
  parallel = safe
);

/*
 * Count-min sketch aggregates and associated functionality
 */

-- We need to create the shell type first so that we can use tdigest in the support function signatures
CREATE TYPE cmsketch;

CREATE FUNCTION cmsketch_in(cstring)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_in'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_out(cmsketch)
RETURNS cstring
AS 'MODULE_PATHNAME', 'cmsketch_out'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_send(cmsketch)
RETURNS bytea
AS 'MODULE_PATHNAME', 'cmsketch_send'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_recv(internal)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_recv'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

-- Now we can fill in the shell type created above
CREATE TYPE cmsketch (
  input = cmsketch_in,
  output = cmsketch_out,
  receive = cmsketch_recv,
  send = cmsketch_send,
  alignment = int4,
  storage = extended
);

-- cmsketch_print
CREATE FUNCTION cmsketch_print(cmsketch)
RETURNS text
AS 'MODULE_PATHNAME', 'cmsketch_print'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_empty()
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_empty'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_empty(float8, float8)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_emptyp'
LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_agg_trans(cmsketch, anyelement)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'freq_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_agg_trans(cmsketch, anyelement, float8, float8)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'freq_agg_transp'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION cmsketch_combine(cmsketch, cmsketch)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq(cmsketch, anyelement)
RETURNS integer
AS 'MODULE_PATHNAME', 'cmsketch_frequency'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq(cmsketch, text)
RETURNS integer
AS 'MODULE_PATHNAME', 'cmsketch_frequency'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_total(cmsketch)
RETURNS int8
AS 'MODULE_PATHNAME', 'cmsketch_total'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_norm(cmsketch, anyelement)
RETURNS float8
AS 'MODULE_PATHNAME', 'cmsketch_norm_frequency'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_add(cmsketch, anyelement)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_add(cmsketch, text)
RETURNS cmsketch
AS 'MODULE_PATHNAME', 'cmsketch_add'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION freq_add(cmsketch, anyelement, integer)
RETURNS float8
AS 'MODULE_PATHNAME', 'cmsketch_addn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE freq_agg(anyelement) (
  sfunc = freq_agg_trans,
  stype = cmsketch,
  combinefunc = cmsketch_combine,
  parallel = safe
);

CREATE AGGREGATE freq_merge_agg(cmsketch) (
  sfunc = cmsketch_combine,
  stype = cmsketch,
  combinefunc = cmsketch_combine,
  parallel = safe
);

CREATE AGGREGATE combine_freq_agg(cmsketch) (
  sfunc = cmsketch_combine,
  stype = cmsketch,
  combinefunc = cmsketch_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_freq_agg(cmsketch) (
  sfunc = cmsketch_combine,
  stype = cmsketch,
  combinefunc = cmsketch_combine,
  parallel = safe
);

/*
 * Keyed min/max aggregates and supporting functions
 */
CREATE FUNCTION keyed_min_trans(bytea, "any", "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'keyed_min_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION keyed_max_trans(bytea, "any", "any")
RETURNS bytea
AS 'MODULE_PATHNAME', 'keyed_max_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION keyed_min_combine(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME', 'keyed_min_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION keyed_max_combine(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME', 'keyed_max_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION keyed_min_max_finalize(bytea, "any", anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'keyed_min_max_finalize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE keyed_min("any", anyelement) (
  sfunc = keyed_min_trans,
  finalfunc = keyed_min_max_finalize,
  finalfunc_extra,
  stype = bytea,
  combinefunc = keyed_min_combine,
  parallel = safe
);

CREATE FUNCTION combine_keyed_min_max_finalize(bytea, anyelement default null)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'keyed_min_max_finalize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combine_keyed_min(bytea) (
  sfunc = keyed_min_combine,
  finalfunc = combine_keyed_min_max_finalize,
  finalfunc_extra,
  stype = bytea,
  combinefunc = keyed_min_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_keyed_min(bytea) (
  sfunc = keyed_min_combine,
  stype = bytea,
  combinefunc = keyed_min_combine,
  parallel = safe
);

INSERT INTO pipelinedb.combine (aggfn, combineaggfn) VALUES
  ((SELECT oid FROM pg_proc WHERE proname = 'keyed_min'), (SELECT oid FROM pg_proc WHERE proname = 'combine_keyed_min'));

CREATE AGGREGATE keyed_max("any", anyelement) (
  sfunc = keyed_max_trans,
  finalfunc = keyed_min_max_finalize,
  finalfunc_extra,
  stype = bytea,
  combinefunc = keyed_max_combine,
  parallel = safe
);

CREATE AGGREGATE combine_keyed_max(bytea) (
  sfunc = keyed_max_combine,
  finalfunc = combine_keyed_min_max_finalize,
  finalfunc_extra,
  stype = bytea,
  combinefunc = keyed_max_combine,
  parallel = safe
);

CREATE AGGREGATE partial_combine_keyed_max(bytea) (
  sfunc = keyed_max_combine,
  stype = bytea,
  combinefunc = keyed_max_combine,
  parallel = safe
);

INSERT INTO pipelinedb.combine (aggfn, combineaggfn) VALUES
  ((SELECT oid FROM pg_proc WHERE proname = 'keyed_max'), (SELECT oid FROM pg_proc WHERE proname = 'combine_keyed_max'));

/*
 * first_values aggregate and supporting functions
 * Combine functions and serialization/deserialization functions don't work well
 * with variadic args, so we have to add explicit signatures for the number of arguments
 * we want to support :/
 */
CREATE FUNCTION first_values_trans1(internal, integer, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME', 'first_values_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_trans2(internal, integer, anyelement, "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'first_values_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_trans3(internal, integer, anyelement, "any", "any")
RETURNS internal
AS 'MODULE_PATHNAME', 'first_values_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_final1(internal, integer, anyelement)
RETURNS anyarray
AS 'MODULE_PATHNAME', 'first_values_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_final2(internal, integer, anyelement, "any")
RETURNS anyarray
AS 'MODULE_PATHNAME', 'first_values_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_final3(internal, integer, anyelement, "any", "any")
RETURNS anyarray
AS 'MODULE_PATHNAME', 'first_values_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'first_values_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'first_values_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION first_values_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'first_values_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE first_values(integer, anyelement) (
  sfunc = first_values_trans1,
  finalfunc = first_values_final1,
  finalfunc_extra,
  stype = internal,
  combinefunc = first_values_combine,
  serialfunc = first_values_serialize,
  deserialfunc = first_values_deserialize,
  parallel = safe
);

CREATE AGGREGATE first_values(integer, anyelement, "any") (
  sfunc = first_values_trans2,
  finalfunc = first_values_final2,
  finalfunc_extra,
  stype = internal,
  combinefunc = first_values_combine,
  serialfunc = first_values_serialize,
  deserialfunc = first_values_deserialize,
  parallel = safe
);

CREATE AGGREGATE first_values(integer, anyelement, "any", "any") (
  sfunc = first_values_trans3,
  finalfunc = first_values_final3,
  finalfunc_extra,
  stype = internal,
  combinefunc = first_values_combine,
  serialfunc = first_values_serialize,
  deserialfunc = first_values_deserialize,
  parallel = safe
);

/*
 * bucket_agg aggregate and supporting functions
 */
CREATE FUNCTION bucket_agg_trans(internal, anyelement, int2)
RETURNS internal
AS 'MODULE_PATHNAME', 'bucket_agg_trans'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_agg_trans_ts(internal, anyelement, int2, timestamptz)
RETURNS internal
AS 'MODULE_PATHNAME', 'bucket_agg_trans_ts'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'bucket_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_agg_final(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'bucket_agg_final'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

-- bucket_cardinalities
CREATE FUNCTION bucket_cardinalities(bytea)
RETURNS integer[]
AS 'MODULE_PATHNAME', 'bucket_cardinalities'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_cardinality(bytea)
RETURNS bigint
AS 'MODULE_PATHNAME', 'bucket_cardinality'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_agg_state_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'bucket_agg_state_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_agg_state_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'bucket_agg_state_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION bucket_ids(bytea)
RETURNS int2[]
AS 'MODULE_PATHNAME', 'bucket_ids'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE bucket_agg(anyelement, int2) (
  sfunc = bucket_agg_trans,
  finalfunc = bucket_agg_final,
  stype = internal,
  combinefunc = bucket_agg_combine,
  serialfunc = bucket_agg_state_serialize,
  deserialfunc = bucket_agg_state_deserialize,
  parallel = safe
);

CREATE AGGREGATE bucket_agg(anyelement, int2, timestamptz) (
  sfunc = bucket_agg_trans_ts,
  finalfunc = bucket_agg_final,
  stype = internal,
  combinefunc = bucket_agg_combine,
  serialfunc = bucket_agg_state_serialize,
  deserialfunc = bucket_agg_state_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_bucket_agg(internal) (
  sfunc = bucket_agg_combine,
  finalfunc = bucket_agg_final,
  stype = internal,
  combinefunc = bucket_agg_combine,
  serialfunc = bucket_agg_state_serialize,
  deserialfunc = bucket_agg_state_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_bucket_agg(internal) (
  sfunc = bucket_agg_combine,
  finalfunc = bucket_agg_state_serialize,
  stype = internal,
  combinefunc = bucket_agg_combine,
  serialfunc = bucket_agg_state_serialize,
  deserialfunc = bucket_agg_state_deserialize,
  parallel = safe
);

/*
 * Combinable string_agg and bytea_string_agg
 */
CREATE FUNCTION combinable_string_agg_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME', 'combinable_string_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_string_agg_transfn(internal, text, text)
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_string_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_bytea_string_agg_finalfn(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'combinable_bytea_string_agg_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_bytea_string_agg_transfn(internal, bytea, bytea)
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_bytea_string_agg_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION combinable_string_agg_combine(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'combinable_string_agg_combine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION string_agg_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'string_agg_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION string_agg_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'string_agg_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE combinable_string_agg(text, text) (
  sfunc = combinable_string_agg_transfn,
  finalfunc = combinable_string_agg_finalfn,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_string_agg(internal) (
  sfunc = combinable_string_agg_combine,
  finalfunc = combinable_string_agg_finalfn,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_string_agg(internal) (
  sfunc = combinable_string_agg_combine,
  finalfunc = string_agg_serialize,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

CREATE AGGREGATE combinable_string_agg(bytea, bytea) (
  sfunc = combinable_bytea_string_agg_transfn,
  finalfunc = combinable_bytea_string_agg_finalfn,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

CREATE AGGREGATE combine_bytea_string_agg(internal) (
  sfunc = combinable_string_agg_combine,
  finalfunc = combinable_bytea_string_agg_finalfn,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

CREATE AGGREGATE partial_combine_bytea_string_agg(internal) (
  sfunc = combinable_string_agg_combine,
  finalfunc = string_agg_serialize,
  stype = internal,
  combinefunc = combinable_string_agg_combine,
  serialfunc = string_agg_serialize,
  deserialfunc = string_agg_deserialize,
  parallel = safe
);

/*
 * json_object_int_sum
 */
CREATE FUNCTION json_object_int_sum_serialize(internal)
RETURNS bytea
AS 'MODULE_PATHNAME', 'json_object_int_sum_serialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION json_object_int_sum_deserialize(bytea, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_object_int_sum_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION json_object_int_sum_transfn(internal, text)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_object_int_sum_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION json_object_int_sum_transfn(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME', 'json_object_int_sum_transfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION json_object_int_sum_finalfn(internal)
RETURNS text
AS 'MODULE_PATHNAME', 'json_object_int_sum_finalfn'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE json_object_int_sum(text) (
  sfunc = json_object_int_sum_transfn,
  finalfunc = json_object_int_sum_finalfn,
  stype = internal,
  combinefunc = json_object_int_sum_transfn,
  serialfunc = json_object_int_sum_serialize,
  deserialfunc = json_object_int_sum_deserialize,
  parallel = safe
);

/*
 * Function that allows us to call aggregate deserialization functions from overlay views.
 * Many of them expect to be called in an aggregate context, so we create the expected
 * context and then use it to call the target deserialization function.
 */
CREATE FUNCTION pipelinedb.finalize(text, text[], bytea, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME', 'pipeline_finalize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION pipelinedb.deserialize(regproc, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME', 'pipeline_deserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

/*
 * miscellaneous utility functions
 */
CREATE FUNCTION year(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_year'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION month(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_month'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION day(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_day'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION hour(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_hour'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION minute(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_minute'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION second(timestamptz)
RETURNS timestamptz
AS 'MODULE_PATHNAME', 'timestamptz_second'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION pipelinedb.version()
RETURNS text
AS 'MODULE_PATHNAME', 'pipeline_version'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION pipelinedb.truncate_continuous_view(text)
RETURNS void
AS 'MODULE_PATHNAME', 'pipeline_truncate_continuous_view'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.ttl_expire(text)
RETURNS int8
AS 'MODULE_PATHNAME', 'pipeline_ttl_expire'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.set_ttl(text, interval, text)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pipeline_set_ttl'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.activate(text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pipeline_activate'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.deactivate(text)
RETURNS bool
AS 'MODULE_PATHNAME', 'pipeline_deactivate'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.flush()
RETURNS bool
AS 'MODULE_PATHNAME', 'pipeline_flush'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.combine_table(text, text)
RETURNS bool
AS 'MODULE_PATHNAME', 'combine_table'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.get_worker_querydef(text)
RETURNS text
AS 'MODULE_PATHNAME', 'pipeline_get_worker_querydef'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.get_combiner_querydef(text)
RETURNS text
AS 'MODULE_PATHNAME', 'pipeline_get_combiner_querydef'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION pipelinedb.get_stream_readers()
RETURNS table (
  stream text,
  continuous_queries text[]
)
AS 'MODULE_PATHNAME', 'pipeline_get_stream_readers'
LANGUAGE C IMMUTABLE;

/*
 * Human-readable stream readers
 */
CREATE VIEW pipelinedb.stream_readers AS
 SELECT stream, continuous_queries FROM pipelinedb.get_stream_readers();

/*
 * stats retrieval functions and VIEWs
 */
CREATE FUNCTION pipelinedb.get_proc_query_stats()
RETURNS table (
  type text,
  pid int4,
  start_time timestamptz,
  query_id int4,
  input_rows int8,
  output_rows int8,
  updated_rows int8,
  input_bytes int8,
  output_bytes int8,
  updated_bytes int8,
  executions int8,
  errors int8,
  exec_ms int8
)
AS 'MODULE_PATHNAME', 'pipeline_get_proc_query_stats'
LANGUAGE C IMMUTABLE;

-- Raw stats, most granular form
CREATE VIEW pipelinedb.proc_query_stats AS
 SELECT
   type,
   pid,
   start_time,
   query_id,
   input_rows,
   output_rows,
   updated_rows,
   output_bytes,
   updated_bytes,
   input_bytes,
   executions,
   errors,
   exec_ms
 FROM pipelinedb.get_proc_query_stats();

-- Stats by process type, pid
CREATE VIEW pipelinedb.proc_stats AS
 SELECT
   type,
   pid,
   min(start_time) AS start_time,
   sum(input_rows) AS input_rows,
   sum(output_rows) AS output_rows,
   sum(updated_rows) AS updated_rows,
   sum(output_bytes) AS output_bytes,
   sum(updated_bytes) AS updated_bytes,
   sum(input_bytes) AS input_bytes,
   sum(executions) AS executions,
   sum(errors) AS errors,
   sum(exec_ms) AS exec_ms
 FROM pipelinedb.proc_query_stats
GROUP BY type, pid
ORDER BY type, pid;

-- Stats by process type, query
CREATE VIEW pipelinedb.query_stats AS
 SELECT
   s.type,
   n.nspname AS namespace,
   c.relname AS continuous_query,
   sum(input_rows) AS input_rows,
   sum(output_rows) AS output_rows,
   sum(updated_rows) AS updated_rows,
   sum(output_bytes) AS output_bytes,
   sum(updated_bytes) AS updated_bytes,
   sum(input_bytes) AS input_bytes,
   sum(executions) AS executions,
   sum(errors) AS errors,
   sum(exec_ms) AS exec_ms
 FROM pipelinedb.proc_query_stats s
 JOIN pipelinedb.cont_query pq ON pq.id = s.query_id
 JOIN pg_class c ON pq.relid = c.oid
 JOIN pg_namespace n ON c.relnamespace = n.oid
GROUP BY s.type, namespace, continuous_query 
ORDER BY s.type, namespace, continuous_query;

-- Stats by process type
CREATE VIEW pipelinedb.db_stats AS
 SELECT
   type,
   sum(input_rows) AS input_rows,
   sum(output_rows) AS output_rows,
   sum(updated_rows) AS updated_rows,
   sum(output_bytes) AS output_bytes,
   sum(updated_bytes) AS updated_bytes,
   sum(input_bytes) AS input_bytes,
   sum(executions) AS executions,
   sum(errors) AS errors,
   sum(exec_ms) AS exec_ms
 FROM pipelinedb.proc_query_stats
GROUP BY type
ORDER BY type;

CREATE FUNCTION pipelinedb.get_stream_stats()
RETURNS table (
  relid oid,
  input_rows int8,
  input_batches int8,
  input_bytes int8
)
AS 'MODULE_PATHNAME', 'pipeline_get_stream_stats'
LANGUAGE C IMMUTABLE;

CREATE VIEW pipelinedb.stream_stats AS
 SELECT
  n.nspname AS namespace,
  c.relname AS stream,
  input_rows,
  input_batches,
  input_bytes
 FROM pipelinedb.get_stream_stats() s
 JOIN pg_class c ON s.relid = c.oid
 JOIN pg_namespace n ON c.relnamespace = n.oid;
