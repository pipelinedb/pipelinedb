CREATE FOREIGN TABLE test_uc_stream (k text, x int, s text, y int) SERVER pipelinedb;

-- Verify some validation
CREATE VIEW test_uc_validation AS SELECT k::text, avg(x::integer) FROM test_uc_stream GROUP BY k;
CREATE TABLE test_uc_table (v numeric);
INSERT INTO test_uc_table (v) VALUES (0), (1), (2);

-- combine only accepts a single colref as an argument
SELECT combine(avg + 1) FROM test_uc_validation;
SELECT combine(avg, avg) FROM test_uc_validation;

-- combine isn't allowed on tables
SELECT combine(v) FROM test_uc_table;

-- combine is only allowed on aggregate columns
SELECT combine(k) FROM test_uc_validation;

-- Column doesn't exist
SELECT combine(nothere) FROM test_uc_validation;

DROP TABLE test_uc_table;
DROP VIEW test_uc_validation;

CREATE VIEW test_uc0 AS
  SELECT x % 10 AS g,
    avg(x::numeric)  AS numeric_avg,
    avg(x::int8)  AS int8_avg,
    avg(x::float8)  AS float8_avg,
    sum(x::numeric)  AS numeric_sum,
    sum(x::int8)  AS int8_sum,
    sum(x::float4)  AS float4_sum
  FROM test_uc_stream
GROUP BY g;

INSERT INTO test_uc_stream (x) SELECT generate_series(1, 1000) AS x;
INSERT INTO test_uc_stream (x) SELECT 0 FROM generate_series(1, 1000);

SELECT * FROM test_uc0 ORDER BY g;

SELECT combine(numeric_avg), avg(numeric_avg),
  combine(int8_avg),
  combine(float8_avg),
  combine(numeric_sum),
  combine(int8_sum),
  combine(float4_sum), sum(float4_sum)
FROM test_uc0;

SELECT g % 2 AS x,
  combine(numeric_avg),
  combine(int8_avg),
  combine(float8_avg),
  combine(numeric_sum),
  combine(int8_sum),
  combine(float4_sum)
FROM test_uc0
GROUP BY x ORDER BY x;

SELECT combine(numeric_avg) + combine(numeric_sum) FROM test_uc0;

CREATE TABLE test_uc_table AS SELECT generate_series(1, 5) AS x;

SELECT g % 2 AS y,
  combine(numeric_avg), avg(numeric_avg),
  combine(int8_avg),
  combine(float8_avg),
  combine(numeric_sum),
  combine(int8_sum),
  combine(float4_sum), sum(float4_sum)
FROM test_uc0 v JOIN test_uc_table t ON v.g = t.x
GROUP BY y ORDER BY y;

DROP TABLE test_uc_table;
DROP FOREIGN TABLE test_uc_stream CASCADE;

-- User combines for all supported aggregate signatures

-- bloom_agg
CREATE FOREIGN TABLE bloom_s (x integer) SERVER pipelinedb;
CREATE VIEW bloom0 AS SELECT x % 10 AS g, bloom_agg(x) FROM bloom_s GROUP BY g;

INSERT INTO bloom_s (x) SELECT generate_series(1, 1000) AS x;

SELECT bloom_cardinality(bloom_agg) FROM bloom0 ORDER BY g;
SELECT bloom_cardinality(combine(bloom_agg)) FROM bloom0;

DROP FOREIGN TABLE bloom_s CASCADE;

-- avg
CREATE FOREIGN TABLE avg_s (x integer) SERVER pipelinedb;

-- numeric
CREATE VIEW avg0 AS SELECT x % 4 AS g , avg(x::numeric) FROM avg_s GROUP BY g;

-- int8
CREATE VIEW avg1 AS SELECT x % 4 AS g , avg(x::int8) FROM avg_s GROUP BY g;

-- int4
CREATE VIEW avg2 AS SELECT x % 4 AS g , avg(x::int4) FROM avg_s GROUP BY g;

-- int2
CREATE VIEW avg3 AS SELECT x % 4 AS g , avg(x::int2) FROM avg_s GROUP BY g;

-- float8
CREATE VIEW avg4 AS SELECT x % 4 AS g , avg(x::float8) FROM avg_s GROUP BY g;

-- float4
CREATE VIEW avg5 AS SELECT x % 4 AS g , avg(x::float4) FROM avg_s GROUP BY g;

-- interval
CREATE VIEW avg6 AS SELECT x % 4 AS g , avg(x * interval '1 second') FROM avg_s GROUP BY g;

INSERT INTO avg_s (x) SELECT generate_series(1, 1000) x;

SELECT combine(avg) FROM avg0;
SELECT combine(avg) FROM avg1;
SELECT combine(avg) FROM avg2;
SELECT combine(avg) FROM avg3;
SELECT combine(avg) FROM avg4;
SELECT combine(avg) FROM avg5;
SELECT combine(avg) FROM avg6;

DROP FOREIGN TABLE avg_s CASCADE;

-- sum
CREATE FOREIGN TABLE sum_s (x integer) SERVER pipelinedb;

-- numeric
CREATE VIEW sum0 AS SELECT x % 4 AS g , sum(x::numeric) FROM sum_s GROUP BY g;

-- int8
CREATE VIEW sum1 AS SELECT x % 4 AS g , sum(x::int8) FROM sum_s GROUP BY g;

-- int4
CREATE VIEW sum2 AS SELECT x % 4 AS g , sum(x::int4) FROM sum_s GROUP BY g;

-- int2
CREATE VIEW sum3 AS SELECT x % 4 AS g , sum((x % 10)::int2) FROM sum_s GROUP BY g;

-- float8
CREATE VIEW sum4 AS SELECT x % 4 AS g , sum(x::float8) FROM sum_s GROUP BY g;

-- float4
CREATE VIEW sum5 AS SELECT x % 4 AS g , sum(x::float4) FROM sum_s GROUP BY g;

-- interval
CREATE VIEW sum6 AS SELECT x % 4 AS g , sum(x * interval '1 second') FROM sum_s GROUP BY g;

-- money
CREATE VIEW sum7 AS SELECT x % 4 AS g , sum(x::money) FROM sum_s GROUP BY g;

INSERT INTO sum_s (x) SELECT generate_series(1, 1000) x;

SELECT combine(sum) FROM sum0;
SELECT combine(sum) FROM sum1;
SELECT combine(sum) FROM sum2;
SELECT combine(sum) FROM sum3;
SELECT combine(sum) FROM sum4;
SELECT combine(sum) FROM sum5;
SELECT combine(sum) FROM sum6;
SELECT combine(sum) FROM sum7;

DROP FOREIGN TABLE sum_s CASCADE;

-- bucket_agg
CREATE FOREIGN TABLE bucket_s (x integer) SERVER pipelinedb;

CREATE VIEW bucket0 AS SELECT x % 10 AS g, bucket_agg(x, (x % 2)::int2) FROM bucket_s GROUP BY g;
CREATE VIEW bucket1 AS SELECT x % 10 AS g, bucket_agg(x, (x % 2)::int2, arrival_timestamp) FROM bucket_s GROUP BY g;

INSERT INTO bucket_s (x) SELECT generate_series(1, 1000) x;

SELECT unnest(bucket_ids(combine(bucket_agg))) AS id, bucket_cardinalities(combine(bucket_agg)) FROM bucket0 ORDER BY id;
SELECT unnest(bucket_ids(combine(bucket_agg))) AS id, bucket_cardinalities(combine(bucket_agg)) FROM bucket1 ORDER BY id;

DROP FOREIGN TABLE bucket_s CASCADE;

-- json_agg, json_object_agg
CREATE FOREIGN TABLE json_s (x integer) SERVER pipelinedb;

CREATE VIEW json0 AS SELECT x % 10 AS g, json_agg(x) FROM json_s GROUP BY g;
CREATE VIEW json1 AS SELECT x % 10 AS g, json_object_agg(x, x) FROM json_s GROUP BY g;

INSERT INTO json_s (x) SELECT generate_series(1, 1000) x;

SELECT json_array_length(combine(json_agg)) FROM json0;
SELECT count(*) FROM (SELECT json_each(combine(json_object_agg)) FROM json1) _;

DROP FOREIGN TABLE json_s CASCADE;

-- jsonb_agg, jsonb_object_agg
CREATE FOREIGN TABLE jsonb_s (x integer) SERVER pipelinedb;

CREATE VIEW jsonb0 AS SELECT x % 10 AS g, jsonb_agg(x) FROM jsonb_s GROUP BY g;
CREATE VIEW jsonb1 AS SELECT x % 10 AS g, jsonb_object_agg(x, x) FROM jsonb_s GROUP BY g;

INSERT INTO jsonb_s (x) SELECT generate_series(1, 1000) x;

SELECT jsonb_array_length(combine(jsonb_agg)) FROM jsonb0;
SELECT count(*) FROM (SELECT jsonb_each(combine(jsonb_object_agg)) FROM jsonb1) _;

DROP FOREIGN TABLE jsonb_s CASCADE;

-- binary regression aggregates
CREATE FOREIGN TABLE regr_s (x integer) SERVER pipelinedb;

-- corr
CREATE VIEW regr0 AS SELECT x % 2 AS g, corr(x, x + 1) FROM regr_s GROUP BY g;

-- covar_pop
CREATE VIEW regr1 AS SELECT x % 2 AS g, covar_pop(x, x + 1) FROM regr_s GROUP BY g;

-- covar_samp
CREATE VIEW regr2 AS SELECT x % 2 AS g, covar_samp(x, x + 1) FROM regr_s GROUP BY g;

-- regr_avgx
CREATE VIEW regr3 AS SELECT x % 2 AS g, regr_avgx(x, x + 1) FROM regr_s GROUP BY g;

-- regr_avgy
CREATE VIEW regr4 AS SELECT x % 2 AS g, regr_avgy(x, x + 1) FROM regr_s GROUP BY g;

-- regr_count
CREATE VIEW regr5 AS SELECT x % 2 AS g, regr_count(x, x + 1) FROM regr_s GROUP BY g;

-- regr_intercept
CREATE VIEW regr6 AS SELECT x % 2 AS g, regr_intercept(x, x + 1) FROM regr_s GROUP BY g;

-- regr_r2
CREATE VIEW regr7 AS SELECT x % 2 AS g, regr_r2(x, x + 1) FROM regr_s GROUP BY g;

-- regr_slope
CREATE VIEW regr8 AS SELECT x % 2 AS g, regr_slope(x, x + 1) FROM regr_s GROUP BY g;

-- regr_sxx
CREATE VIEW regr9 AS SELECT x % 2 AS g, regr_sxx(x, x + 1), regr_sxy(x, x + 1), regr_syy(x, x + 1) FROM regr_s GROUP BY g;

-- stddev (stddev_samp)
CREATE VIEW regr10 AS SELECT
  x % 2 AS g,
  stddev(x::int8) AS int8_stddev,
  stddev(x::int4) AS int4_stddev,
  stddev(x::int2) AS int2_stddev,
  stddev(x::float8) AS float8_stddev,
  stddev(x::float4) AS float4_stddev,
  stddev(x::numeric) AS numeric_stddev,
  stddev_samp(x::int8) AS int8_stddev_samp,
  stddev_samp(x::int4) AS int4_stddev_samp,
  stddev_samp(x::int2) AS int2_stddev_samp,
  stddev_samp(x::float8) AS float8_stddev_samp,
  stddev_samp(x::float4) AS float4_stddev_samp,
  stddev_samp(x::numeric) AS numeric_stddev_samp
FROM regr_s GROUP BY g;

-- stddev_pop
CREATE VIEW regr11 AS SELECT
  x % 2 AS g,
  stddev_pop(x::int8) AS int8_stddev,
  stddev_pop(x::int4) AS int4_stddev,
  stddev_pop(x::int2) AS int2_stddev,
  stddev_pop(x::float8) AS float8_stddev,
  stddev_pop(x::float4) AS float4_stddev,
  stddev_pop(x::numeric) AS numeric_stddev
FROM regr_s GROUP BY g;

-- var_pop
CREATE VIEW regr12 AS SELECT
  x % 2 AS g,
  var_pop(x::int8) AS int8_var_pop,
  var_pop(x::int4) AS int4_var_pop,
  var_pop(x::int2) AS int2_var_pop,
  var_pop(x::float8) AS float8_var_pop,
  var_pop(x::float4) AS float4_var_pop,
  var_pop(x::numeric) AS numeric_var_pop
FROM regr_s GROUP BY g;

-- var_samp
CREATE VIEW regr13 AS SELECT
  x % 2 AS g,
  var_samp(x::int8) AS int8_var_samp,
  var_samp(x::int4) AS int4_var_samp,
  var_samp(x::int2) AS int2_var_samp,
  var_samp(x::float8) AS float8_var_samp,
  var_samp(x::float4) AS float4_var_samp,
  var_samp(x::numeric) AS numeric_var_samp,
  variance(x::int8) AS int8_variance,
  variance(x::int4) AS int4_variance,
  variance(x::int2) AS int2_variance,
  variance(x::float8) AS float8_variance,
  variance(x::float4) AS float4_variance,
  variance(x::numeric) AS numeric_variance
FROM regr_s GROUP BY g;

INSERT INTO regr_s (x) SELECT generate_series(1, 1000) x;

SELECT combine(corr) FROM regr0;
SELECT combine(covar_pop) FROM regr1;
SELECT combine(covar_samp) FROM regr2;
SELECT combine(regr_avgx) FROM regr3;
SELECT combine(regr_avgy) FROM regr4;
SELECT combine(regr_count) FROM regr5;
SELECT combine(regr_intercept) FROM regr6;
SELECT combine(regr_r2) FROM regr7;
SELECT combine(regr_slope) FROM regr8;
SELECT combine(regr_sxx), combine(regr_sxy), combine(regr_syy) FROM regr9;

SELECT
 combine(int8_stddev),
 combine(int4_stddev),
 combine(int2_stddev),
 combine(float8_stddev),
 combine(float4_stddev),
 combine(numeric_stddev),
 combine(int8_stddev_samp),
 combine(int4_stddev_samp),
 combine(int2_stddev_samp),
 combine(float8_stddev_samp),
 combine(float4_stddev_samp),
 combine(numeric_stddev_samp)
FROM regr10;

SELECT
 combine(int8_stddev),
 combine(int4_stddev),
 combine(int2_stddev),
 combine(float8_stddev),
 combine(float4_stddev),
 combine(numeric_stddev)
FROM regr11;

SELECT
 combine(int8_var_pop),
 combine(int4_var_pop),
 combine(int2_var_pop),
 combine(float8_var_pop),
 combine(float4_var_pop),
 combine(numeric_var_pop)
FROM regr12;

SELECT
 combine(int8_var_samp),
 combine(int4_var_samp),
 combine(int2_var_samp),
 combine(float8_var_samp),
 combine(float4_var_samp),
 combine(numeric_var_samp),
 combine(int8_variance),
 combine(int4_variance),
 combine(int2_variance),
 combine(float8_variance),
 combine(float4_variance),
 combine(numeric_variance)
FROM regr13;

DROP FOREIGN TABLE regr_s CASCADE;

-- combinable_cume_dist
CREATE FOREIGN TABLE cume_dist_s (x integer) SERVER pipelinedb;

CREATE VIEW cume_dist0 AS SELECT
 x % 10 AS g,
 cume_dist(2) WITHIN GROUP (ORDER BY x) AS cd0,
 cume_dist(3, 3) WITHIN GROUP (ORDER BY x, x) AS cd1,
 cume_dist(4, 4, 4) WITHIN GROUP (ORDER BY x, x, x) AS cd2
FROM cume_dist_s GROUP BY g;

INSERT INTO cume_dist_s (x) SELECT generate_series(1, 1000) x;

SELECT combine(cd0), combine(cd1), combine(cd2) FROM cume_dist0;

DROP FOREIGN TABLE cume_dist_s CASCADE;

-- combinable_dense_rank, combinable_rank, combinable_percent_rank
CREATE FOREIGN TABLE dense_rank_s (x integer) SERVER pipelinedb;

CREATE VIEW dense_rank0 AS SELECT
 x % 10 AS g,
 dense_rank(5) WITHIN GROUP (ORDER BY x) AS d0,
 dense_rank(5, 5) WITHIN GROUP (ORDER BY x, x) AS d1,
 dense_rank(5, 5, 5) WITHIN GROUP (ORDER BY x, x, x) AS d2
FROM dense_rank_s GROUP BY g;

CREATE VIEW rank0 AS SELECT
 x % 10 AS g,
 rank(5) WITHIN GROUP (ORDER BY x) AS d0,
 rank(5, 5) WITHIN GROUP (ORDER BY x, x) AS d1,
 rank(5, 5, 5) WITHIN GROUP (ORDER BY x, x, x) AS d2
FROM dense_rank_s GROUP BY g;

CREATE VIEW rank1 AS SELECT
 x % 10 AS g,
 percent_rank(5) WITHIN GROUP (ORDER BY x) AS d0,
 percent_rank(5, 5) WITHIN GROUP (ORDER BY x, x) AS d1,
 percent_rank(5, 5, 5) WITHIN GROUP (ORDER BY x, x, x) AS d2
FROM dense_rank_s GROUP BY g;

INSERT INTO dense_rank_s (x) SELECT generate_series(1, 1000) x;

SELECT combine(d0), combine(d1), combine(d2) FROM dense_rank0;
SELECT combine(d0), combine(d1), combine(d2) FROM rank0;
SELECT combine(d0), combine(d1), combine(d2) FROM rank1;

DROP FOREIGN TABLE dense_rank_s CASCADE;

-- combinable_percentile_cont
CREATE FOREIGN TABLE percentile_s (x integer) SERVER pipelinedb;

CREATE VIEW percentile0 AS SELECT
 x % 10 AS g,
 percentile_cont(0.90) WITHIN GROUP (ORDER BY x),
 percentile_cont(ARRAY[0.90, 0.95, 0.99]) WITHIN GROUP (ORDER BY x) AS multi
FROM percentile_s GROUP BY g;

INSERT INTO percentile_s (x) SELECT generate_series(1, 1000) x;

-- tdigest results can vary due to  nondeterministic combine order, so we allow a margin of error here
SELECT round(combine(percentile_cont)) - 900 < 2 FROM percentile0;
SELECT round(unnest(combine((multi)))) - unnest('{900,950,990}'::int[]) < 2 FROM percentile0;

DROP FOREIGN TABLE percentile_s CASCADE;

-- dist_agg
CREATE FOREIGN TABLE dist_s (x integer) SERVER pipelinedb;

CREATE VIEW dist0 AS SELECT
 x % 10 AS g,
 dist_agg(x) AS d0,
 dist_agg(x, 1) AS d1
FROM dist_s GROUP BY g;

INSERT INTO dist_s (x) SELECT generate_series(1, 1000) x;

SELECT round(dist_cdf(combine(d0), 600)), round(dist_quantile(combine(d1), 0.99)) FROM dist0;

DROP FOREIGN TABLE dist_s CASCADE;

-- freq_agg
CREATE FOREIGN TABLE freq_s (x integer) SERVER pipelinedb;

CREATE VIEW freq0 AS SELECT x % 10 AS g, freq_agg(x) FROM freq_s GROUP BY g;

INSERT INTO freq_s (x) SELECT generate_series(1, 1000) x;

SELECT freq_total(combine(freq_agg)) FROM freq0;
SELECT freq_total(combine(freq_agg)) FROM freq0 WHERE g % 2 = 0;

DROP FOREIGN TABLE freq_s CASCADE;

-- hll_agg, hll_count_distinct
CREATE FOREIGN TABLE hll_s (x integer) SERVER pipelinedb;

CREATE VIEW hll0 AS SELECT x % 10 AS g, hll_agg(x) FROM hll_s GROUP BY g;
CREATE VIEW hll1 AS SELECT x % 10 AS g, count(DISTINCT x) FROM hll_s GROUP BY g;

INSERT INTO hll_s (x) SELECT generate_series(1, 1000) x;

SELECT hll_cardinality(combine(hll_agg)) FROM hll0;
SELECT combine(count) FROM hll1;

DROP FOREIGN TABLE hll_s CASCADE;

-- topk_agg, topk_agg_weighted
CREATE FOREIGN TABLE topk_s (x integer) SERVER pipelinedb;

CREATE VIEW topk0 AS SELECT
 x % 10 AS g,
 topk_agg(x, 10) AS topk,
 topk_agg(x, 10, 1) as topkw
FROM topk_s GROUP BY g;

INSERT INTO topk_s (x) SELECT generate_series(1, 1000) x;
INSERT INTO topk_s (x) SELECT 1 FROM generate_series(1, 1000);

SELECT
 topk_freqs(combine(topk)), topk_freqs(combine(topkw))
FROM topk0;

DROP FOREIGN TABLE topk_s CASCADE;

-- keyed_min/max
CREATE FOREIGN TABLE keyed_s (x integer, y integer) SERVER pipelinedb;

CREATE VIEW keyed0 AS SELECT
 x % 10 AS g,
 keyed_min(x, y),
 keyed_max(x, y)
FROM keyed_s GROUP BY g;

INSERT INTO keyed_s (x, y) SELECT x, 10 * x FROM generate_series(1, 1000) x;

SELECT combine(keyed_min), combine(keyed_max) FROM keyed0;

DROP FOREIGN TABLE keyed_s CASCADE;

-- combine in HAVING clause
CREATE FOREIGN TABLE having_s (x integer) SERVER pipelinedb;

CREATE VIEW having0 AS SELECT x % 10 AS g, count(distinct x), sum(x) FROM having_s GROUP BY g;

INSERT INTO having_s (x) SELECT generate_series(1, 1000) x;

SELECT g % 4 AS x, combine(count) AS count, combine(sum) AS sum
 FROM having0 GROUP BY x HAVING combine(count) - combine(sum) >= -99800 ORDER BY x;

CREATE TABLE having_t AS SELECT generate_series(1, 5) x;

-- HAVING referencing both table and CV aggregates
SELECT
 t.x % 4 AS g,
 sum(t.x)
FROM having0 h
JOIN having_t t ON h.g = t.x
GROUP BY t.x
HAVING combine(h.count) + 1 > 100 ORDER BY g;

DROP FOREIGN TABLE having_s CASCADE;
DROP TABLE having_t;

-- Verify combine works within CTEs
CREATE FOREIGN TABLE cte_s (x integer) SERVER pipelinedb;

CREATE VIEW cte0 WITH (sw = '1 day') AS
 SELECT x, count(*) FROM cte_s GROUP BY x;

CREATE VIEW cte1 AS
 SELECT x, count(*) FROM cte_s GROUP BY x;

INSERT INTO cte_s (x) SELECT x % 10 FROM generate_series(1, 1000) x;

-- This will have an implicit sw_combine call
WITH cte AS (
  SELECT * FROM cte0
)
SELECT * FROM cte ORDER BY x;

-- Now use an explicit combine call on non-SW
WITH cte AS (
  SELECT combine(count) FROM cte0
)
SELECT * FROM cte;

-- Now use an explicit combine call on non-SW
WITH cte AS (
  SELECT combine(count) FROM cte1
)
SELECT * FROM cte;

DROP FOREIGN TABLE cte_s CASCADE;

-- Verify combine works within targetlist entries whose expressions are parenthesized queries
CREATE FOREIGN TABLE expr_s (x numeric) SERVER pipelinedb;

CREATE VIEW expr0 AS
 SELECT x, avg(x), count(*) FROM expr_s GROUP BY x;

CREATE VIEW expr1 WITH (sw = '1 day') AS
 SELECT x, avg(x), count(*) FROM expr_s GROUP BY x;

INSERT INTO expr_s (x) SELECT x % 10 FROM generate_series(1, 1000) x;

SELECT (SELECT count(count) FROM expr0) AS v;
SELECT (SELECT count(count) FROM expr1) AS v;
SELECT (SELECT combine(avg) FROM expr0) AS v;
SELECT (SELECT combine(avg) FROM expr1) AS v;

-- Now verify we combine calls are properly compiled when used in WHERE and HAVING clauses
CREATE TABLE expr_t AS SELECT generate_series(1, 10) x;

SELECT * FROM expr_t WHERE x = (SELECT count(*) - 1 FROM expr1);
SELECT x % 2 AS g, count(*) FROM expr_t GROUP BY g HAVING count(*) > (SELECT combine(avg) FROM expr0) ORDER BY g;

DROP FOREIGN TABLE expr_s CASCADE;
DROP TABLE expr_t;
