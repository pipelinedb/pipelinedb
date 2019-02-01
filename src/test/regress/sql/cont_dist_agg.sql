CREATE FOREIGN TABLE cont_dist_agg_stream (x integer) SERVER pipelinedb;

CREATE VIEW test_dist_agg0 AS
  SELECT x % 10 AS g, dist_agg(x)
 FROM cont_dist_agg_stream
GROUP BY g;

CREATE VIEW test_dist_agg1 AS
  SELECT x % 10 AS g, dist_agg(x)
 FROM cont_dist_agg_stream
GROUP BY g;

INSERT INTO cont_dist_agg_stream (x) SELECT x FROM generate_series(1, 1000) x;

SELECT g, dist_cdf(dist_agg, 50) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 500) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 10) FROM test_dist_agg1 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 900) FROM test_dist_agg1 ORDER BY g;

SELECT g, dist_quantile(dist_agg, 0.1) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.25) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.50) FROM test_dist_agg1 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.99) FROM test_dist_agg1 ORDER BY g;

INSERT INTO cont_dist_agg_stream (x) SELECT x FROM generate_series(1000, 5000) x;

SELECT g, dist_cdf(dist_agg, 50) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 500) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 10) FROM test_dist_agg1 ORDER BY g;
SELECT g, dist_cdf(dist_agg, 900) FROM test_dist_agg1 ORDER BY g;

SELECT g, dist_quantile(dist_agg, 0.1) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.25) FROM test_dist_agg0 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.50) FROM test_dist_agg1 ORDER BY g;
SELECT g, dist_quantile(dist_agg, 0.99) FROM test_dist_agg1 ORDER BY g;

CREATE VIEW empty_regress WITH (sw = '1 day') AS
 SELECT dist_agg(x) FILTER (WHERE x > 30) FROM cont_dist_agg_stream;

SELECT * FROM empty_regress;

DROP FOREIGN TABLE cont_dist_agg_stream CASCADE;
