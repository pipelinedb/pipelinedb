CREATE CONTINUOUS VIEW test_fss_agg0 AS SELECT k::text, fss_agg(x::int, 3) FROM test_fss_agg_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_fss_agg1 AS SELECT k::text, fss_agg(x::float8, 3) FROM test_fss_agg_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_fss_agg2 AS SELECT k::text, fss_agg(k::text, 3) FROM test_fss_agg_stream GROUP BY k;

INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 2);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 3);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 2);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 4.0);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 5.0);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 3.0);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 3.0);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 4.0);
INSERT INTO test_fss_agg_stream (k, x) VALUES ('b', 3.0);

SELECT k, fss_topk(fss_agg) FROM test_fss_agg0 ORDER BY k;
SELECT k, fss_topk(fss_agg) FROM test_fss_agg1 ORDER BY k;

SELECT k, fss_topk_values(fss_agg) FROM test_fss_agg0 ORDER BY k;
SELECT k, fss_topk_freqs(fss_agg) FROM test_fss_agg0 ORDER BY k;

SELECT fss_topk(combine(fss_agg)) FROM test_fss_agg0;
SELECT fss_topk(combine(fss_agg)) FROM test_fss_agg1;

DROP CONTINUOUS VIEW test_fss_agg0;
DROP CONTINUOUS VIEW test_fss_agg1;

CREATE CONTINUOUS VIEW test_fss_agg3 AS SELECT fss_agg_weighted(x::integer, 3, y::integer) FROM test_fss_agg_stream;

INSERT INTO test_fss_agg_stream (x, y) VALUES (0, 10);
INSERT INTO test_fss_agg_stream (x, y) VALUES (0, 10);
SELECT fss_topk(fss_agg_weighted) FROM test_fss_agg3;

INSERT INTO test_fss_agg_stream (x, y) VALUES (2, 1);
INSERT INTO test_fss_agg_stream (x, y) VALUES (2, 1);
SELECT fss_topk(fss_agg_weighted) FROM test_fss_agg3;

INSERT INTO test_fss_agg_stream (x, y) VALUES (3, 1);
INSERT INTO test_fss_agg_stream (x, y) VALUES (3, 1);
SELECT fss_topk(fss_agg_weighted) FROM test_fss_agg3;

INSERT INTO test_fss_agg_stream (x, y) VALUES (4, 1);
INSERT INTO test_fss_agg_stream (x, y) VALUES (4, 10);
SELECT fss_topk(fss_agg_weighted) FROM test_fss_agg3;

INSERT INTO test_fss_agg_stream (x, y) VALUES (5, 500);
INSERT INTO test_fss_agg_stream (x, y) VALUES (6, 1000);
INSERT INTO test_fss_agg_stream (x, y) VALUES (7, 10000);

SELECT fss_topk(fss_agg_weighted) FROM test_fss_agg3;
DROP CONTINUOUS VIEW test_fss_agg3;
