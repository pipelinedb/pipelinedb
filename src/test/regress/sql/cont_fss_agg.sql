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

SELECT fss_topk(combine(fss_agg)) FROM test_fss_agg0;
SELECT fss_topk(combine(fss_agg)) FROM test_fss_agg1;

DROP CONTINUOUS VIEW test_fss_agg0;
DROP CONTINUOUS VIEW test_fss_agg1;
