CREATE FOREIGN TABLE test_topk_agg_stream (x float8, y int, k text, t text) SERVER pipelinedb;

CREATE VIEW test_topk_agg0 AS SELECT k::text, topk_agg(x::int, 3) FROM test_topk_agg_stream GROUP BY k;
CREATE VIEW test_topk_agg1 AS SELECT k::text, topk_agg(x::float8, 3) FROM test_topk_agg_stream GROUP BY k;
CREATE VIEW test_topk_agg2 AS SELECT k::text, topk_agg(k::text, 3) FROM test_topk_agg_stream GROUP BY k;

INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 2);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 3);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 2);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('a', 1);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 4.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 5.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 3.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 3.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 4.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 4.0);
INSERT INTO test_topk_agg_stream (k, x) VALUES ('b', 3.0);

SELECT k, topk(topk_agg) FROM test_topk_agg0 ORDER BY k;
SELECT k, topk(topk_agg) FROM test_topk_agg1 ORDER BY k;

SELECT k, topk_values(topk_agg) FROM test_topk_agg0 ORDER BY k;
SELECT k, topk_freqs(topk_agg) FROM test_topk_agg0 ORDER BY k;

SELECT topk(combine(topk_agg)) FROM test_topk_agg0;
SELECT topk(combine(topk_agg)) FROM test_topk_agg1;

DROP VIEW test_topk_agg0;
DROP VIEW test_topk_agg1;
DROP VIEW test_topk_agg2;

CREATE VIEW test_topk_agg3 AS SELECT topk_agg(x::integer, 3, y::integer) AS topk_agg_weighted FROM test_topk_agg_stream;

INSERT INTO test_topk_agg_stream (x, y) VALUES (0, 10);
INSERT INTO test_topk_agg_stream (x, y) VALUES (0, 10);
SELECT topk(topk_agg_weighted) FROM test_topk_agg3;

INSERT INTO test_topk_agg_stream (x, y) VALUES (2, 1);
INSERT INTO test_topk_agg_stream (x, y) VALUES (2, 1);
SELECT topk(topk_agg_weighted) FROM test_topk_agg3;

INSERT INTO test_topk_agg_stream (x, y) VALUES (3, 1);
INSERT INTO test_topk_agg_stream (x, y) VALUES (3, 1);
SELECT topk(topk_agg_weighted) FROM test_topk_agg3;

INSERT INTO test_topk_agg_stream (x, y) VALUES (4, 1);
INSERT INTO test_topk_agg_stream (x, y) VALUES (4, 10);
SELECT topk(topk_agg_weighted) FROM test_topk_agg3;

INSERT INTO test_topk_agg_stream (x, y) VALUES (5, 500);
INSERT INTO test_topk_agg_stream (x, y) VALUES (6, 1000);
INSERT INTO test_topk_agg_stream (x, y) VALUES (7, 10000);
INSERT INTO test_topk_agg_stream (x, y) VALUES (8, 10000);
INSERT INTO test_topk_agg_stream (x, y) VALUES (8, 10000);

SELECT topk(topk_agg_weighted) FROM test_topk_agg3;

CREATE VIEW test_topk_agg4 AS SELECT topk_agg(t::text, 4, y::integer) AS topk_agg_weighted FROM test_topk_agg_stream;

INSERT INTO test_topk_agg_stream (t, y) VALUES ('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx', 100);
INSERT INTO test_topk_agg_stream (t, y) VALUES ('xxxx', 200);
INSERT INTO test_topk_agg_stream (t, y) VALUES ('yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy', 500);
INSERT INTO test_topk_agg_stream (t, y) VALUES ('yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy', 500);
INSERT INTO test_topk_agg_stream (t, y) VALUES ('xxxxxxxxxxxxxxxxxxxxx', 500);

SELECT topk_values(topk_agg_weighted) FROM test_topk_agg4;
SELECT topk_freqs(topk_agg_weighted) FROM test_topk_agg4;

DROP VIEW test_topk_agg3;
DROP VIEW test_topk_agg4;

CREATE VIEW test_topk_agg5 AS SELECT topk_agg(x::integer, 4) FROM test_topk_agg_stream;

INSERT INTO test_topk_agg_stream (x) VALUES (null);
INSERT INTO test_topk_agg_stream (x) VALUES (null);
INSERT INTO test_topk_agg_stream (x) VALUES (null);
INSERT INTO test_topk_agg_stream (x) VALUES (0);
INSERT INTO test_topk_agg_stream (x) VALUES (0);
INSERT INTO test_topk_agg_stream (x) VALUES (1);
INSERT INTO test_topk_agg_stream (x) VALUES (null);

SELECT topk(topk_agg) FROM test_topk_agg5;
DROP VIEW test_topk_agg5;

DROP FOREIGN TABLE test_topk_agg_stream CASCADE;
