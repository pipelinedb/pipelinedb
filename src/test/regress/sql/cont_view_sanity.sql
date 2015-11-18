DROP SERVER pipeline_streams;
DROP FOREIGN DATA WRAPPER stream_fdw;

CREATE CONTINUOUS VIEW test_avg AS SELECT key::text, avg(value::float8) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 1), ('x', 2), ('y', 100);

SELECT * FROM test_avg ORDER BY key;
SELECT * FROM test_avg_mrel ORDER BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM test_avg ORDER BY key;
SELECT * FROM test_avg_mrel ORDER BY key;

CREATE CONTINUOUS VIEW cv AS SELECT key::text, COUNT(*), MAX(x::integer + y::integer) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, x, y) VALUES ('x', -1000, 1000), ('x', 0, 1), ('x', 1, 0);
INSERT INTO sanity_stream (key, x, y) VALUES ('y', 10, 20), ('y', 20, 30), ('y', 1, 200), ('y', 1, 200), ('y', 1, 200);
INSERT INTO sanity_stream (key, x, y) VALUES ('z', -1000, 1001);

SELECT * FROM cv ORDER BY key;

CREATE CONTINUOUS VIEW cv_weird_tl AS SELECT COUNT(*), key::text, SUM(value::integer) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM cv_weird_tl ORDER BY key;

CREATE CONTINUOUS VIEW cv_no_grp AS SELECT COUNT(*), SUM(value::integer) FROM sanity_stream;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM cv_no_grp;

CREATE CONTINUOUS VIEW cv_grp_expr AS SELECT COUNT(*), substring(key::text, 1, 2) AS s FROM sanity_stream GROUP BY s;

INSERT INTO sanity_stream (key) VALUES ('aab'), ('aba'), ('aaa'), ('cab');

SELECT * FROM cv_grp_expr ORDER BY s;

CREATE CONTINUOUS VIEW cv_multi_grp AS SELECT a, b, COUNT(*) FROM sanity_stream GROUP BY a::integer, b::integer;

INSERT INTO sanity_stream (a, b) VALUES (1, 1), (1, 1), (1, 2), (2, 2), (2, 1);

SELECT * FROM cv_multi_grp ORDER BY a, b;

CREATE CONTINUOUS VIEW cv_agg_expr AS SELECT k::text, COUNT(*) + SUM(v::int) FROM sanity_stream GROUP BY k;

INSERT INTO sanity_stream (k, v) VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 4), ('b', 5);

SELECT * FROM cv_agg_expr ORDER BY k;

INSERT INTO sanity_stream (k, v) VALUES ('a', 1), ('a', 2), ('b', 3);

SELECT * FROM cv_agg_expr ORDER BY k;

CREATE CONTINUOUS VIEW test_null_group AS SELECT x::int, y::int FROM sanity_stream GROUP BY x, y;

INSERT INTO sanity_stream (z) VALUES (1);
INSERT INTO sanity_stream (z) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (y) VALUES (1);
INSERT INTO sanity_stream (y) VALUES (1);

SELECT * FROM test_null_group;

DROP CONTINUOUS VIEW test_avg;
DROP CONTINUOUS VIEW cv;
DROP CONTINUOUS VIEW cv_weird_tl;
DROP CONTINUOUS VIEW cv_no_grp;
DROP CONTINUOUS VIEW cv_grp_expr;
DROP CONTINUOUS VIEW cv_multi_grp;
DROP CONTINUOUS VIEW cv_agg_expr;
DROP CONTINUOUS VIEW test_null_group;
