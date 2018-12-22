\d+ pipelinedb.cont_query

DROP SERVER pipelinedb;
DROP FOREIGN DATA WRAPPER stream_fdw;

CREATE FOREIGN TABLE sanity_stream (key text, value float8, x int, y int, z int, a int, b int, k text, v int) SERVER pipelinedb;

CREATE VIEW test_avg AS SELECT key, avg(value) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 1), ('x', 2), ('y', 100);

SELECT * FROM test_avg ORDER BY key;
SELECT key, avg FROM test_avg_mrel ORDER BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM test_avg ORDER BY key;
SELECT key, avg FROM test_avg_mrel ORDER BY key;

CREATE VIEW cv AS SELECT key, COUNT(*), MAX(x + y) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, x, y) VALUES ('x', -1000, 1000), ('x', 0, 1), ('x', 1, 0);
INSERT INTO sanity_stream (key, x, y) VALUES ('y', 10, 20), ('y', 20, 30), ('y', 1, 200), ('y', 1, 200), ('y', 1, 200);
INSERT INTO sanity_stream (key, x, y) VALUES ('z', -1000, 1001);

SELECT * FROM cv ORDER BY key;

CREATE VIEW cv_weird_tl AS SELECT COUNT(*), key, SUM(value) FROM sanity_stream GROUP BY key;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM cv_weird_tl ORDER BY key;

CREATE VIEW cv_no_grp AS SELECT COUNT(*), SUM(value) FROM sanity_stream;

INSERT INTO sanity_stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM cv_no_grp;

CREATE VIEW cv_grp_expr AS SELECT COUNT(*), substring(key, 1, 2) AS s FROM sanity_stream GROUP BY s;

INSERT INTO sanity_stream (key) VALUES ('aab'), ('aba'), ('aaa'), ('cab');

SELECT * FROM cv_grp_expr ORDER BY s;

CREATE VIEW cv_multi_grp AS SELECT a, b, COUNT(*) FROM sanity_stream GROUP BY a, b;

INSERT INTO sanity_stream (a, b) VALUES (1, 1), (1, 1), (1, 2), (2, 2), (2, 1);

SELECT * FROM cv_multi_grp ORDER BY a, b;

CREATE VIEW cv_agg_expr AS SELECT k, COUNT(*) + SUM(v) FROM sanity_stream GROUP BY k;

INSERT INTO sanity_stream (k, v) VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 4), ('b', 5);

SELECT * FROM cv_agg_expr ORDER BY k;

INSERT INTO sanity_stream (k, v) VALUES ('a', 1), ('a', 2), ('b', 3);

SELECT * FROM cv_agg_expr ORDER BY k;

CREATE VIEW test_null_group AS SELECT x, y FROM sanity_stream GROUP BY x, y;

INSERT INTO sanity_stream (z) VALUES (1);
INSERT INTO sanity_stream (z) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (y) VALUES (1);
INSERT INTO sanity_stream (y) VALUES (1);

SELECT * FROM test_null_group;

CREATE VIEW pk AS SELECT k AS "$pk" FROM sanity_stream;

CREATE VIEW tl_expr AS SELECT sum(x) + count(*) FROM sanity_stream;
CREATE VIEW tl_expr_group0 AS SELECT count(*) + 1 AS expr FROM sanity_stream GROUP BY x;
CREATE VIEW tl_expr_group1 AS SELECT x, sum(x) + count(*) FROM sanity_stream GROUP BY x;

INSERT INTO sanity_stream (x) VALUES (0);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (2);

SELECT * FROM tl_expr;
SELECT * FROM tl_expr_group0 ORDER BY expr;
SELECT * FROM tl_expr_group1 ORDER BY x;

INSERT INTO sanity_stream (x) VALUES (0);
INSERT INTO sanity_stream (x) VALUES (1);
INSERT INTO sanity_stream (x) VALUES (2);

SELECT * FROM tl_expr;
SELECT * FROM tl_expr_group0 ORDER BY expr;
SELECT * FROM tl_expr_group1 ORDER BY x;

CREATE VIEW overlay0 AS SELECT x % 10 AS g, ceil(round(avg(x::numeric))) + floor(round(avg(x::numeric))) FROM sanity_stream GROUP BY g;

INSERT INTO sanity_stream (x) SELECT generate_series(1, 100) x;

SELECT * FROM overlay0 ORDER BY g;

DROP FOREIGN TABLE sanity_stream CASCADE;
