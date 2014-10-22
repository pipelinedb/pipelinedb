CREATE CONTINUOUS VIEW test_avg AS SELECT key::text, avg(value::float8) FROM stream GROUP BY key;
ACTIVATE test_avg;

SET debug_sync_stream_insert = 'on';
INSERT INTO stream (key, value) VALUES ('x', 1), ('x', 2), ('y', 100);

DEACTIVATE test_avg;

SELECT * FROM test_avg;
SELECT * FROM test_avg_pdb;

ACTIVATE test_avg;

INSERT INTO stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

DEACTIVATE test_avg;

SELECT * FROM test_avg;
SELECT * FROM test_avg_pdb;

CREATE CONTINUOUS VIEW cv AS SELECT key::text, COUNT(*), MAX(x::integer + y::integer) FROM stream GROUP BY key;

ACTIVATE cv;

INSERT INTO stream (key, x, y) VALUES ('x', -1000, 1000), ('x', 0, 1), ('x', 1, 0);
INSERT INTO stream (key, x, y) VALUES ('y', 10, 20), ('y', 20, 30), ('y', 1, 200), ('y', 1, 200), ('y', 1, 200);
INSERT INTO stream (key, x, y) VALUES ('z', -1000, 1001);

DEACTIVATE cv;

SELECT * FROM cv;

CREATE CONTINUOUS VIEW cv_weird_tl AS SELECT COUNT(*), key::text, SUM(value::integer) FROM stream GROUP BY key;

ACTIVATE cv_weird_tl;

INSERT INTO stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

DEACTIVATE cv_weird_tl;

SELECT * FROM cv_weird_tl;

CREATE CONTINUOUS VIEW cv_no_grp AS SELECT COUNT(*), SUM(value::integer) FROM stream;

ACTIVATE cv_no_grp;

INSERT INTO stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

DEACTIVATE cv_no_grp;

SELECT * FROM cv_no_grp;

CREATE CONTINUOUS VIEW cv_grp_expr AS SELECT COUNT(*), substring(key::text, 1, 2) AS s FROM stream GROUP BY s;

ACTIVATE cv_grp_expr;

INSERT INTO stream (key) VALUES ('aab'), ('aba'), ('aaa'), ('cab');

DEACTIVATE cv_grp_expr;

SELECT * FROM cv_grp_expr;

CREATE CONTINUOUS VIEW cv_multi_grp AS SELECT a, b, COUNT(*) FROM stream GROUP BY a::integer, b::integer;

ACTIVATE cv_multi_grp;

INSERT INTO stream (a, b) VALUES (1, 1), (1, 1), (1, 2), (2, 2), (2, 1);

DEACTIVATE cv_multi_grp;

SELECT * FROM cv_multi_grp;

DROP CONTINUOUS VIEW test_avg;
DROP CONTINUOUS VIEW cv;
DROP CONTINUOUS VIEW cv_weird_tl;
DROP CONTINUOUS VIEW cv_no_grp;
DROP CONTINUOUS VIEW cv_grp_expr;
DROP CONTINUOUS VIEW cv_multi_grp;
