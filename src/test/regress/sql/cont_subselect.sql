CREATE SCHEMA test_cont_subselect;
SET search_path TO test_cont_subselect,public;

-- Disallowed subselects
CREATE CONTINUOUS VIEW v0 AS SELECT x FROM (SELECT COUNT(*) FROM stream) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT COUNT(*) OVER (PARTITION BY key::text) FROM stream) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT x::integer, y::integer FROM stream GROUP BY x, y) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT COUNT(*) FROM stream HAVING COUNT(*) = 1) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT x::integer FROM stream ORDER BY x) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT DISTINCT x::integer FROM stream) _;

CREATE CONTINUOUS VIEW v0 AS SELECT x FROM
	(SELECT x::integer FROM stream LIMIT 10 OFFSET 2) _;

-- Simple stuff
CREATE CONTINUOUS VIEW v0 AS SELECT COUNT(*) FROM
	(SELECT x::integer FROM stream WHERE x < 0) _;
INSERT INTO stream (x) (SELECT generate_series(-100, 999));

SELECT * FROM v0;

CREATE CONTINUOUS VIEW v1 AS SELECT x, sum(y) FROM
	(SELECT x::integer, y::integer FROM stream) _ GROUP by x;
INSERT INTO stream (x, y) (SELECT x % 10, -x AS y FROM generate_series(1, 1000) AS x);

SELECT * FROM v1 ORDER BY x;

-- JSON unrolling
CREATE CONTINUOUS VIEW v2 AS SELECT (element->>'k')::integer AS value FROM
	(SELECT json_array_elements(data::json) AS element FROM stream) _;
INSERT INTO stream (data) (SELECT '[{"k": 1}, {"k": 1}, {"k": 1}]'::json FROM generate_series(1, 1000));

SELECT sum(value) FROM v2;

-- Column renames between subquery levels
CREATE CONTINUOUS VIEW v3 AS SELECT a0, c0, b0 FROM
	(SELECT b1 AS b0, a1 AS a0, c1 AS c0 FROM
		(SELECT c2 AS c1, b2 AS b1, a2 AS a1 FROM
			(SELECT a2::text, b2::text, c2::text FROM stream) s0) s1) s2;
INSERT INTO stream (a2, b2, c2) (SELECT x AS a2, x AS b2, x AS c2 FROM generate_series(1, 1000) AS x);

SELECT COUNT(DISTINCT(a0, b0, c0)) FROM v3;

-- References to function calls in inner query
CREATE CONTINUOUS VIEW v4 AS SELECT day, upper FROM
	(SELECT day(arrival_timestamp), upper(s::text) FROM stream) _;
INSERT INTO stream (s) (SELECT s::text FROM generate_series(1, 1000) AS s);

SELECT COUNT(DISTINCT(day, upper)) FROM v4;

-- Deep nesting
CREATE CONTINUOUS VIEW v5 AS SELECT x, COUNT(*) FROM
	(SELECT x FROM
		(SELECT x FROM
			(SELECT x FROM
				(SELECT x FROM
					(SELECT x FROM
						(SELECT x FROM
							(SELECT x FROM
								(SELECT x FROM
									(SELECT x FROM
										(SELECT x::integer FROM stream) s0) s1) s2) s3) s4) s5) s6) s7) s8) s9
	GROUP BY x;
INSERT INTO stream (x) (SELECT x % 10 FROM generate_series(1, 1000) AS x);

SELECT * FROM v5 ORDER BY x;

DROP SCHEMA test_cont_subselect CASCADE;

-- Stream-table joins in subselects
CREATE TABLE test_cont_sub_t0 (x integer, y integer);
INSERT INTO test_cont_sub_t0 (x, y) VALUES (0, 0);
INSERT INTO test_cont_sub_t0 (x, y) VALUES (1, 1);
INSERT INTO test_cont_sub_t0 (x, y) VALUES (2, 2);

CREATE STREAM test_cont_sub_s0 (x integer, y integer);
CREATE CONTINUOUS VIEW test_cont_sub_v6 AS SELECT COUNT(*) FROM
	(SELECT s0.x, s0.y, t0.y FROM test_cont_sub_s0 s0 JOIN test_cont_sub_t0 t0 ON s0.x = t0.x) _;
INSERT INTO test_cont_sub_s0 (x, y) VALUES (0, 0);
INSERT INTO test_cont_sub_s0 (x, y) VALUES (0, 0);
INSERT INTO test_cont_sub_s0 (x, y) VALUES (1, 1);
INSERT INTO test_cont_sub_s0 (x, y) VALUES (1, 1);
INSERT INTO test_cont_sub_s0 (x, y) (SELECT x, x AS y FROM generate_series(1, 100) AS x);

SELECT * FROM test_cont_sub_v6;

DROP CONTINUOUS VIEW test_cont_sub_v6;
DROP TABLE test_cont_sub_t0;
DROP STREAM test_cont_sub_s0;
