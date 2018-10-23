CREATE FOREIGN TABLE test_set_agg_stream (x int, y float8, z numeric, t text) SERVER pipelinedb;

CREATE VIEW test_set_agg0 AS SELECT array_length(set_agg(x::integer), 1), exact_count_distinct(x) FROM test_set_agg_stream;
CREATE VIEW test_set_agg1 AS SELECT array_length(set_agg(y::float8), 1), exact_count_distinct(y) FROM test_set_agg_stream;
CREATE VIEW test_set_agg2 AS SELECT array_length(set_agg(z::numeric), 1), exact_count_distinct(z) FROM test_set_agg_stream;
CREATE VIEW test_set_agg3 AS SELECT array_length(set_agg(t::text), 1), exact_count_distinct(t) FROM test_set_agg_stream;

INSERT INTO test_set_agg_stream (x, y, z, t)
	SELECT x, x::float8 + 0.1 AS y, x::numeric AS z, x::text AS t FROM generate_series(1, 1000) AS x;
INSERT INTO test_set_agg_stream (x, y, z, t)
	SELECT x, x::float8 + 0.1 AS y, x::numeric AS z, x::text AS t FROM generate_series(1, 1000) AS x;

SELECT * FROM test_set_agg0;
SELECT * FROM test_set_agg1;
SELECT * FROM test_set_agg2;
SELECT * FROM test_set_agg3;

INSERT INTO test_set_agg_stream (x, y, z, t)
	SELECT x, x::float8 + 0.1 AS y, x::numeric AS z, x::text AS t FROM generate_series(1, 1001) AS x;

SELECT * FROM test_set_agg0;
SELECT * FROM test_set_agg1;
SELECT * FROM test_set_agg2;
SELECT * FROM test_set_agg3;

-- Composite types not supported yet
SELECT (set_agg((1, 1)));

DROP VIEW test_set_agg0;
DROP VIEW test_set_agg1;
DROP VIEW test_set_agg2;
DROP VIEW test_set_agg3;

CREATE VIEW test_set_agg4 AS SELECT array_agg(DISTINCT x::int), set_agg(x::int) FROM test_set_agg_stream;
INSERT INTO test_set_agg_stream (x) VALUES (1), (2), (3);
INSERT INTO test_set_agg_stream (x) VALUES (1), (2), (4);

SELECT * FROM test_set_agg4;
SELECT pg_get_viewdef('test_set_agg4');
DROP VIEW test_set_agg4;

-- Check for NULLs
CREATE VIEW test_set_agg5 AS SELECT set_agg(t) FROM test_set_agg_stream;

INSERT INTO test_set_agg_stream (t) VALUES ('a'), (1), (NULL);
INSERT INTO test_set_agg_stream (t) VALUES ('a'), (1), (NULL);

SELECT * FROM test_set_agg5;

DROP VIEW test_set_agg5;

CREATE VIEW test_set_agg6 AS SELECT x, set_agg(t) FROM test_set_agg_stream GROUP BY x;

INSERT INTO test_set_agg_stream (x, t) VALUES (0, 'x'), (0, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'), (0, 'z');
INSERT INTO test_set_agg_stream (x, t) VALUES (0, 'x'), (1, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'), (2, 'z'), (0, '0000000000000000'), (3, '000000000000000');

SELECT x, unnest(set_agg) FROM test_set_agg6 ORDER BY x, unnest;
-- Disabled until #93
-- SELECT x, unnest(combine(set_agg)) FROM test_set_agg6 GROUP BY x ORDER BY x, unnest;
-- SELECT unnest(combine(set_agg)) FROM test_set_agg6 ORDER BY unnest;

INSERT INTO test_set_agg_stream (x, t) VALUES (0, 'x'), (0, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'), (0, 'z');
INSERT INTO test_set_agg_stream (x, t) VALUES (0, 'x'), (1, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'), (2, 'z'), (0, '0000000000000000'), (3, '000000000000000');

SELECT x, unnest(set_agg) FROM test_set_agg6 ORDER BY x, unnest;
-- Disabled until #93
-- SELECT x, unnest(combine(set_agg)) FROM test_set_agg6 GROUP BY x ORDER BY x, unnest;
-- SELECT unnest(combine(set_agg)) FROM test_set_agg6 ORDER BY unnest;

DROP VIEW test_set_agg6;
DROP FOREIGN TABLE test_set_agg_stream CASCADE;