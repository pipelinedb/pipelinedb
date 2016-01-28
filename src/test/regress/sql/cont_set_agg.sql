CREATE CONTINUOUS VIEW test_set_agg0 AS SELECT array_length(set_agg(x::integer), 1), exact_count_distinct(x) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg1 AS SELECT array_length(set_agg(y::float8), 1), exact_count_distinct(y) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg2 AS SELECT array_length(set_agg(z::numeric), 1), exact_count_distinct(z) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg3 AS SELECT array_length(set_agg(t::text), 1), exact_count_distinct(t) FROM test_set_agg_stream;

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

DROP CONTINUOUS VIEW test_set_agg0;
DROP CONTINUOUS VIEW test_set_agg1;
DROP CONTINUOUS VIEW test_set_agg2;
DROP CONTINUOUS VIEW test_set_agg3;

CREATE CONTINUOUS VIEW test_set_agg4 AS SELECT array_agg(DISTINCT x::int), set_agg(x::int) FROM test_set_agg_stream;
INSERT INTO test_set_agg_stream (x) VALUES (1), (2), (3);
INSERT INTO test_set_agg_stream (x) VALUES (1), (2), (4);

SELECT * FROM test_set_agg4;
\d+ test_set_agg4
DROP CONTINUOUS VIEW test_set_agg4;

-- Check for NULLs
CREATE CONTINUOUS VIEW test_set_agg5 AS SELECT set_agg(x::text) FROM test_set_agg_stream;

INSERT INTO test_set_agg_stream (x) VALUES ('a'), (1), (NULL);
INSERT INTO test_set_agg_stream (x) VALUES ('a'), (1), (NULL);

SELECT * FROM test_set_agg5;

DROP CONTINUOUS VIEW test_set_agg5;
