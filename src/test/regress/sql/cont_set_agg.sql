CREATE CONTINUOUS VIEW test_set_agg0 AS SELECT set_cardinality(set_agg(x::integer)), exact_count_distinct(x) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg1 AS SELECT set_cardinality(set_agg(y::float8)), exact_count_distinct(y) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg2 AS SELECT set_cardinality(set_agg(z::numeric)), exact_count_distinct(z) FROM test_set_agg_stream;
CREATE CONTINUOUS VIEW test_set_agg3 AS SELECT set_cardinality(set_agg(t::text)), exact_count_distinct(t) FROM test_set_agg_stream;

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


