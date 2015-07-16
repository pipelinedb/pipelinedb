CREATE TYPE test_cont_complex_type AS (
  x int,
  y int,
  z text
);

CREATE CONTINUOUS VIEW test_cont_complex1 AS SELECT bloom_agg(r::test_cont_complex_type) FROM cont_complex_stream;

INSERT INTO cont_complex_stream (r) VALUES ((1, 1, 'hello'));
INSERT INTO cont_complex_stream (r) VALUES ((1, 2, 'world')::test_cont_complex_type);

SELECT bloom_cardinality(bloom_agg) FROM test_cont_complex1;

-- These shouldn't change the cardinality
INSERT INTO cont_complex_stream (r) VALUES ((1, 1, 'hello'));
INSERT INTO cont_complex_stream (r) VALUES ((1, 2, 'world'));
INSERT INTO cont_complex_stream (r) VALUES ((1, 1, 'hello')::test_cont_complex_type);
INSERT INTO cont_complex_stream (r) VALUES ((1, 2, 'world')::test_cont_complex_type);

SELECT bloom_cardinality(bloom_agg) FROM test_cont_complex1;

-- Null check
INSERT INTO cont_complex_stream (r) VALUES (NULL);

SELECT bloom_cardinality(bloom_agg) FROM test_cont_complex1;

-- These should be false
SELECT bloom_contains(bloom_agg, (1, 1, 'hello')) FROM test_cont_complex1;
SELECT bloom_contains(bloom_agg, (1, 2, 'world')) FROM test_cont_complex1;
-- These should be true
SELECT bloom_contains(bloom_agg, (1, 1, 'hello')::test_cont_complex_type) FROM test_cont_complex1;
SELECT bloom_contains(bloom_agg, (1, 2, 'world')::test_cont_complex_type) FROM test_cont_complex1;

CREATE CONTINUOUS VIEW test_cont_complex2 AS SELECT bloom_agg((x::int, y::int)) FROM cont_complex_stream;
CREATE CONTINUOUS VIEW test_cont_complex3 AS SELECT bloom_agg((x::int, y::int, z::text)) FROM cont_complex_stream;

INSERT INTO cont_complex_stream (x, y, z) VALUES (1, 1, 'hello');
INSERT INTO cont_complex_stream (x, y, z) VALUES (1, 2, 'world');

SELECT bloom_cardinality(bloom_agg) FROM test_cont_complex2;
SELECT bloom_cardinality(bloom_agg) FROM test_cont_complex3;

-- All should be true
SELECT bloom_contains(bloom_agg, (1, 1)) FROM test_cont_complex2;
SELECT bloom_contains(bloom_agg, (1, 2)) FROM test_cont_complex2;
SELECT bloom_contains(bloom_agg, (1, 1, 'hello')) FROM test_cont_complex3;
SELECT bloom_contains(bloom_agg, (1, 2, 'world')) FROM test_cont_complex3;

DROP CONTINUOUS VIEW test_cont_complex1;
DROP CONTINUOUS VIEW test_cont_complex2;
DROP CONTINUOUS VIEW test_cont_complex3;
DROP TYPE test_cont_complex_type;
