CREATE STREAM bucket_stream (x integer, y integer, z text);

CREATE CONTINUOUS VIEW bucket0 AS SELECT bucket_agg(x, y::smallint) FROM bucket_stream;

INSERT INTO bucket_stream (x, y) VALUES (0, 0);
SELECT bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket0;

-- Move element 0 into bucket 1
INSERT INTO bucket_stream (x, y) VALUES (0, 1);
SELECT bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket0;

-- Now add a new element to the empty bucket 0
INSERT INTO bucket_stream (x, y) VALUES (1, 0);
SELECT bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket0;

INSERT INTO bucket_stream (x, y) SELECT generate_series(2, 10), 2;
SELECT bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket0;
DROP CONTINUOUS VIEW bucket0;

CREATE CONTINUOUS VIEW bucket1 AS SELECT bucket_agg(x, y::smallint) FROM bucket_stream;

INSERT INTO bucket_stream (x, y) SELECT x, x % 10 AS y FROM generate_series(1, 10000) AS x;
SELECT bucket_ids(bucket_agg) FROM bucket1;
SELECT bucket_cardinalities(bucket_agg) FROM bucket1;

INSERT INTO bucket_stream (x, y) SELECT x % 2, 11 AS y FROM generate_series(1, 10000) AS x;
SELECT bucket_ids(bucket_agg) FROM bucket1;
SELECT bucket_cardinalities(bucket_agg) FROM bucket1;
DROP CONTINUOUS VIEW bucket1;

CREATE CONTINUOUS VIEW bucket2 AS SELECT x % 10 AS g, bucket_agg(z, y::smallint) FROM bucket_stream GROUP BY g;
INSERT INTO bucket_stream (x, y, z) SELECT x % 4, x % 10, lpad(x::text, 32, '0')  FROM generate_series(1, 10000) AS x;
SELECT g, bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket2 ORDER BY g;

INSERT INTO bucket_stream (x, y, z) SELECT x % 4, 11, lpad((x % 2)::text, 32, '0') AS y FROM generate_series(1, 10000) AS x;
SELECT g, bucket_ids(bucket_agg), bucket_cardinalities(bucket_agg) FROM bucket2 ORDER BY g;

-- Now let's run some user combines on them
SELECT g % 2 as x, bucket_ids(combine(bucket_agg)), bucket_cardinalities(combine(bucket_agg))
FROM bucket2 GROUP BY x ORDER BY x;

SELECT bucket_cardinality(combine(bucket_agg), 1::smallint) FROM bucket2;
SELECT bucket_cardinality(combine(bucket_agg), 2::smallint) FROM bucket2;
SELECT bucket_cardinality(combine(bucket_agg), 4::smallint) FROM bucket2;
SELECT bucket_cardinality(combine(bucket_agg), 8::smallint) FROM bucket2;

INSERT INTO bucket_stream (x, y, z) VALUES (3, 8::smallint, 'unique');
SELECT g, bucket_ids(combine(bucket_agg)), bucket_cardinalities(combine(bucket_agg)) FROM bucket2
GROUP BY g ORDER BY g;

INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique');
INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique1');
INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique2');
SELECT g, bucket_ids(combine(bucket_agg)), bucket_cardinalities(combine(bucket_agg)) FROM bucket2
GROUP BY g ORDER BY g;

DROP CONTINUOUS VIEW bucket2;
DROP STREAM bucket_stream;
