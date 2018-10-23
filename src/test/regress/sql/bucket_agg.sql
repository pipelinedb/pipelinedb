CREATE FOREIGN TABLE bucket_stream (x integer, y integer, z text) SERVER pipelinedb;

CREATE VIEW bucket0 AS SELECT bucket_agg(x, y::smallint) FROM bucket_stream;

INSERT INTO bucket_stream (x, y) VALUES (0, 0);
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card
FROM bucket0 ORDER BY bucket_id;

-- Move element 0 into bucket 1
INSERT INTO bucket_stream (x, y) VALUES (0, 1);
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card
FROM bucket0 ORDER BY bucket_id;

-- Now add a new element to the empty bucket 0
INSERT INTO bucket_stream (x, y) VALUES (1, 0);
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card
FROM bucket0 ORDER BY bucket_id;

INSERT INTO bucket_stream (x, y) SELECT generate_series(2, 10), 2;
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket0;
DROP VIEW bucket0;

CREATE VIEW bucket1 AS SELECT bucket_agg(x, y::smallint) FROM bucket_stream;

INSERT INTO bucket_stream (x, y) SELECT x, x % 10 AS y FROM generate_series(1, 10000) AS x;
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id FROM bucket1 ORDER BY bucket_id;
SELECT unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket1 ORDER BY card;

INSERT INTO bucket_stream (x, y) SELECT x % 2, 11 AS y FROM generate_series(1, 10000) AS x;
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id FROM bucket1 ORDER BY bucket_id;
SELECT unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket1 ORDER BY card;
DROP VIEW bucket1;

CREATE VIEW bucket2 AS SELECT x % 10 AS g, bucket_agg(z, y::smallint) FROM bucket_stream GROUP BY g;
INSERT INTO bucket_stream (x, y, z) SELECT x % 4, x % 10, lpad(x::text, 32, '0')  FROM generate_series(1, 10000) AS x;
SELECT g, unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card
FROM bucket2 ORDER BY g, bucket_id;

INSERT INTO bucket_stream (x, y, z) SELECT x % 4, 11, lpad((x % 2)::text, 32, '0') AS y FROM generate_series(1, 10000) AS x;
SELECT g, unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card
FROM bucket2 ORDER BY g, bucket_id;

-- Now let's run some user combines on them
-- All combine calls disabled unti #93
-- SELECT g % 2 as x, unnest(bucket_ids(combine(bucket_agg))) AS bucket_id, unnest(bucket_cardinalities(combine(bucket_agg))) AS card
-- FROM bucket2 GROUP BY x ORDER BY x, bucket_id;

-- SELECT bucket_cardinality(combine(bucket_agg), 1::smallint) FROM bucket2;
-- SELECT bucket_cardinality(combine(bucket_agg), 2::smallint) FROM bucket2;
-- SELECT bucket_cardinality(combine(bucket_agg), 4::smallint) FROM bucket2;
-- SELECT bucket_cardinality(combine(bucket_agg), 8::smallint) FROM bucket2;

INSERT INTO bucket_stream (x, y, z) VALUES (3, 8::smallint, 'unique');
-- SELECT g, unnest(bucket_ids(combine(bucket_agg))) AS bucket_id, unnest(bucket_cardinalities(combine(bucket_agg))) AS card FROM bucket2
-- GROUP BY g ORDER BY g;

INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique');
INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique1');
INSERT INTO bucket_stream (x, y, z) VALUES (9, 8::smallint, 'unique2');
-- SELECT g, unnest(bucket_ids(combine(bucket_agg))) AS bucket_id, unnest(bucket_cardinalities(combine(bucket_agg))) AS card FROM bucket2
-- GROUP BY g ORDER BY g;

DROP VIEW bucket2;
DROP FOREIGN TABLE bucket_stream;

CREATE FOREIGN TABLE bucket_stream_ts (x integer, y integer, ts timestamptz) SERVER pipelinedb;
CREATE VIEW bucket3 AS SELECT bucket_agg(x, y::smallint, ts) FROM bucket_stream_ts;

INSERT INTO bucket_stream_ts (x, y, ts) VALUES (0, 0, '2017-03-22 00:00:01');
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket3;

-- This comes before the previous bucket entry, so we should still be in bucket 0
INSERT INTO bucket_stream_ts (x, y, ts) VALUES (0, 1, '2017-03-22 00:00:00');
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket3;

INSERT INTO bucket_stream_ts (x, y, ts) VALUES (0, 2, '2017-03-22 00:00:02');
SELECT unnest(bucket_ids(bucket_agg)) AS bucket_id, unnest(bucket_cardinalities(bucket_agg)) AS card FROM bucket3;

DROP VIEW bucket3;
DROP FOREIGN TABLE bucket_stream_ts;