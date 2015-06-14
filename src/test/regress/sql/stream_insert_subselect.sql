CREATE SCHEMA test_stream_insert_subselect;
SET search_path TO test_stream_insert_subselect,public;

CREATE TABLE t (x integer);
INSERT INTO t (SELECT generate_series(1, 100));

CREATE CONTINUOUS VIEW v0 AS SELECT x::integer FROM stream;

INSERT INTO stream (x) (SELECT * FROM t);
INSERT INTO stream (x) (SELECT * FROM (SELECT x AS y FROM t) s0);

SELECT COUNT(DISTINCT x) FROM v0;

CREATE CONTINUOUS VIEW v1 AS SELECT x::integer, COUNT(*) FROM stream GROUP BY x;
INSERT INTO stream (x) (SELECT generate_series(1, 1000));

SELECT COUNT(DISTINCT x) FROM v1;

-- It's not possible to SELECT from another stream in a stream INSERT
CREATE STREAM s0 (x integer);
INSERT INTO stream (x) (SELECT x FROM s0);

CREATE CONTINUOUS VIEW v2 AS SELECT x::integer FROM stream;
INSERT INTO stream (x) (SELECT generate_series(1, 1000) AS x ORDER BY random());

SELECT COUNT(DISTINCT x) FROM v2;

CREATE CONTINUOUS VIEW v3 AS SELECT x::integer FROM stream;
INSERT INTO stream (x) (SELECT * FROM t WHERE x IN (SELECT generate_series(1, 20)));

SELECT * FROM v3 ORDER BY x;

CREATE CONTINUOUS VIEW v4 AS SELECT COUNT(*) FROM stream;
INSERT INTO stream (price, t) SELECT 10 + random() AS price, current_timestamp - interval '1 minute' * random() AS t FROM generate_series(1, 1000);

SELECT * FROM v4;

DROP SCHEMA test_stream_insert_subselect CASCADE;
