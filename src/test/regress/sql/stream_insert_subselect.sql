CREATE TABLE stream_subselect_t (x integer);
INSERT INTO stream_subselect_t (SELECT generate_series(1, 100));

CREATE CONTINUOUS VIEW stream_subselect_v0 AS SELECT x::integer FROM stream_subselect_stream;

INSERT INTO stream_subselect_stream (x) (SELECT * FROM stream_subselect_t);
INSERT INTO stream_subselect_stream (x) (SELECT * FROM (SELECT x AS y FROM stream_subselect_t) s0);

SELECT COUNT(DISTINCT x) FROM stream_subselect_v0;

CREATE CONTINUOUS VIEW stream_subselect_v1 AS SELECT x::integer, COUNT(*) FROM stream_subselect_stream GROUP BY x;
INSERT INTO stream_subselect_stream (x) (SELECT generate_series(1, 1000));

SELECT COUNT(DISTINCT x) FROM stream_subselect_v1;

-- It's not possible to SELECT from another stream in a stream INSERT
CREATE STREAM stream_subselect_s0 (x integer);
INSERT INTO stream_subselect_stream (x) (SELECT x FROM stream_subselect_s0);

CREATE CONTINUOUS VIEW stream_subselect_v2 AS SELECT x::integer FROM stream_subselect_stream;
INSERT INTO stream_subselect_stream (x) (SELECT generate_series(1, 1000) AS x ORDER BY random());

SELECT COUNT(DISTINCT x) FROM stream_subselect_v2;

CREATE CONTINUOUS VIEW stream_subselect_v3 AS SELECT x::integer FROM stream_subselect_stream;
INSERT INTO stream_subselect_stream (x) (SELECT * FROM stream_subselect_t WHERE x IN (SELECT generate_series(1, 20)));

SELECT * FROM stream_subselect_v3 ORDER BY x;

CREATE CONTINUOUS VIEW stream_subselect_v4 AS SELECT COUNT(*) FROM stream_subselect_stream;
INSERT INTO stream_subselect_stream (price, t) SELECT 10 + random() AS price, current_timestamp - interval '1 minute' * random() AS t FROM generate_series(1, 1000);

SELECT * FROM stream_subselect_v4;

DROP CONTINUOUS VIEW stream_subselect_v0;
DROP CONTINUOUS VIEW stream_subselect_v1;
DROP CONTINUOUS VIEW stream_subselect_v2;
DROP CONTINUOUS VIEW stream_subselect_v3;
DROP CONTINUOUS VIEW stream_subselect_v4;
DROP TABLE stream_subselect_t;
