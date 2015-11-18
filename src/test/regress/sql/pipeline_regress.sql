CREATE STREAM test_stream (
  ts TIMESTAMP (0) WITH TIME ZONE,
  foobar TEXT
);

CREATE CONTINUOUS VIEW test_view AS
  SELECT
    SECOND(ts) AS secondstamp,
    foobar,
    COUNT(*) AS count
  FROM test_stream
  WHERE (SECOND(arrival_timestamp) > clock_timestamp() - interval '7 day')
  GROUP BY
    secondstamp,
    foobar;

INSERT INTO test_stream (foobar, ts) VALUES ('foo', clock_timestamp()), ('foo', clock_timestamp());
SELECT pg_sleep(1);
INSERT INTO test_stream (foobar, ts) VALUES ('foo', clock_timestamp()), ('bar', clock_timestamp());

SELECT foobar, count FROM test_view ORDER BY secondstamp, foobar;

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream;

CREATE CONTINUOUS VIEW test_view AS
  SELECT
    user_id::int,
    page_id::int,
    arrival_timestamp as ts
  FROM test_stream
  WHERE arrival_timestamp >= ( clock_timestamp() - interval '90 minutes' );

INSERT INTO test_stream (user_id, page_id) VALUES (1, 1);
SELECT pg_sleep(1);
INSERT INTO test_stream (user_id, page_id) VALUES (2, 2);

SELECT user_id, page_id FROM test_view ORDER BY ts;

DROP CONTINUOUS VIEW test_view;

CREATE OR REPLACE FUNCTION arrival_timestamp()
RETURNS timestamptz AS
$$
BEGIN
  RETURN timestamp '2000-01-01 00:00:00';
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE CONTINUOUS VIEW test_view AS
  SELECT arrival_timestamp FROM test_stream
  WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';

INSERT INTO test_stream (x) VALUES (NULL);
INSERT INTO test_stream (x) VALUES (NULL);

SELECT COUNT(*) FROM test_view;

DROP CONTINUOUS VIEW test_view;
DROP FUNCTION arrival_timestamp();

CREATE CONTINUOUS VIEW test_view AS
  SELECT ((id::integer)::numeric + avg(id::integer)) AS x
  FROM test_stream
  GROUP BY id::integer;

INSERT INTO test_stream (id) VALUES (1), (2), (3);

SELECT * FROM test_view ORDER BY x;

DROP CONTINUOUS VIEW test_view;

CREATE CONTINUOUS VIEW test_view AS
  SELECT id::integer + avg(id) AS x, SUM(x::float8)
  FROM test_stream
  GROUP BY id;

\d+ test_view_mrel;

INSERT INTO test_stream (id, x) VALUES (1, 1.5), (2, 3.0), (3, 4.5);

SELECT * FROM test_view ORDER BY x;

DROP CONTINUOUS VIEW test_view;

CREATE CONTINUOUS VIEW test_view AS
SELECT
  second(arrival_timestamp),
  COUNT(*)
FROM
  stream
WHERE
  (minute(arrival_timestamp) > clock_timestamp() - interval '10 minute')
GROUP BY second;

INSERT INTO stream (x) VALUES (1), (1);
SELECT pg_sleep(1);
INSERT INTO stream (x) VALUES (1), (1);

SELECT count FROM test_view_mrel;
SELECT count FROM test_view;

DROP CONTINUOUS VIEW test_view;

CREATE CONTINUOUS VIEW test_view AS
  SELECT uid::bigint, COUNT(*)
FROM
  stream
GROUP BY uid;

-- Ensure that hashes colide.
SELECT hash_group(13362), hash_group(41950);

INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (41950);
INSERT INTO stream (uid) VALUES (41950);
INSERT INTO stream (uid) VALUES (41950);
INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (13362);
INSERT INTO stream (uid) VALUES (41950);
INSERT INTO stream (uid) VALUES (41950);
INSERT INTO stream (uid) VALUES (41950);

SELECT * FROM test_view ORDER BY uid;

CREATE CONTINUOUS VIEW v AS SELECT array_agg(ARRAY[x::int, y::int]) FROM stream;
