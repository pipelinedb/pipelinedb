CREATE STREAM delta_stream (x integer, y integer);

-- SWs don't have delta streams
CREATE CONTINUOUS VIEW delta_sw  WITH (sw = '1 hour') AS SELECT x, count(*) FROM delta_stream GROUP BY x;
CREATE CONTINUOUS VIEW delta0 AS SELECT combine((delta).count) FROM output_of('delta_sw');

-- Several delta input columns
CREATE CONTINUOUS VIEW delta1 AS SELECT x, count(*), sum(y), avg(x + y) FROM delta_stream GROUP BY x;
CREATE CONTINUOUS VIEW delta2 AS
  SELECT
    combine((delta).count) AS count,
    combine((delta).sum) AS sum,
    combine((delta).avg) AS avg
  FROM output_of('delta1');

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
SELECT * FROM delta2;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
SELECT * FROM delta2;

-- COUNT DISTINCT
CREATE CONTINUOUS VIEW delta3 AS SELECT x, COUNT(DISTINCT y) FROM delta_stream GROUP BY x;
CREATE CONTINUOUS VIEW delta4 AS SELECT combine((delta).count) AS count FROM output_of('delta3');

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
SELECT * FROM delta4;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(101, 200) AS x;
SELECT * FROM delta4;

-- Large values in delta stream
CREATE CONTINUOUS VIEW delta5 AS SELECT x, bloom_agg(y) FROM delta_stream GROUP BY x;
CREATE CONTINUOUS VIEW delta6 AS SELECT (new).x % 2 AS x, combine((delta).bloom_agg) AS bloom_agg FROM output_of('delta5') GROUP BY x;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;

SELECT x, bloom_cardinality(bloom_agg) FROM delta6 ORDER BY x;

-- User combine
SELECT bloom_cardinality(combine(bloom_agg)) FROM delta6;

-- STJs with delta streams
CREATE TABLE delta_t AS SELECT generate_series(1, 10) AS x;
CREATE CONTINUOUS VIEW delta7 AS
  SELECT
    t.x,
    combine((os.delta).count) AS count,
    combine((os.delta).sum) AS sum,
    combine((os.delta).avg) AS avg
  FROM delta1_osrel os JOIN delta_t t ON (os.delta).x = t.x
  GROUP BY t.x;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;

-- User combine
SELECT combine(count) AS count, combine(sum) AS sum, combine(avg) AS avg FROM delta7;

DROP STREAM delta_stream CASCADE;
DROP TABLE delta_t;
