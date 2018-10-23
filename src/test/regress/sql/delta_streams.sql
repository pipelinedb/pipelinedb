CREATE FOREIGN TABLE delta_stream (x integer, y integer) SERVER pipelinedb;

-- SWs don't have delta streams
CREATE VIEW delta_sw  WITH (sw = '1 hour') AS SELECT x, count(*) FROM delta_stream GROUP BY x;
CREATE VIEW delta0 AS SELECT combine((delta).count) FROM output_of('delta_sw');

-- Several delta input columns
CREATE VIEW delta1 AS SELECT x, count(*), sum(y), avg(x + y) FROM delta_stream GROUP BY x;
CREATE VIEW delta2 AS
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
CREATE VIEW delta3 AS SELECT x, COUNT(DISTINCT y) FROM delta_stream GROUP BY x;
CREATE VIEW delta4 AS SELECT combine((delta).count) AS count FROM output_of('delta3');

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
SELECT * FROM delta4;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(101, 200) AS x;
SELECT * FROM delta4;

-- Large values in delta stream
CREATE VIEW delta5 AS SELECT x, bloom_agg(y) FROM delta_stream GROUP BY x;
CREATE VIEW delta6 AS SELECT (new).x % 2 AS x, combine((delta).bloom_agg) AS bloom_agg FROM output_of('delta5') GROUP BY x;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;

SELECT x, bloom_cardinality(bloom_agg) FROM delta6 ORDER BY x;

-- User combine
SELECT bloom_cardinality(combine(bloom_agg)) FROM delta6;

-- STJs with delta streams
CREATE TABLE delta_t AS SELECT generate_series(1, 10) AS x;
CREATE VIEW delta7 AS
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

-- Ordered sets with serialization/deserializtion functions
CREATE VIEW delta8 AS SELECT x,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY y) AS p50,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY y) AS p99
  FROM delta_stream GROUP BY x;

CREATE VIEW delta9 AS
  SELECT (delta).x % 2 AS x, combine((delta).p50) AS p50, combine((delta).p99) AS p99
  FROM output_of('delta8') GROUP BY x;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
SELECT * FROM delta9 ORDER BY x;

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(101, 200) AS x;
SELECT * FROM delta9 ORDER BY x;

SELECT combine(p50) AS p50, combine(p99) AS p99 FROM delta9;

-- Multiple levels of output streams
CREATE VIEW delta10 AS
  SELECT avg(x::numeric), count(*) FROM delta_stream;

CREATE VIEW delta11 AS
  SELECT combine((delta).avg) AS avg, combine((delta).count) AS count FROM output_of('delta10');

CREATE VIEW delta12 AS
  SELECT combine((delta).avg) AS avg, combine((delta).count) AS count FROM output_of('delta11');

CREATE VIEW delta13 AS
  SELECT combine((delta).avg) AS avg, combine((delta).count) AS count FROM output_of('delta12');

INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 100) AS x;
INSERT INTO delta_stream (x, y) SELECT x % 10, x FROM generate_series(1, 200) AS x;

SELECT * FROM delta10;
SELECT * FROM delta11;
SELECT * FROM delta12;
SELECT * FROM delta13;

DROP FOREIGN TABLE delta_stream CASCADE;
DROP TABLE delta_t;
