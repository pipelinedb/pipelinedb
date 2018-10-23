CREATE FOREIGN TABLE os_stream (x int, y int, z int) SERVER pipelinedb;

CREATE VIEW os0 AS SELECT COUNT(*) FROM os_stream;
CREATE VIEW os0_output AS SELECT (old).count AS old_count, (new).count AS new_count FROM output_of('os0');

INSERT INTO os_stream (x) VALUES (0);

SELECT * FROM os0_output ORDER BY old_count, new_count;

INSERT INTO os_stream (x) VALUES (0);

SELECT * FROM os0_output ORDER BY old_count, new_count;

INSERT INTO os_stream (x) VALUES (0);

SELECT * FROM os0_output ORDER BY old_count, new_count;

INSERT INTO os_stream (x) VALUES (0);

SELECT * FROM os0_output ORDER BY old_count, new_count;

-- We shouldn't be able to drop this because os0_output depends on it now
DROP VIEW os0;

DROP VIEW os0_output;
DROP VIEW os0;

CREATE VIEW os1 AS SELECT abs(x::integer) AS g, COUNT(*), sum(x)
FROM os_stream GROUP BY g;

CREATE VIEW os1_output AS SELECT
  (new).g,
  (old).count AS old_count, (new).count AS new_count,
  (old).sum AS old_sum, (new).sum AS new_sum
FROM output_of('os1');

INSERT INTO os_stream (x) VALUES (10);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (20);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (30);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (40);

INSERT INTO os_stream (x) VALUES (-40);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (-30);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (-20);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

INSERT INTO os_stream (x) VALUES (-10);

SELECT * FROM os1_output ORDER BY g, old_count, new_count, old_sum, new_sum;

DROP VIEW os1 CASCADE;

-- Verify SW ticking into output streams
CREATE VIEW os2 WITH (sw = '10 seconds') AS
SELECT x::integer, COUNT(*)
FROM os_stream GROUP BY x;

CREATE VIEW os2_output AS SELECT
  arrival_timestamp,
  CASE WHEN old IS NULL THEN (new).x ELSE (old).x END AS x,
  old, new
FROM output_of('os2');

INSERT INTO os_stream (x) VALUES (0);

SELECT x, old, new FROM os2_output ORDER BY x, old, new;

INSERT INTO os_stream (x) VALUES (1);

SELECT x, old, new FROM os2_output ORDER BY x, old, new;

INSERT INTO os_stream (x) VALUES (2);

SELECT x, old, new FROM os2_output ORDER BY x, old, new;

INSERT INTO os_stream (x) VALUES (3);
SELECT pg_sleep(12);

SELECT x, old, new FROM os2_output ORDER BY x, old, new;

DROP VIEW os2 CASCADE;

-- Stream-table joins on output streams
CREATE TABLE os_t0 (x integer, y integer);
INSERT INTO os_t0 (x, y) VALUES (0, 42);

CREATE VIEW os3 AS SELECT x::integer, COUNT(*)
FROM os_stream GROUP BY x;

CREATE VIEW os3_output AS SELECT
  CASE WHEN (old) IS NULL THEN (new).x ELSE (old).x END AS x,
  new, t.y
  FROM output_of('os3') JOIN os_t0 t ON x = t.x;

INSERT INTO os_stream (x) VALUES (0);

SELECT x, new, y FROM os3_output ORDER BY x, new, y;

INSERT INTO os_stream (x) VALUES (0);

SELECT x, new, y FROM os3_output ORDER BY x, new, y;

INSERT INTO os_stream (x) VALUES (0);

SELECT x, new, y FROM os3_output ORDER BY x, new, y;

DROP TABLE os_t0;
DROP VIEW os3 CASCADE;
DROP TABLE os_t0;

-- Final functions should be applied to output stream tuples where necessary
CREATE VIEW os3 AS SELECT x::integer, avg(y::integer), count(distinct z::integer)
FROM os_stream GROUP BY x;

CREATE VIEW os3_output AS SELECT
  arrival_timestamp,
  CASE WHEN old IS NULL THEN (new).x ELSE (old).x END AS x,
  old, new
FROM output_of('os3');

INSERT INTO os_stream (x, y, z) VALUES (0, 2, 0);

SELECT x, old, new FROM os3_output ORDER BY x, old, new;

INSERT INTO os_stream (x, y, z) VALUES (0, 4, 1);

SELECT x, old, new FROM os3_output ORDER BY x, old, new;

INSERT INTO os_stream (x, y, z) VALUES (1, 8, 2);

SELECT x, old, new FROM os3_output ORDER BY x, old, new;

INSERT INTO os_stream (x, y, z) VALUES (1, 16, 2);

SELECT x, old, new FROM os3_output ORDER BY x, old, new;

DROP VIEW os3 CASCADE;

-- Verify that transforms write to output streams
CREATE VIEW os_xform WITH (action=transform) AS SELECT x, y FROM os_stream;

CREATE VIEW os4 AS SELECT x, y FROM output_of('os_xform');

INSERT INTO os_stream (x, y) VALUES (7, 7);
INSERT INTO os_stream (x, y) VALUES (8, 8);
INSERT INTO os_stream (x, y) VALUES (9, 9);

SELECT * FROM os4 ORDER BY x;

DROP FOREIGN TABLE os_stream CASCADE;

CREATE FOREIGN TABLE os_stream (x integer, y numeric) SERVER pipelinedb;

CREATE VIEW os5 AS
SELECT x,
  abs(sum(y) - sum(y)) AS abs,
	count(*),
  avg(y) AS avg
FROM os_stream GROUP BY x;

CREATE VIEW os6 AS
SELECT
  (new).x % 2 AS g,
  combine((delta).avg) AS avg
FROM output_of('os5') GROUP BY g;

INSERT INTO os_stream (x, y) SELECT x, x AS y FROM generate_series(1, 100) AS x;

SELECT count(*) FROM os5;
SELECT combine(avg) FROM os5;

SELECT * FROM os6 ORDER BY g;
SELECT combine(avg) FROM os6;

DROP FOREIGN TABLE os_stream CASCADE;

CREATE FOREIGN TABLE os_stream (ts timestamp, x text, y float) SERVER pipelinedb;

CREATE VIEW os7 WITH (sw = '5 minutes') AS
  SELECT x, date_trunc('second', ts) AS second, y, count(*)
FROM os_stream
GROUP BY second, y, x;

CREATE VIEW os8 AS SELECT (new).x, (new).y FROM output_of('os7');

BEGIN;
INSERT INTO os_stream (ts, x, y) VALUES (now(), 'text!', 42.42);
INSERT INTO os_stream (ts, x, y) VALUES (now(), 'text!', 42.42);
INSERT INTO os_stream (ts, x, y) VALUES (now(), 'text!', 42.42);
COMMIT;

SELECT pg_sleep(1);

SELECT x, y FROM os8 ORDER BY x, y;

-- Verify that complex projections are properly performed on output stream tuples
CREATE VIEW os9 AS SELECT x, avg(y), count(distinct y), avg(y) + count(distinct y) + 100 AS expr FROM os_stream GROUP BY x;
CREATE VIEW os10 AS SELECT (new).x, (new).expr FROM output_of('os9');

INSERT INTO os_stream (x, y) VALUES (0, 1.5);
INSERT INTO os_stream (x, y) VALUES (1, 2.5);
INSERT INTO os_stream (x, y) VALUES (0, 3.5);

SELECT * FROM os9 ORDER BY x;
SELECT * FROM os10 ORDER BY x, expr;

DROP FOREIGN TABLE os_stream CASCADE;