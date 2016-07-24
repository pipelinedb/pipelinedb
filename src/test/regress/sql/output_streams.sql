
CREATE CONTINUOUS VIEW os0 AS SELECT COUNT(*) FROM os_stream;
CREATE CONTINUOUS VIEW os0_output AS SELECT (old).count AS old_count, (new).count AS new_count FROM os0_osrel;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT * FROM os0_output;

-- We shouldn't be able to drop this because os0_output depends on it now
DROP CONTINUOUS VIEW os0;

DROP CONTINUOUS VIEW os0_output;
DROP CONTINUOUS VIEW os0;

CREATE CONTINUOUS VIEW os1 AS SELECT abs(x::integer) AS g, COUNT(*), sum(x)
FROM os_stream GROUP BY g;

CREATE CONTINUOUS VIEW os1_output AS SELECT
  (new).g,
  (old).count AS old_count, (new).count AS new_count,
  (old).sum AS old_sum, (new).sum AS new_sum
FROM os1_osrel;

INSERT INTO os_stream (x) VALUES (10);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (20);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (30);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (40);
SELECT pg_sleep(2);

INSERT INTO os_stream (x) VALUES (-40);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-30);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-20);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-10);
SELECT pg_sleep(2);

SELECT * FROM os1_output;

DROP CONTINUOUS VIEW os1 CASCADE;

-- Verify SW ticking into output streams
CREATE CONTINUOUS VIEW os2 WITH (max_age = '10 seconds') AS
SELECT x::integer, COUNT(*)
FROM os_stream GROUP BY x;

CREATE CONTINUOUS VIEW os2_output AS SELECT
  arrival_timestamp,
  CASE WHEN old IS NULL THEN (new).x ELSE (old).x END AS x,
  old, new
FROM os2_osrel;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (1);
SELECT pg_sleep(2);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (2);
SELECT pg_sleep(2);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (3);
SELECT pg_sleep(20);

SELECT x, old, new FROM os2_output ORDER BY arrival_timestamp;

DROP CONTINUOUS VIEW os2 CASCADE;

-- Stream-table joins on output streams
CREATE TABLE os_t0 (x integer, y integer);
INSERT INTO os_t0 (x, y) VALUES (0, 42);

CREATE CONTINUOUS VIEW os3 AS SELECT x::integer, COUNT(*)
FROM os_stream GROUP BY x;

CREATE CONTINUOUS VIEW os3_output AS SELECT
  CASE WHEN (s.old) IS NULL THEN (s.new).x ELSE (s.old).x END AS x,
  s.new, t.y
  FROM os3_osrel s JOIN os_t0 t ON x = t.x;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT x, new, y FROM os3_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT x, new, y FROM os3_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(2);

SELECT x, new, y FROM os3_output;

DROP TABLE os_t0;
DROP CONTINUOUS VIEW os3 CASCADE;
