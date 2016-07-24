
CREATE CONTINUOUS VIEW os0 AS SELECT COUNT(*) FROM os_stream;
CREATE CONTINUOUS VIEW os0_output AS SELECT (old).count AS old_count, (new).count AS new_count FROM os0_osrel;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(1);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(1);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(1);

SELECT * FROM os0_output;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(1);

SELECT * FROM os0_output;

-- Shouldn't be able to drop this because os0_output depends on it now
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
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (20);
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (30);
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (40);
SELECT pg_sleep(1);

INSERT INTO os_stream (x) VALUES (-40);
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-30);
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-20);
SELECT pg_sleep(1);

SELECT * FROM os1_output;

INSERT INTO os_stream (x) VALUES (-10);
SELECT pg_sleep(1);

DROP CONTINUOUS VIEW os1 CASCADE;

CREATE CONTINUOUS VIEW os2 WITH (max_age = '10 seconds') AS
SELECT x::integer, COUNT(*)
FROM os_stream GROUP BY x;

CREATE CONTINUOUS VIEW os2_output AS SELECT
  arrival_timestamp,
  CASE WHEN old IS NULL THEN (new).x ELSE (old).x END AS x,
  old, new
FROM os2_osrel;

INSERT INTO os_stream (x) VALUES (0);
SELECT pg_sleep(1);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (1);
SELECT pg_sleep(1);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (2);
SELECT pg_sleep(1);

SELECT x, old, new FROM os2_output;

INSERT INTO os_stream (x) VALUES (3);
SELECT pg_sleep(20);

SELECT x, old, new FROM os2_output ORDER BY arrival_timestamp;

DROP CONTINUOUS VIEW os2 CASCADE;
