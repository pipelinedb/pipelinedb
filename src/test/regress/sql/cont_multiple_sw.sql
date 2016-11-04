SET IntervalStyle to postgres;

CREATE STREAM msw_stream (x int, y int, z text);

-- We can't reference arrival_timestamp if the underlying CV isn't a SW
CREATE CONTINUOUS VIEW msw0 AS SELECT x::integer, COUNT(*) FROM msw_stream
WHERE x > 10 GROUP BY x;

CREATE VIEW msw1 AS SELECT combine(count) AS count FROM msw0
WHERE arrival_timestamp > clock_timestamp() - interval '2 seconds';

DROP CONTINUOUS VIEW msw0;

CREATE CONTINUOUS VIEW msw0 AS SELECT x::integer, COUNT(*), avg(x) FROM msw_stream
WHERE arrival_timestamp > clock_timestamp() - interval '10 second' GROUP BY x;

CREATE VIEW msw1 AS SELECT combine(count) AS count, combine(avg) AS avg FROM msw0
WHERE arrival_timestamp > clock_timestamp() - interval '2 seconds';

-- Verify that we can use sw on views that read from SW CVs
CREATE VIEW msw1_ma WITH (sw = '2 seconds') AS SELECT combine(count) AS count, combine(avg) AS avg FROM msw0;
\d+ msw1_ma;

CREATE VIEW msw2 AS SELECT combine(count) AS count, combine(avg) AS avg FROM msw0
WHERE arrival_timestamp > clock_timestamp() - interval '5 seconds';

INSERT INTO msw_stream (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM msw0 ORDER BY x;
SELECT * FROM msw1;
SELECT * FROM msw1_ma;
SELECT * FROM msw2;

SELECT pg_sleep(2.1);

SELECT * FROM msw0  ORDER BY x;
SELECT * FROM msw1;
SELECT * FROM msw1_ma;
SELECT * FROM msw2;

SELECT pg_sleep(3);

SELECT * FROM msw0  ORDER BY x;
SELECT * FROM msw1;
SELECT * FROM msw1_ma;
SELECT * FROM msw2;

-- View depends on CV
DROP CONTINUOUS VIEW msw0;
DROP CONTINUOUS VIEW msw0 CASCADE;

CREATE CONTINUOUS VIEW msw3 AS SELECT
	x::integer + 10 AS a,
	x - y::integer AS b,
	substring(z::text, 1, 2) AS c
FROM msw_stream
WHERE arrival_timestamp > clock_timestamp() - interval '1 minute';

CREATE VIEW msw4 AS SELECT * FROM msw3
WHERE arrival_timestamp > clock_timestamp() - interval '2 seconds';

INSERT INTO msw_stream (x, y, z) VALUES (0, 10, 'two');
INSERT INTO msw_stream (x, y, z) VALUES (1, 20, 'three');
INSERT INTO msw_stream (x, y, z) VALUES (2, 30, 'four');

SELECT * FROM msw3 ORDER BY a;
SELECT * FROM msw4 ORDER BY a;

SELECT pg_sleep(2.1);

SELECT * FROM msw3 ORDER BY a;
SELECT * FROM msw4 ORDER BY a;

DROP CONTINUOUS VIEW msw3 CASCADE;

CREATE CONTINUOUS VIEW msw5 AS
SELECT
  arrival_timestamp AS sw_time
FROM msw_stream
WHERE arrival_timestamp > clock_timestamp() - INTERVAL '10 minute';
\d+ msw5

CREATE VIEW msw6 WITH (sw = '1 minute') AS SELECT * FROM msw5;
\d+ msw6
SELECT * FROM msw6;

CREATE VIEW msw7 AS SELECT * FROM msw5 WHERE sw_time > clock_timestamp() - INTERVAL '1 minute';
\d+ msw7
SELECT * FROM msw7;

DROP CONTINUOUS VIEW msw5 CASCADE;

-- sw of view vs step_size
CREATE CONTINUOUS VIEW msw8 WITH (sw='10 minute', step_factor='10') AS SELECT count(*) FROM msw_stream;
CREATE VIEW msw9 WITH (sw='5 minute') AS SELECT * FROM msw8;
CREATE VIEW msw10 WITH (sw='2 minute') AS SELECT * FROM msw8;
CREATE VIEW msw11 WITH (sw='1 minute') AS SELECT * FROM msw8;

DROP CONTINUOUS VIEW msw8 CASCADE;

DROP STREAM msw_stream CASCADE;
