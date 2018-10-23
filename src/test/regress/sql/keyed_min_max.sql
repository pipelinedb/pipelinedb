SELECT keyed_min(x, -x) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(-x::float, x::int8) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(x::numeric, x * 100) FROM generate_series(1, 1000) AS x;
SELECT keyed_min('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'::text, x + 1) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(-x, 'xxxxxxxxxxxxxxxxxxxxxxxxxx'::text) FROM generate_series(1, 1000) AS x;
SELECT keyed_min('xxxxxxxxxxxxxxxxxxxxxxxxxxx'::text, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'::text) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(x, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'::text) FROM
	(SELECT CASE WHEN x < 10 THEN NULL ELSE x END AS x FROM generate_series(1, 1000) AS x) _;
SELECT keyed_min(x, NULL::text) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(NULL::text, x) FROM generate_series(1, 1000) AS x;
SELECT keyed_min(NULL::integer, NULL::integer) FROM generate_series(1, 1000) AS x;

SELECT keyed_max(x, -x) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(x::float, x::int8) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(x::numeric, x * 100) FROM generate_series(1, 1000) AS x;
SELECT keyed_max('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'::text, x + 1) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(x, 'xxxxxxxxxxxxxxxxxxxxxxxxxx'::text) FROM generate_series(1, 1000) AS x;
SELECT keyed_max('xxxxxxxxxxxxxxxxxxxxxxxxxxx'::text, 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy'::text) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(x::text, y::text) FROM
	(SELECT CASE WHEN x < 10 THEN NULL ELSE x END AS x, 42 AS y FROM generate_series(1, 1000) AS x) _;
SELECT keyed_max(x, NULL::integer) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(NULL::text, x) FROM generate_series(1, 1000) AS x;
SELECT keyed_max(NULL::float8, NULL::float8) FROM generate_series(1, 1000) AS x;
SELECT keyed_max('192.168.0.1'::inet, -x) FROM generate_series(1, 1000) AS x;

CREATE FOREIGN TABLE keyed_min_max_stream (x int, y numeric, z text, t text) SERVER pipelinedb;
CREATE VIEW keyed_min_max0 AS
	SELECT x::integer % 10 AS g, keyed_min(x, y::numeric), keyed_max(x, y),
		 keyed_min(z::text, substring(t::text, 1, 2)) AS keyed_min_text
FROM keyed_min_max_stream GROUP BY g;

INSERT INTO keyed_min_max_stream (x, y, z, t) SELECT x, -x AS y, x AS z, x + 1 AS t FROM generate_series(201, 300) AS x;
INSERT INTO keyed_min_max_stream (x, y, z, t) SELECT x, -x AS y, x AS z, x + 1 AS t FROM generate_series(1, 100) AS x;
INSERT INTO keyed_min_max_stream (x, y, z, t) SELECT x, -x AS y, x AS z, x + 1 AS t FROM generate_series(101, 200) AS x;

SELECT * FROM keyed_min_max0 ORDER BY g;

DROP VIEW keyed_min_max0;
DROP FOREIGN TABLE keyed_min_max_stream CASCADE;

CREATE FOREIGN TABLE keyed_min_max_stream (ts timestamptz, x text) SERVER pipelinedb;
CREATE VIEW keyed_min_max1 AS
	SELECT keyed_min(ts::timestamptz, x::text) AS first_value, keyed_max(ts, x) AS last_value
FROM keyed_min_max_stream;

INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:01:00', '10000000000000000'); -- First
INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:03:00', '2000000000000000');
INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:04:00', '300000000000000');
INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:02:00', '40000000000000');
INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:06:00', '5000000000000'); -- Last
INSERT INTO keyed_min_max_stream (ts, x) VALUES ('2015-10-31 00:05:00', '600000000000');

SELECT * FROM keyed_min_max1;

DROP VIEW keyed_min_max1;
DROP FOREIGN TABLE keyed_min_max_stream CASCADE;

-- #1797
CREATE FOREIGN TABLE keyed_min_max_stream (ticketid text, val int, status int) SERVER pipelinedb;

CREATE VIEW keyed_min_max2 AS
  SELECT
    ticketid,
    min(val) as v0,
   keyed_min(val, status) as v0_status
  FROM keyed_min_max_stream
  GROUP BY ticketid;

CREATE VIEW keyed_min_max3 AS
  SELECT
    (new).ticketid,
    (old).v0 as oldV0,
    (new).v0 as newV0
  FROM output_of('keyed_min_max2');

INSERT INTO keyed_min_max_stream (ticketid, val, status) VALUES ('t1', 124, 1);
SELECT pg_sleep(0.1);
INSERT INTO keyed_min_max_stream (ticketid, val, status) VALUES ('t3', 140, 1), ('t1', 80, 0);

SELECT * FROM keyed_min_max2 WHERE ticketid = 't1';
SELECT * FROM keyed_min_max3 WHERE ticketid = 't1';

DROP VIEW keyed_min_max3;
DROP VIEW keyed_min_max2;

CREATE VIEW keyed_min_max4 WITH (sw = '1 day') AS
  SELECT keyed_max(1, val) AS keymax FROM keyed_min_max_stream;

SELECT * FROM keyed_min_max4;

DROP VIEW keyed_min_max4;
DROP FOREIGN TABLE keyed_min_max_stream;
