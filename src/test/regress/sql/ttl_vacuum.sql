-- Test errors/invalid inputs
CREATE STREAM ttl_stream (x integer);

-- Invalid interval
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='not an interval', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- No ttl_column
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='4 days')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- No ttl
CREATE CONTINUOUS VIEW ttl0 WITH (ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- Wrong types
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='1 day', ttl_column=1)
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

CREATE CONTINUOUS VIEW ttl0 WITH (ttl=10000, ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- TTL column isn't a timestamp or timestamptz
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='1 day', ttl_column='x')
	AS SELECT x FROM ttl_stream;

-- Can't specify TTLs with SWs
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='1 day', ttl_column='ts', sw='2 days')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

CREATE CONTINUOUS VIEW ttl0 WITH (ttl='3 seconds', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

CREATE CONTINUOUS VIEW ttl1 WITH (ttl='1 month', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

SELECT ttl, ttl_attno FROM pipeline_query pq
JOIN pg_class c ON c.oid = pq.relid WHERE c.relname IN ('ttl0', 'ttl1');

INSERT INTO ttl_stream (x) VALUES (0);
INSERT INTO ttl_stream (x) VALUES (1);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

SELECT pg_sleep(3);
VACUUM ttl0;

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

INSERT INTO ttl_stream (x) VALUES (0);
INSERT INTO ttl_stream (x) VALUES (1);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

SELECT pg_sleep(3);
VACUUM FULL ttl0;

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;
SELECT x, "$pk" FROM ttl1_mrel ORDER BY ts;

DROP CONTINUOUS VIEW ttl0;
DROP CONTINUOUS VIEW ttl1;

CREATE CONTINUOUS VIEW ttl2 AS SELECT second(clock_timestamp()) AS ts, x, COUNT(*) FROM ttl_stream GROUP BY ts, x;
CREATE CONTINUOUS VIEW ttl3 WITH (sw = '1 second') AS SELECT count(*) FROM ttl_stream;

-- Can't change the TTL of a SW CV
SELECT set_ttl('ttl3', '1 day', 'count');
DROP CONTINUOUS VIEW ttl3;

-- Bad CV names
SELECT set_ttl(NULL, '1 day', 'x');
SELECT set_ttl('does_not_exist', '1 day', 'x');

-- Bad interval
SELECT set_ttl('ttl2', 'not an interval', 'x');

-- Bad column name
SELECT set_ttl('ttl2', '1 day', 'does not exist');
SELECT set_ttl('ttl2', '1 day', 'x');

-- Ok, now verify legitimate invocations
SELECT set_ttl('ttl2', '5 seconds', 'ts');

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, count FROM ttl2;

SELECT pg_sleep(6);
VACUUM FULL ttl2;

SELECT x, count FROM ttl2;

SELECT set_ttl('ttl2', '1 second', 'ts');

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, count FROM ttl2;

SELECT pg_sleep(2);
VACUUM FULL ttl2;

SELECT x, count FROM ttl2;

-- No verify we can remove a TTL
SELECT set_ttl('ttl2', null, null);

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);
VACUUM FULL ttl2;

SELECT x, count FROM ttl2;

DROP CONTINUOUS VIEW ttl2;
DROP STREAM ttl_stream;
