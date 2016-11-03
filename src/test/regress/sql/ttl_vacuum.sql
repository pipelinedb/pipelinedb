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

-- Can't specify TTLs with SWs
CREATE CONTINUOUS VIEW ttl0 WITH (ttl='1 day', ttl_column='ts', max_age='2 days')
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
DROP STREAM ttl_stream;
