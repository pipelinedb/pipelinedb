-- Test errors/invalid inputs
CREATE FOREIGN TABLE ttl_stream (x integer) SERVER pipelinedb;

-- TTLs must be at least 1 second
CREATE VIEW ttl0 WITH (ttl='1 millisecond', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- Invalid interval
CREATE VIEW ttl0 WITH (ttl='not an interval', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- No ttl_column
CREATE VIEW ttl0 WITH (ttl='4 days')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- No ttl
CREATE VIEW ttl0 WITH (ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- Wrong types
CREATE VIEW ttl0 WITH (ttl='1 day', ttl_column=1)
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

CREATE VIEW ttl0 WITH (ttl=10000, ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- TTL column isn't a timestamp or timestamptz
CREATE VIEW ttl0 WITH (ttl='1 day', ttl_column='x')
	AS SELECT x FROM ttl_stream;

-- Can't specify TTLs with SWs
CREATE VIEW ttl0 WITH (ttl='1 day', ttl_column='ts', sw='2 days')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

-- Implicit SW expression
CREATE VIEW ttl0 WITH (ttl='3 seconds')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream
WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';

CREATE VIEW ttl0 WITH (ttl='3 seconds', ttl_column = 'ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream
WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';

CREATE VIEW ttl0 WITH (ttl='3 seconds', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;

CREATE VIEW ttl1 WITH (ttl='1 month', ttl_column='ts')
	AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;


SELECT c.relname, ttl, ttl_attno FROM pipelinedb.cont_query cq
JOIN pg_class c ON c.oid = cq.relid WHERE c.relname IN ('ttl0', 'ttl1')
ORDER BY c.relname;

INSERT INTO ttl_stream (x) VALUES (0);
INSERT INTO ttl_stream (x) VALUES (1);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

SELECT pg_sleep(3);

SELECT 0 * pipelinedb.ttl_expire('ttl0');

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

INSERT INTO ttl_stream (x) VALUES (0);
INSERT INTO ttl_stream (x) VALUES (1);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;

SELECT pg_sleep(3);
SELECT 0 * pipelinedb.ttl_expire('ttl0');

SELECT x, "$pk" FROM ttl0_mrel ORDER BY ts;
SELECT x, "$pk" FROM ttl1_mrel ORDER BY ts;

DROP VIEW ttl0;
DROP VIEW ttl1;

CREATE VIEW ttl2 AS SELECT arrival_timestamp AS ts, x FROM ttl_stream;
CREATE VIEW ttl3 WITH (sw = '1 second') AS SELECT count(*) FROM ttl_stream;

-- Can't change the TTL of a SW CV
SELECT pipelinedb.set_ttl('ttl3', '1 day', 'count');
DROP VIEW ttl3;

-- Bad CV names
SELECT pipelinedb.set_ttl(NULL, '1 day', 'x');
SELECT pipelinedb.set_ttl('does_not_exist', '1 day', 'x');

-- Bad interval
SELECT pipelinedb.set_ttl('ttl2', 'not an interval', 'x');

-- Bad column name
SELECT pipelinedb.set_ttl('ttl2', '1 day', 'does not exist');
SELECT pipelinedb.set_ttl('ttl2', '1 day', 'x');

-- Ok, now verify legitimate invocations
SELECT pipelinedb.set_ttl('ttl2', '5 seconds', 'ts');

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x FROM ttl2 ORDER BY ts;

SELECT pg_sleep(6);
SELECT 0 * pipelinedb.ttl_expire('ttl2');

SELECT x FROM ttl2 ORDER BY ts;

SELECT pipelinedb.set_ttl('ttl2', '1 second', 'ts');

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);

SELECT x FROM ttl2 ORDER BY ts;

SELECT pg_sleep(2);
SELECT 0 * pipelinedb.ttl_expire('ttl2');

SELECT x FROM ttl2 ORDER BY ts;

-- No verify we can remove a TTL
SELECT pipelinedb.set_ttl('ttl2', null, null);

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);
SELECT 0 * pipelinedb.ttl_expire('ttl2');

SELECT x FROM ttl2 ORDER BY ts;

DROP VIEW ttl2;

-- #1881 regression test
CREATE VIEW "MyTTLCV" WITH (ttl = '1 second', ttl_column = 'second') AS
  SELECT second(arrival_timestamp), count(*) FROM ttl_stream GROUP BY second;

INSERT INTO ttl_stream (x) VALUES (2);
INSERT INTO ttl_stream (x) VALUES (2);
SELECT pg_sleep(1.1);

SELECT 0 * pipelinedb.ttl_expire('"MyTTLCV"');

DROP VIEW "MyTTLCV";

DROP FOREIGN TABLE ttl_stream CASCADE;
