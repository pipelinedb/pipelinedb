CREATE FOREIGN TABLE sw_vacuum_stream (key text) SERVER pipelinedb;
CREATE VIEW sw_vacuum AS SELECT key, COUNT(*) FROM sw_vacuum_stream WHERE arrival_timestamp > clock_timestamp() - interval '3 second' GROUP BY key;

INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT pg_sleep(1);

INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT * FROM sw_vacuum ORDER BY key;

-- Just verify that the mrel has more rows, as we can't gaurantee which time bucket the
-- rows will fall in which makes it tricky to compare this result to a predetermined result
SELECT (SELECT COUNT(*) FROM sw_vacuum) < (SELECT COUNT(*) FROM sw_vacuum_mrel);
SELECT DISTINCT key FROM sw_vacuum_mrel ORDER BY key;

SELECT pg_sleep(3);
SELECT 0 * pipelinedb.ttl_expire('sw_vacuum');

SELECT * FROM sw_vacuum ORDER BY key;
SELECT key, SUM(count) FROM sw_vacuum_mrel GROUP BY key ORDER BY key;

INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT pg_sleep(1);

INSERT INTO sw_vacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT * FROM sw_vacuum ORDER BY key;
SELECT (SELECT COUNT(*) FROM sw_vacuum) < (SELECT COUNT(*) FROM sw_vacuum_mrel);
SELECT DISTINCT key FROM sw_vacuum_mrel ORDER BY key;

SELECT 0 * pipelinedb.ttl_expire('sw_vacuum');
SELECT * FROM sw_vacuum ORDER BY key;
SELECT key, SUM(count) FROM sw_vacuum_mrel GROUP BY key ORDER BY key;

SELECT pg_sleep(3);
SELECT 0 * pipelinedb.ttl_expire('sw_vacuum');

SELECT * FROM sw_vacuum ORDER BY key;
SELECT key, SUM(count) FROM sw_vacuum_mrel GROUP BY key ORDER BY key;

DROP FOREIGN TABLE sw_vacuum_stream CASCADE;
