CREATE CONTINUOUS VIEW cqvacuum AS SELECT key::text, COUNT(*) FROM cqvacuum_stream WHERE arrival_timestamp > clock_timestamp() - interval '3 second' GROUP BY key;

INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT pg_sleep(1);

INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT * FROM cqvacuum ORDER BY key;

-- Just verify that the mrel has more rows, as we can't gaurantee which time bucket the
-- rows will fall in which makes it tricky to compare this result to a predetermined result
SELECT (SELECT COUNT(*) FROM cqvacuum) < (SELECT COUNT(*) FROM cqvacuum_mrel0);
SELECT DISTINCT key FROM cqvacuum_mrel0 ORDER BY key;

SELECT pg_sleep(3);

SELECT * FROM cqvacuum ORDER BY key;
SELECT key, SUM(count) FROM cqvacuum_mrel0 GROUP BY key ORDER BY key;

INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');

SELECT * FROM cqvacuum ORDER BY key;
SELECT (SELECT COUNT(*) FROM cqvacuum) < (SELECT COUNT(*) FROM cqvacuum_mrel0);
SELECT DISTINCT key FROM cqvacuum_mrel0 ORDER BY key;

VACUUM cqvacuum_mrel0;
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, SUM(count) FROM cqvacuum_mrel0 GROUP BY key ORDER BY key;

SELECT pg_sleep(3);

SELECT * FROM cqvacuum ORDER BY key;
SELECT key, SUM(count) FROM cqvacuum_mrel0 GROUP BY key ORDER BY key;

VACUUM FULL cqvacuum_mrel0;
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, SUM(count) FROM cqvacuum_mrel0 GROUP BY key ORDER BY key;

DROP CONTINUOUS VIEW cqvacuum;
