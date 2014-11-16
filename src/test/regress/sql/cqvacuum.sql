SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW cqvacuum AS SELECT key::text, COUNT(*) FROM cqvacuum_stream WHERE arrival_timestamp > clock_timestamp() - interval '3 second' GROUP BY key;
ACTIVATE cqvacuum;
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
SELECT pg_sleep(1);
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
DEACTIVATE cqvacuum;
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, count FROM cqvacuum_mrel0 ORDER BY key, count;
SELECT pg_sleep(3);
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, count FROM cqvacuum_mrel0 ORDER BY key, count;
ACTIVATE cqvacuum;
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
DEACTIVATE cqvacuum;
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, count FROM cqvacuum_mrel0 ORDER BY key, count;
VACUUM cqvacuum_mrel0;
SELECT * FROM cqvacuum ORDER BY key;
SELECT key, count FROM cqvacuum_mrel0 ORDER BY key, count;

DROP CONTINUOUS VIEW cqvacuum;
