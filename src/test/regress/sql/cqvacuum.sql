SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW cqvacuum AS SELECT key::text, COUNT(*) FROM cqvacuum_stream WHERE arrival_timestamp > clock_timestamp() - interval '3 second' GROUP BY key;
ACTIVATE cqvacuum;
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
DEACTIVATE cqvacuum;
SELECT pg_sleep(1);
ACTIVATE cqvacuum;
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
INSERT INTO cqvacuum_stream (key) VALUES ('a'), ('b'), ('c');
DEACTIVATE cqvacuum;
SELECT * FROM cqvacuum;
SELECT key, count FROM cqvacuum_pdb;
SELECT pg_sleep(3);
SELECT * FROM cqvacuum;
SELECT key, count FROM cqvacuum_pdb;
VACUUM cqvacuum_pdb;
SELECT key, count FROM cqvacuum_pdb;
