set debug_sync_stream_insert = on;
CREATE CONTINUOUS VIEW cqlatch AS SELECT id::integer FROM stream_latch;
ACTIVATE cqlatch;
-- Wait at least 3 seconds till the worker latch is actually blocked
SELECT pg_sleep_for('3 seconds');
-- Query the activity table to verify that the worker is waiting on the latch
SELECT query, state FROM pg_stat_activity WHERE query LIKE '%stream_latch%' ORDER BY query;
-- Insert something into the stream so that the latch is released
INSERT INTO stream_latch (id) VALUES (4);
INSERT INTO stream_latch (id) VALUES (5);
-- Query the activity table verify that the worker latch is unblocked
SELECT query, state FROM pg_stat_activity WHERE query LIKE '%stream_latch%' ORDER BY query;
DEACTIVATE cqlatch;
DROP CONTINUOUS VIEW cqlatch;
