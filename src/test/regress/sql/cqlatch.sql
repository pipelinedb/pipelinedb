set debug_sync_stream_insert = on;
CREATE CONTINUOUS VIEW cqlatch AS SELECT id::integer FROM stream_latch;
ACTIVATE CONTINUOUS VIEW cqlatch;
-- Wait at least 10 seconds till the worker latch is actually blocoed
SELECT pg_sleep_for('3 seconds');
-- Query the activity table verify that the worker is waiting on the latch
select query,state from pg_stat_activity;
-- Insert something into the stream so that the latch is released
INSERT INTO stream_latch (id) VALUES (4);
INSERT INTO stream_latch (id) VALUES (5);
-- Query the activity table verify that the worker latch is unblocked
select query,state from pg_stat_activity;
DEACTIVATE CONTINUOUS VIEW cqlatch;
