SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

ACTIVATE test_count;

INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE;

SELECT k, count FROM test_count_pdb ORDER BY k;
SELECT * FROM test_count ORDER BY k;

ACTIVATE test_count;

SELECT pg_sleep(1);

INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE;

SELECT k, count FROM test_count_pdb ORDER BY k;
SELECT * FROM test_count ORDER BY k;

DROP CONTINUOUS VIEW test_count;
