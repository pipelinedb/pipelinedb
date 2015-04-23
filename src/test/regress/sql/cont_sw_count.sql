CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

ACTIVATE test_count;

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE test_count;

SELECT * FROM test_count ORDER BY k;

ACTIVATE test_count;

SELECT pg_sleep(1);

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE test_count;

SELECT * FROM test_count ORDER BY k;

DROP CONTINUOUS VIEW test_count;
