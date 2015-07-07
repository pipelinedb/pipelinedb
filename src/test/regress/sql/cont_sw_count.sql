CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second' GROUP BY k;

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

DROP CONTINUOUS VIEW test_count;
