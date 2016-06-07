CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second' GROUP BY k;

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

DROP CONTINUOUS VIEW test_count;

CREATE CONTINUOUS VIEW sw_count0 AS SELECT COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '10 second';

CREATE VIEW sw_count1 AS SELECT combine(count) FROM sw_count0_mrel WHERE arrival_timestamp > clock_timestamp() - interval '5 second';

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x');

SELECT * FROM sw_count0;
SELECT * FROM sw_count1;

SELECT pg_sleep(6);

SELECT * FROM sw_count0;
SELECT * FROM sw_count1;

DROP CONTINUOUS VIEW sw_count0 CASCADE;
