CREATE FOREIGN TABLE cqswcount_stream (k text) SERVER pipelinedb;

CREATE VIEW test_count AS SELECT k::text, COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second' GROUP BY k;

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

DROP VIEW test_count;

CREATE VIEW sw_count0 AS SELECT COUNT(*) FROM cqswcount_stream WHERE arrival_timestamp > clock_timestamp() - interval '10 second';

CREATE VIEW sw_count1 AS SELECT combine(count) FROM sw_count0_mrel WHERE arrival_timestamp > clock_timestamp() - interval '5 second';

INSERT INTO cqswcount_stream (k) VALUES ('x'), ('x');

SELECT * FROM sw_count0;
SELECT * FROM sw_count1;

SELECT pg_sleep(6);

SELECT * FROM sw_count0;
SELECT * FROM sw_count1;

DROP FOREIGN TABLE cqswcount_stream CASCADE;

-- Verify that we can specify our own sliding-window column
CREATE FOREIGN TABLE sw_col_s (x integer, ts timestamptz) SERVER pipelinedb;

CREATE VIEW sw_col0 WITH (sw = '10 seconds', sw_column = 'ts') AS SELECT COUNT(*) FROM sw_col_s;

INSERT INTO sw_col_s (x, ts) VALUES (0, now() - interval '8 seconds');

SELECT * FROM sw_col0;

SELECT pg_sleep(3);

SELECT * FROM sw_col0;

DROP FOREIGN TABLE sw_col_s CASCADE;