CREATE CONTINUOUS VIEW test_cont_activate AS SELECT COUNT(*) FROM cont_activate_stream;

INSERT INTO cont_activate_stream (x) VALUES (1);
SELECT * FROM test_cont_activate;
SELECT pg_sleep(0.2);
SELECT state, query FROM pg_stat_activity WHERE query LIKE '%regression%' ORDER BY query;

ACTIVATE;

INSERT INTO cont_activate_stream (x) VALUES (1);
SELECT * FROM test_cont_activate;
SELECT pg_sleep(0.2);
SELECT state, query FROM pg_stat_activity WHERE query LIKE '%regression%' ORDER BY query;

DEACTIVATE;

INSERT INTO cont_activate_stream (x) VALUES (1);
SELECT * FROM test_cont_activate;
SELECT pg_sleep(0.2);
SELECT state, query FROM pg_stat_activity WHERE query LIKE '%regression%' ORDER BY query;

DEACTIVATE;

INSERT INTO cont_activate_stream (x) VALUES (1);
SELECT * FROM test_cont_activate;
SELECT pg_sleep(0.2);
SELECT state, query FROM pg_stat_activity WHERE query LIKE '%regression%' ORDER BY query;

ACTIVATE;

INSERT INTO cont_activate_stream (x) VALUES (1);
SELECT * FROM test_cont_activate;
SELECT pg_sleep(0.2);
SELECT state, query FROM pg_stat_activity WHERE query LIKE '%regression%' ORDER BY query;
