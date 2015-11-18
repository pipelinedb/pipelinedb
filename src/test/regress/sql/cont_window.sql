CREATE CONTINUOUS VIEW cqwindow0 AS SELECT key::text, SUM(x::numeric) OVER (PARTITION BY key ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM cqwindow_stream;
\d+ cqwindow0_mrel;
\d+ cqwindow0;
SELECT pipeline_get_overlay_viewdef('cqwindow0');

INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 3), ('b', 4);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 5), ('b', 6);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 7), ('b', 8);

SELECT * FROM cqwindow0 ORDER BY key;

CREATE CONTINUOUS VIEW cqwindow1 AS SELECT key::text, AVG(x::int) OVER (PARTITION BY key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM cqwindow_stream;
\d+ cqwindow1_mrel;
\d+ cqwindow1;
SELECT pipeline_get_overlay_viewdef('cqwindow1');
SELECT pipeline_get_worker_querydef('cqwindow1');
SELECT pipeline_get_combiner_querydef('cqwindow1');

INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2), ('a', 3);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 4), ('b', 5), ('b', 6);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 7), ('b', 8), ('a', 9);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 10), ('b', 11), ('b', 12);

SELECT * FROM cqwindow1 ORDER BY key;

CREATE CONTINUOUS VIEW cqwindow2 AS SELECT COUNT(*) FROM cqwindow_stream WHERE (hour(arrival_timestamp) > clock_timestamp() - interval '1 hour');
CREATE CONTINUOUS VIEW cqwindow3 AS SELECT COUNT(*) FROM cqwindow_stream WHERE (year(arrival_timestamp) > clock_timestamp() - interval '10 years');

INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2), ('a', 3);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2), ('a', 3);
SELECT pg_sleep(1);

SELECT COUNT(*) FROM cqwindow2_mrel;
SELECT COUNT(*) FROM cqwindow3_mrel;

CREATE CONTINUOUS VIEW cqwindow4 AS SELECT COUNT(*) FROM cqwindow_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';
SELECT pipeline_get_worker_querydef('cqwindow4');

DROP CONTINUOUS VIEW cqwindow0;
DROP CONTINUOUS VIEW cqwindow1;
DROP CONTINUOUS VIEW cqwindow2;
DROP CONTINUOUS VIEW cqwindow3;
DROP CONTINUOUS VIEW cqwindow4;
