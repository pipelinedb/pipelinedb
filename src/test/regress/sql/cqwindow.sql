SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW cqwindow0 AS SELECT key::text, SUM(x::numeric) OVER (PARTITION BY key ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM cqwindow_stream;
\d+ cqwindow0_mrel0;
\d+ cqwindow0;
ACTIVATE cqwindow0;
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 3), ('b', 4);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 5), ('b', 6);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 7), ('b', 8);
DEACTIVATE cqwindow0;
SELECT * FROM cqwindow0 ORDER BY key;

CREATE CONTINUOUS VIEW cqwindow1 AS SELECT key::text, AVG(x::int) OVER (PARTITION BY key ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM cqwindow_stream;
\d+ cqwindow1_mrel0;
\d+ cqwindow1;
ACTIVATE cqwindow1;
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 1), ('b', 2), ('a', 3);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 4), ('b', 5), ('b', 6);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 7), ('b', 8), ('a', 9);
SELECT pg_sleep(1);
INSERT INTO cqwindow_stream (key, x) VALUES ('a', 10), ('b', 11), ('b', 12);
DEACTIVATE cqwindow1;
SELECT * FROM cqwindow1 ORDER BY key;

DROP CONTINUOUS VIEW cqwindow0;
DROP CONTINUOUS VIEW cqwindow1;
