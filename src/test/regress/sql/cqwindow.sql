CREATE CONTINUOUS VIEW cqwindow0 AS SELECT key::text, SUM(x::int) OVER (PARTITION BY key ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM stream;
\d+ cqwindow0_pdb;
\d+ cqwindow0;
ACTIVATE cqwindow0;
INSERT INTO stream (key, x) VALUES ('a', 1), ('b', 2);
SELECT pg_sleep(1);
INSERT INTO stream (key, x) VALUES ('a', 3), ('b', 4);
SELECT pg_sleep(1);
INSERT INTO stream (key, x) VALUES ('a', 5), ('b', 6);
SELECT pg_sleep(1);
INSERT INTO stream (key, x) VALUES ('a', 7), ('b', 8);
DEACTIVATE;
SELECT key, sum FROM cqwindow0_pdb ORDER BY _0, key;
SELECT * FROM cqwindow0 ORDER BY key;
