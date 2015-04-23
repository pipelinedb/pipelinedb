CREATE CONTINUOUS VIEW cqlimit AS SELECT x::int FROM cqlimit_stream LIMIT 10 OFFSET 3;
ACTIVATE cqlimit;

INSERT INTO cqlimit_stream (x) VALUES (1), (2), (3);
INSERT INTO cqlimit_stream (x) VALUES (4), (5), (6);
INSERT INTO cqlimit_stream (x) VALUES (7), (8), (9);
INSERT INTO cqlimit_stream (x) VALUES (10), (11), (12);
INSERT INTO cqlimit_stream (x) VALUES (13), (14), (15);
INSERT INTO cqlimit_stream (x) VALUES (16), (17), (18);
INSERT INTO cqlimit_stream (x) VALUES (19), (20), (21);

DEACTIVATE cqlimit;
SELECT * FROM cqlimit;
SELECT * FROM cqlimit_mrel0;

DROP CONTINUOUS VIEW cqlimit;
