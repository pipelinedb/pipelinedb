CREATE CONTINUOUS VIEW test_avg AS SELECT key::text, avg(value::float8) FROM stream GROUP BY key;
ACTIVATE test_avg;

SET debug_sync_stream_insert = 'on';
INSERT INTO stream (key, value) VALUES ('x', 1), ('x', 2), ('y', 100);

DEACTIVATE test_avg;

SELECT * FROM test_avg;
SELECT * FROM test_avg_pdb;

ACTIVATE test_avg;

INSERT INTO stream (key, value) VALUES ('x', 10), ('x', 20), ('y', 200);

SELECT * FROM test_avg;
SELECT * FROM test_avg_pdb;

DEACTIVATE test_avg;

SELECT * FROM test_avg;

CREATE CONTINUOUS VIEW cv AS SELECT key::text, COUNT(*), MAX(x::integer + y::integer) FROM stream GROUP BY key;

ACTIVATE cv;

INSERT INTO stream (key, x, y) VALUES ('x', -1000, 1000), ('x', 0, 1), ('x', 1, 0);
INSERT INTO stream (key, x, y) VALUES ('y', 10, 20), ('y', 20, 30), ('y', 1, 200), ('y', 1, 200), ('y', 1, 200);
INSERT INTO stream (key, x, y) VALUES ('z', -1000, 1001);

DEACTIVATE cv;

SELECT * FROM cv;
