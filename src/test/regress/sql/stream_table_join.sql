SET debug_sync_stream_insert = 'on';

-- Simple joins
CREATE TABLE test_stj_t0 (tid integer, data text, val float8);
CREATE CONTINUOUS VIEW test_stj0 AS SELECT s.id::integer, t.tid, t.data FROM test_stj_stream s JOIN test_stj_t0 t ON s.id = t.tid;
CREATE CONTINUOUS VIEW test_stj1 AS SELECT s.id::integer, t.tid, t.data, s.data as jdata FROM test_stj_t0 t JOIN test_stj_stream s ON s.id = t.tid WHERE s.data::jsonb = '[0, 1]';
CREATE CONTINUOUS VIEW test_stj2 AS SELECT test_stj_stream.id::integer, test_stj_t0.val FROM test_stj_t0, test_stj_stream WHERE test_stj_t0.tid = 0;
CREATE CONTINUOUS VIEW test_stj3 AS SELECT s.id::integer, t.val, s.data::json FROM test_stj_t0 t JOIN test_stj_stream s ON s.id = t.tid AND s.id = t.val;

INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '0', 0.1);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '1', 1.2);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '2', 2.3);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '3', 3.4);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '4', 4.5);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '5', 5.6);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (3, '6', 6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (4, '0', 4.0);

ACTIVATE test_stj0, test_stj1, test_stj2, test_stj3;

-- Verify that we see rows that were inserted after activation
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '7', -2.3);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '8', -3.4);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '9', -4.5);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '10', -5.6);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '[0, 1]', -6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '"0"', -6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '"1"', -6.7);

INSERT INTO test_stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_stj_stream (id, data) VALUES (3, 42);
INSERT INTO test_stj_stream (id, data) VALUES (4, '"matched"');

DEACTIVATE test_stj0, test_stj1, test_stj2, test_stj3;

SELECT * FROM test_stj0 ORDER BY id;
SELECT * FROM test_stj1 ORDER BY id;
SELECT * FROM test_stj2 ORDER BY val;
SELECT * FROM test_stj3 ORDER BY id;

-- Stream-table joins involving multiple tables
CREATE TABLE test_stj_t1 (jid jsonb, data jsonb);
CREATE CONTINUOUS VIEW test_stj4 AS SELECT t1.jid, s.data::jsonb AS stream_data, t0.data as table_data FROM test_stj_t1 t1 JOIN test_stj_stream s ON t1.jid = s.data JOIN test_stj_t0 t0 ON t0.data::jsonb = t1.jid; 

INSERT INTO test_stj_t1 (jid, data) VALUES ('[0, 1]', '"json string"');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"0"', '71');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"1"', '[1, 2, 4, 8, 16]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('[1, 0]', '{"key": "v"}');
INSERT INTO test_stj_t1 (jid, data) VALUES ('2', '["one", "two", "three"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('3', '["four", 5, "six"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('4', '{"k0": {"k1": "v"}}');
INSERT INTO test_stj_t1 (jid, data) VALUES ('{}', '0');

ACTIVATE test_stj4;

INSERT INTO test_stj_t1 (jid, data) VALUES ('[0, 1]', '["strrrr", "iiing"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('[]', '[{}, {}]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"0"', '[[]]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"1"', '[32, 64, 128]');

INSERT INTO test_stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_stj_stream (id, data) VALUES (3, '42');
INSERT INTO test_stj_stream (id, data) VALUES (4, '"not here"');

DEACTIVATE test_stj4;

SELECT * FROM test_stj4 ORDER BY jid;

-- Stream-table joins with GROUP BYs and aggregates
CREATE TABLE test_stj_t2 (id integer, str text, val float);
CREATE CONTINUOUS VIEW test_stj5 AS SELECT s.id::integer, t.str, sum(s.val::float8 + t.val) FROM test_stj_stream s JOIN test_stj_t2 t ON s.id = t.id GROUP BY s.id, t.str;

INSERT INTO test_stj_t2 (id, str, val) VALUES (0, 'str0', 101.1);
INSERT INTO test_stj_t2 (id, str, val) VALUES (1, 'str1', 202.2);
INSERT INTO test_stj_t2 (id, str, val) VALUES (2, 'str2', 42.42);
INSERT INTO test_stj_t2 (id, str, val) VALUES (3, 'not here', 1000.1);

ACTIVATE test_stj5;

INSERT INTO test_stj_stream (id, val) VALUES (0, -101.1);
INSERT INTO test_stj_stream (id, val) VALUES (1, -202.2);
INSERT INTO test_stj_stream (id, val) VALUES (4, 2000.201);

INSERT INTO test_stj_t2 (id, str, val) VALUES (5, 'joined', 52.0);
INSERT INTO test_stj_stream (id, val) VALUES (5, 0.52);

DEACTIVATE test_stj5;

DROP CONTINUOUS VIEW test_stj0;
DROP CONTINUOUS VIEW test_stj1;
DROP CONTINUOUS VIEW test_stj2;
DROP CONTINUOUS VIEW test_stj3;
DROP CONTINUOUS VIEW test_stj4;
DROP CONTINUOUS VIEW test_stj5;
DROP TABLE test_stj_t0;
DROP TABLE test_stj_t1;
DROP TABLE test_stj_t2;
