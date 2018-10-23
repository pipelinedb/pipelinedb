CREATE SCHEMA test_typed_streams;
SET search_path TO test_typed_streams,public;

CREATE FOREIGN TABLE s0 (x integer, y integer) SERVER pipelinedb;

-- It isn't possible for users to directly read a stream
SELECT * FROM s0;

CREATE VIEW v0 AS
SELECT x, COUNT(*) FROM test_typed_streams.s0 GROUP BY x;

-- Nothing is reading from this stream
CREATE FOREIGN TABLE no_readers (x integer) SERVER pipelinedb;
INSERT INTO no_readers (x) VALUES (0);
PREPARE p AS INSERT INTO no_readers (x) VALUES ($1);
EXECUTE p(0);

INSERT INTO s0 (x) VALUES (0);
INSERT INTO test_typed_streams.s0 (x) VALUES (0);

SELECT * FROM v0;

CREATE TABLE t (x integer);
INSERT INTO t (x) VALUES (0);

CREATE VIEW stj AS SELECT s0.x FROM test_typed_streams.s0 JOIN test_typed_streams.t USING (x);
INSERT INTO s0 (x) VALUES (0);
SELECT * FROM stj;

CREATE VIEW aliased AS SELECT alias.x FROM test_typed_streams.s0 AS alias;
INSERT INTO s0 (x) VALUES (0);

SELECT * FROM aliased;

-- Stream column doesn't exist
CREATE VIEW nonexistent AS SELECT not_here FROM s0;

CREATE FOREIGN TABLE test_typed_streams.stj_stream (id integer, tid integer, data json, val float, col0 integer, col1 integer) SERVER pipelinedb;

-- Simple joins
CREATE TABLE stj_t0 (tid integer, data text, val float8);

INSERT INTO stj_t0 (tid, data, val) VALUES (0, '0', 0.1);
INSERT INTO stj_t0 (tid, data, val) VALUES (0, '1', 1.2);
INSERT INTO stj_t0 (tid, data, val) VALUES (1, '2', 2.3);
INSERT INTO stj_t0 (tid, data, val) VALUES (1, '3', 3.4);
INSERT INTO stj_t0 (tid, data, val) VALUES (2, '4', 4.5);
INSERT INTO stj_t0 (tid, data, val) VALUES (2, '5', 5.6);
INSERT INTO stj_t0 (tid, data, val) VALUES (3, '6', 6.7);
INSERT INTO stj_t0 (tid, data, val) VALUES (4, '0', 4.0);
INSERT INTO stj_t0 (tid, data, val) VALUES (0, '7', -2.3);
INSERT INTO stj_t0 (tid, data, val) VALUES (0, '8', -3.4);
INSERT INTO stj_t0 (tid, data, val) VALUES (1, '9', -4.5);
INSERT INTO stj_t0 (tid, data, val) VALUES (2, '10', -5.6);
INSERT INTO stj_t0 (tid, data, val) VALUES (7, '[0, 1]', -6.7);
INSERT INTO stj_t0 (tid, data, val) VALUES (7, '"0"', -6.7);
INSERT INTO stj_t0 (tid, data, val) VALUES (7, '"1"', -6.7);

CREATE VIEW stj0 AS SELECT s.id, t.tid, t.data FROM test_typed_streams.stj_stream s JOIN test_typed_streams.stj_t0 t ON s.id = t.tid;
CREATE VIEW stj1 AS SELECT s.id, t.tid, t.data, s.data as jdata FROM test_typed_streams.stj_t0 t JOIN test_typed_streams.stj_stream s ON s.id = t.tid WHERE s.data::jsonb = '[0, 1]';
CREATE VIEW stj2 AS SELECT stj_stream.id, stj_t0.val FROM test_typed_streams.stj_t0, test_typed_streams.stj_stream WHERE stj_t0.tid = 0;
CREATE VIEW stj3 AS SELECT s.id, t.val, s.data::json FROM test_typed_streams.stj_t0 t JOIN test_typed_streams.stj_stream s ON s.id = t.tid AND s.id = t.val;

INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (3, '42');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (4, '"matched"');

SELECT pg_sleep(0.5);

SELECT * FROM stj0 ORDER BY id;
SELECT * FROM stj1 ORDER BY id;
SELECT * FROM stj2 ORDER BY id, val;
SELECT * FROM stj3 ORDER BY id;

-- Stream-table joins involving multiple tables
CREATE TABLE stj_t1 (jid jsonb, data jsonb);

INSERT INTO stj_t1 (jid, data) VALUES ('[0, 1]', '"json string"');
INSERT INTO stj_t1 (jid, data) VALUES ('"0"', '71');
INSERT INTO stj_t1 (jid, data) VALUES ('"1"', '[1, 2, 4, 8, 16]');
INSERT INTO stj_t1 (jid, data) VALUES ('[1, 0]', '{"key": "v"}');
INSERT INTO stj_t1 (jid, data) VALUES ('2', '["one", "two", "three"]');
INSERT INTO stj_t1 (jid, data) VALUES ('3', '["four", 5, "six"]');
INSERT INTO stj_t1 (jid, data) VALUES ('4', '{"k0": {"k1": "v"}}');
INSERT INTO stj_t1 (jid, data) VALUES ('{}', '0');
INSERT INTO stj_t1 (jid, data) VALUES ('[0, 1]', '["strrrr", "iiing"]');
INSERT INTO stj_t1 (jid, data) VALUES ('[]', '[{}, {}]');
INSERT INTO stj_t1 (jid, data) VALUES ('"0"', '[[]]');
INSERT INTO stj_t1 (jid, data) VALUES ('"1"', '[32, 64, 128]');

CREATE VIEW stj4 AS SELECT t1.jid, s.data AS stream_data, t0.data as table_data FROM test_typed_streams.stj_t1 t1 JOIN test_typed_streams.stj_stream s ON t1.jid = s.data::jsonb JOIN test_typed_streams.stj_t0 t0 ON t0.data::jsonb = t1.jid;

INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (3, '42');
INSERT INTO test_typed_streams.stj_stream (id, data) VALUES (4, '"not here"');

SELECT * FROM stj4 ORDER BY jid;

-- Stream-table joins with GROUP BYs and aggregates
CREATE TABLE stj_t2 (id integer, str text, val float);

INSERT INTO stj_t2 (id, str, val) VALUES (0, 'str0', 101.1);
INSERT INTO stj_t2 (id, str, val) VALUES (1, 'str1', 202.2);
INSERT INTO stj_t2 (id, str, val) VALUES (2, 'str2', 42.42);
INSERT INTO stj_t2 (id, str, val) VALUES (3, 'not here', 1000.1);
INSERT INTO stj_t2 (id, str, val) VALUES (5, 'joined', 52.0);

CREATE VIEW stj5 AS SELECT s.id, t.str, sum(s.val + t.val) FROM test_typed_streams.stj_stream s JOIN test_typed_streams.stj_t2 t ON s.id = t.id GROUP BY s.id, t.str;

INSERT INTO test_typed_streams.stj_stream (id, val) VALUES (0, -101.1);
INSERT INTO test_typed_streams.stj_stream (id, val) VALUES (1, -202.2);
INSERT INTO test_typed_streams.stj_stream (id, val) VALUES (4, 2000.201);

INSERT INTO test_typed_streams.stj_stream (id, val) VALUES (5, 0.52);

SELECT * FROM stj5 ORDER BY id;

CREATE TABLE stj_t3 (col0 integer, col1 integer, col2 integer, col3 integer);

INSERT INTO stj_t3 (col0, col1, col2, col3) VALUES (0, 0, 0, 42);
INSERT INTO stj_t3 (col0, col1, col2, col3) VALUES (0, 1, 1, 1000);

CREATE VIEW stj6 AS SELECT t.col2, sum(s.col0 + t.col3) FROM test_typed_streams.stj_stream s JOIN test_typed_streams.stj_t3 t ON s.col1 = t.col1 GROUP BY t.col2;

INSERT INTO test_typed_streams.stj_stream (col0, col1) VALUES (400, 1);
INSERT INTO test_typed_streams.stj_stream (col0, col1) VALUES (0, 0);
INSERT INTO test_typed_streams.stj_stream (col0, col1) VALUES (-1200, 1);

SELECT * FROM stj6 ORDER BY col2;

CREATE FOREIGN TABLE test_hs_stream (g integer, x integer, y integer, z text) SERVER pipelinedb;

CREATE VIEW test_percent0 AS SELECT percent_rank(2) WITHIN GROUP (ORDER BY x) FROM test_typed_streams.test_hs_stream;
CREATE VIEW test_percent1 AS SELECT g, percent_rank('7'::text) WITHIN GROUP (ORDER BY z) AS p0, percent_rank('00'::text) WITHIN GROUP (ORDER BY z) AS p1 FROM test_typed_streams.test_hs_stream GROUP BY g;
CREATE VIEW test_percent2 AS SELECT percent_rank(27, -27, '27'::text) WITHIN GROUP (ORDER BY x, y, z) FROM test_typed_streams.test_hs_stream;
CREATE VIEW test_percent3 AS SELECT percent_rank(10, 10) WITHIN GROUP (ORDER BY x, y) FROM test_typed_streams.test_hs_stream;

INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (7, 27, -27, '27'), (2, 32, -32, '32'), (4, 4, -4, '4'), (6, 26, -26, '26'), (1, 11, -11, '11'), (0, 60, -60, '60'), (2, 82, -82, '82'), (0, 40, -40, '40'), (9, 19, -19, '19'), (7, 77, -77, '77'), (8, 18, -18, '18'), (9, 99, -99, '99'), (5, 85, -85, '85'), (8, 98, -98, '98'), (7, 57, -57, '57'), (5, 65, -65, '65'), (3, 43, -43, '43'), (0, 0, 0, '0'), (8, 38, -38, '38'), (6, 36, -36, '36'), (3, 83, -83, '83'), (7, 97, -97, '97'), (6, 86, -86, '86'), (9, 29, -29, '29'), (9, 79, -79, '79'), (4, 24, -24, '24'), (6, 46, -46, '46'), (1, 51, -51, '51'), (5, 45, -45, '45'), (0, 30, -30, '30'), (1, 61, -61, '61'), (7, 87, -87, '87'), (5, 5, -5, '5'), (4, 54, -54, '54'), (2, 22, -22, '22'), (5, 55, -55, '55'), (3, 13, -13, '13'), (2, 2, -2, '2'), (8, 58, -58, '58'), (2, 72, -72, '72'), (8, 48, -48, '48'), (4, 74, -74, '74'), (6, 16, -16, '16'), (9, 39, -39, '39'), (3, 53, -53, '53'), (8, 78, -78, '78'), (8, 88, -88, '88'), (3, 93, -93, '93'), (0, 50, -50, '50'), (1, 21, -21, '21'), (4, 34, -34, '34'), (1, 71, -71, '71'), (9, 49, -49, '49'), (7, 47, -47, '47'), (6, 56, -56, '56'), (6, 76, -76, '76'), (3, 73, -73, '73'), (0, 10, -10, '10'), (8, 28, -28, '28'), (2, 52, -52, '52'), (8, 8, -8, '8'), (3, 33, -33, '33'), (1, 41, -41, '41'), (0, 90, -90, '90'), (4, 94, -94, '94'), (4, 64, -64, '64'), (3, 23, -23, '23'), (0, 20, -20, '20'), (9, 69, -69, '69'), (6, 66, -66, '66'), (5, 95, -95, '95'), (6, 6, -6, '6'), (7, 37, -37, '37'), (5, 15, -15, '15'), (4, 84, -84, '84'), (3, 63, -63, '63'), (1, 31, -31, '31'), (2, 62, -62, '62'), (0, 80, -80, '80'), (5, 25, -25, '25'), (3, 3, -3, '3'), (9, 89, -89, '89'), (2, 42, -42, '42'), (8, 68, -68, '68'), (2, 12, -12, '12'), (1, 81, -81, '81'), (4, 14, -14, '14'), (0, 70, -70, '70'), (7, 67, -67, '67'), (2, 92, -92, '92'), (5, 75, -75, '75'), (7, 17, -17, '17'), (7, 7, -7, '7'), (1, 91, -91, '91'), (5, 35, -35, '35'), (9, 59, -59, '59'), (9, 9, -9, '9'), (1, 1, -1, '1'), (6, 96, -96, '96'), (4, 44, -44, '44');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (5, 5, -5, '5');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (5, 15, -15, '15');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (6, 6, -6, '6');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (0, 10, -10, '10');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (2, 2, -2, '2');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (4, 4, -4, '4');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (9, 9, -9, '9');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (7, 17, -17, '17');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (3, 3, -3, '3');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (1, 1, -1, '1');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (3, 13, -13, '13');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (8, 18, -18, '18');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (4, 14, -14, '14');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (7, 7, -7, '7');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (0, 0, 0, '0');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (6, 16, -16, '16');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (1, 11, -11, '11');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (8, 8, -8, '8');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (9, 19, -19, '19');
INSERT INTO test_typed_streams.test_hs_stream (g, x, y, z) VALUES (2, 12, -12, '12');

SELECT * FROM test_percent0 ORDER BY percent_rank;
SELECT * FROM test_percent1 ORDER BY g;
SELECT * FROM test_percent2 ORDER BY percent_rank;
SELECT * FROM test_percent3 ORDER BY percent_rank;

CREATE FOREIGN TABLE invalid (x int, arrival_timestamp text) SERVER pipelinedb;
CREATE FOREIGN TABLE invalid (arrival_timestamp timestamptz, x int) SERVER pipelinedb;

DROP SCHEMA test_typed_streams CASCADE;
