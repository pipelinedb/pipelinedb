-- Simple joins
CREATE TABLE test_stj_t0 (tid integer, data text, val float8);

INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '0', 0.1);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '1', 1.2);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '2', 2.3);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '3', 3.4);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '4', 4.5);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '5', 5.6);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (3, '6', 6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (4, '0', 4.0);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '7', -2.3);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (0, '8', -3.4);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (1, '9', -4.5);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (2, '10', -5.6);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '[0, 1]', -6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '"0"', -6.7);
INSERT INTO test_stj_t0 (tid, data, val) VALUES (7, '"1"', -6.7);

SELECT pg_sleep(0.1);

CREATE CONTINUOUS VIEW test_stj0 AS SELECT s.id::integer, t.tid, t.data FROM test_stj_stream s JOIN test_stj_t0 t ON s.id = t.tid;
CREATE CONTINUOUS VIEW test_stj1 AS SELECT s.id::integer, t.tid, t.data, s.data as jdata FROM test_stj_t0 t JOIN test_stj_stream s ON s.id = t.tid WHERE s.data::jsonb = '[0, 1]';
CREATE CONTINUOUS VIEW test_stj2 AS SELECT test_stj_stream.id::integer, test_stj_t0.val FROM test_stj_t0, test_stj_stream WHERE test_stj_t0.tid = 0;
CREATE CONTINUOUS VIEW test_stj3 AS SELECT s.id::integer, t.val, s.data::json FROM test_stj_t0 t JOIN test_stj_stream s ON s.id = t.tid AND s.id = t.val;

INSERT INTO test_stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_stj_stream (id, data) VALUES (3, '42');
INSERT INTO test_stj_stream (id, data) VALUES (4, '"matched"');

SELECT * FROM test_stj0 ORDER BY id;
SELECT * FROM test_stj1 ORDER BY id;
SELECT * FROM test_stj2 ORDER BY id, val;
SELECT * FROM test_stj3 ORDER BY id;

-- Stream-table joins involving multiple tables
CREATE TABLE test_stj_t1 (jid jsonb, data jsonb);

INSERT INTO test_stj_t1 (jid, data) VALUES ('[0, 1]', '"json string"');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"0"', '71');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"1"', '[1, 2, 4, 8, 16]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('[1, 0]', '{"key": "v"}');
INSERT INTO test_stj_t1 (jid, data) VALUES ('2', '["one", "two", "three"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('3', '["four", 5, "six"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('4', '{"k0": {"k1": "v"}}');
INSERT INTO test_stj_t1 (jid, data) VALUES ('{}', '0');

INSERT INTO test_stj_t1 (jid, data) VALUES ('[0, 1]', '["strrrr", "iiing"]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('[]', '[{}, {}]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"0"', '[[]]');
INSERT INTO test_stj_t1 (jid, data) VALUES ('"1"', '[32, 64, 128]');

SELECT pg_sleep(0.1);

CREATE CONTINUOUS VIEW test_stj4 AS SELECT t1.jid, s.data::jsonb AS stream_data, t0.data as table_data FROM test_stj_t1 t1 JOIN test_stj_stream s ON t1.jid = s.data JOIN test_stj_t0 t0 ON t0.data::jsonb = t1.jid;

INSERT INTO test_stj_stream (id, data) VALUES (0, '[0, 1]');
INSERT INTO test_stj_stream (id, data) VALUES (0, '{"key": 4}');
INSERT INTO test_stj_stream (id, data) VALUES (1, '4');
INSERT INTO test_stj_stream (id, data) VALUES (2, '[1, 0]');
INSERT INTO test_stj_stream (id, data) VALUES (2, '{ "key": [0, 1] }');
INSERT INTO test_stj_stream (id, data) VALUES (3, '42');
INSERT INTO test_stj_stream (id, data) VALUES (4, '"not here"');

SELECT * FROM test_stj4 ORDER BY jid;

-- Stream-table joins with GROUP BYs and aggregates
CREATE TABLE test_stj_t2 (id integer, str text, val float);

INSERT INTO test_stj_t2 (id, str, val) VALUES (0, 'str0', 101.1);
INSERT INTO test_stj_t2 (id, str, val) VALUES (1, 'str1', 202.2);
INSERT INTO test_stj_t2 (id, str, val) VALUES (2, 'str2', 42.42);
INSERT INTO test_stj_t2 (id, str, val) VALUES (3, 'not here', 1000.1);

SELECT pg_sleep(0.1);

CREATE CONTINUOUS VIEW test_stj5 AS SELECT s.id::integer, t.str, sum(s.val::float8 + t.val) FROM test_stj_stream s JOIN test_stj_t2 t ON s.id = t.id GROUP BY s.id, t.str;

INSERT INTO test_stj_stream (id, val) VALUES (0, -101.1);
INSERT INTO test_stj_stream (id, val) VALUES (1, -202.2);
INSERT INTO test_stj_stream (id, val) VALUES (4, 2000.201);
INSERT INTO test_stj_t2 (id, str, val) VALUES (5, 'joined', 52.0);

SELECT pg_sleep(0.1);

INSERT INTO test_stj_stream (id, val) VALUES (5, 0.52);

SELECT * FROM test_stj5 ORDER BY id;

CREATE TABLE test_stj_t3 (col0 integer, col1 integer, col2 integer, col3 integer);

INSERT INTO test_stj_t3 (col0, col1, col2, col3) VALUES (0, 0, 0, 42);
INSERT INTO test_stj_t3 (col0, col1, col2, col3) VALUES (0, 1, 1, 1000);

SELECT pg_sleep(0.1);

CREATE CONTINUOUS VIEW test_stj6 AS SELECT t.col2, sum(s.col0::integer + t.col3) FROM test_stj_stream s JOIN test_stj_t3 t ON s.col1::integer = t.col1 GROUP BY t.col2;

INSERT INTO test_stj_stream (col0, col1, col2) VALUES (400, 1, 0);
INSERT INTO test_stj_stream (col0, col1, col2) VALUES (0, 0, 0);
INSERT INTO test_stj_stream (col0, col1, col2) VALUES (-1200, 1, 0);

SELECT * FROM test_stj6 ORDER BY col2;

CREATE CONTINUOUS VIEW stj_no_tl AS SELECT COUNT(*)
FROM test_stj_stream JOIN test_stj_t0 ON test_stj_stream.id::integer = test_stj_t0.tid;

INSERT INTO test_stj_stream (id) VALUES (0);
SELECT * FROM stj_no_tl;

CREATE TABLE test_stj_location (locid integer);
CREATE TABLE test_stj_blocks (locid integer, ip inet);
CREATE INDEX loc_index ON test_stj_location(locid);

INSERT INTO test_stj_location (locid) VALUES (42);
INSERT INTO test_stj_blocks (locid, ip) VALUES (42, '0.0.0.0');

CREATE CONTINUOUS VIEW test_stj7 AS
SELECT (test_stj_stream.data::jsonb->>'key')::text, avg((test_stj_stream.data::jsonb->>'value')::decimal), test_stj_location.locid
FROM test_stj_stream, test_stj_blocks JOIN test_stj_location USING(locid)
WHERE test_stj_blocks.ip = (test_stj_stream.data::jsonb->>'ip')::inet
GROUP BY  (test_stj_stream.data::jsonb->>'key')::text, test_stj_location.locid;

INSERT INTO test_stj_stream (data) VALUES ('{"key": "1", "value": 42.42, "ip": "0.0.0.0"}');
INSERT INTO test_stj_stream (data) VALUES ('{"key": "1", "value": 420.42, "ip": "0.0.0.0"}');
INSERT INTO test_stj_stream (data) VALUES ('{"key": "2", "value": 4200.42, "ip": "0.0.0.0"}');
INSERT INTO test_stj_stream (data) VALUES ('{"key": "2", "value": 42020.42, "ip": "0.0.0.0"}');

SELECT pg_sleep(0.1);

SELECT * FROM test_stj7;

CREATE CONTINUOUS VIEW test_stj8 AS
SELECT
(test_stj_stream.data::jsonb#>>'{header,game}')::text as game,
(test_stj_stream.data::jsonb#>>'{body,peripheryID}')::text as peripheryID,
to_timestamp((test_stj_stream.data::jsonb#>>'{body,time}')::int) as created_at,
avg((test_stj_stream.data::jsonb#>>'{body,avgRoundTripTime}')::decimal) as avgrtt,
test_stj_location.locid
FROM test_stj_stream, test_stj_blocks JOIN test_stj_location USING(locid)
WHERE test_stj_blocks.ip = (test_stj_stream.data::jsonb#>>'{body,clientIP}')::inet
GROUP BY game, peripheryID, created_at, test_stj_location.locid;

INSERT INTO test_stj_stream (data) VALUES ('{"header": {"ver": 1, "op_type": 402, "op_asset": null, "spa_id": 6333813, "game": "wot", "creation_time": 1427713483, "reserved1": null, "reserved2": null}, "body": {"peripheryID": 101, "arenaID": 2128201, "arenaGeometryID": 24, "arenaGameplayID": 1, "time": 1427712977, "clientIP": "0.0.0.0", "packetsReceived": 6264, "packetsSent": 5059, "packetsResent": 4, "minRoundTripTime": 0.043896623, "maxRoundTripTime": 0.68855155, "avgRoundTripTime": 10}}');
INSERT INTO test_stj_stream (data) VALUES ('{"header": {"ver": 1, "op_type": 402, "op_asset": null, "spa_id": 6333813, "game": "wot", "creation_time": 1427713483, "reserved1": null, "reserved2": null}, "body": {"peripheryID": 101, "arenaID": 2128201, "arenaGeometryID": 24, "arenaGameplayID": 1, "time": 1427712977, "clientIP": "0.0.0.0", "packetsReceived": 6264, "packetsSent": 5059, "packetsResent": 4, "minRoundTripTime": 0.043896623, "maxRoundTripTime": 0.68855155, "avgRoundTripTime": 20}}');
INSERT INTO test_stj_stream (data) VALUES ('{"header": {"ver": 1, "op_type": 402, "op_asset": null, "spa_id": 6333813, "game": "wot", "creation_time": 1427713483, "reserved1": null, "reserved2": null}, "body": {"peripheryID": 101, "arenaID": 2128201, "arenaGeometryID": 24, "arenaGameplayID": 1, "time": 1427712977, "clientIP": "0.0.0.0", "packetsReceived": 6264, "packetsSent": 5059, "packetsResent": 4, "minRoundTripTime": 0.043896623, "maxRoundTripTime": 0.68855155, "avgRoundTripTime": 30}}');

SELECT pg_sleep(0.1);

SELECT * FROM test_stj8;

-- Regression test for join with empty table.
CREATE TABLE test_stj_empty (x int);
CREATE CONTINUOUS VIEW test_stj_empty_join AS SELECT test_stj_empty_stream.x::int FROM test_stj_empty_stream JOIN test_stj_empty ON test_stj_empty_stream.x = test_stj_empty.x;

INSERT INTO test_stj_empty_stream (x) VALUES (0);

SELECT * FROM test_stj_empty_join;

-- STJ involving sliding windows
CREATE STREAM test_stj_sw0_s (x integer, y integer);
CREATE TABLE test_stj_sw0_t AS SELECT x, x AS y FROM generate_series(0, 99) AS x;

CREATE CONTINUOUS VIEW test_stj_sw0 AS SELECT s.x, s.y, t.x AS t_x, count(*)
FROM test_stj_sw0_s s JOIN test_stj_sw0_t t
USING (x)
WHERE (s.arrival_timestamp > clock_timestamp() - interval '4 seconds')
GROUP by s.x, s.y, t_x;

INSERT INTO test_stj_sw0_s (x, y) SELECT x, x AS y FROM generate_series(0, 9) AS x;
INSERT INTO test_stj_sw0_s (x, y) SELECT x, x AS y FROM generate_series(0, 9) AS x;
INSERT INTO test_stj_sw0_s (x, y) SELECT x, x AS y FROM generate_series(0, 9) AS x;

SELECT * FROM test_stj_sw0;

SELECT pg_sleep(5);

SELECT * FROM test_stj_sw0;

DROP CONTINUOUS VIEW test_stj_sw0;
DROP STREAM test_stj_sw0_s;
DROP TABLE test_stj_sw0_t;

CREATE TABLE test_stj_t4 (x integer, y integer, z integer);
CREATE FUNCTION test_stj_foo(integer) RETURNS integer AS $$
BEGIN
  RETURN 0;
END
$$
LANGUAGE plpgsql;
CREATE CONTINUOUS VIEW stj_deps AS SELECT test_stj_foo(s.x::integer), t.x FROM stj_deps_stream s JOIN test_stj_t4 t ON s.x = t.x;

-- Table columns being joined on can't be dropped
ALTER TABLE test_stj_t4 DROP COLUMN x;

-- But unused columns can be
ALTER TABLE test_stj_t4 DROP COLUMN y;

-- Functions used by CVs can't be dropped
DROP FUNCTION test_stj_foo(integer);

DROP CONTINUOUS VIEW stj_deps;

-- Now we can drop everything
ALTER TABLE test_stj_t4 DROP COLUMN x;
DROP TABLE test_stj_t4;
DROP FUNCTION test_stj_foo(integer);

DROP CONTINUOUS VIEW test_stj0;
DROP CONTINUOUS VIEW test_stj1;
DROP CONTINUOUS VIEW test_stj2;
DROP CONTINUOUS VIEW test_stj3;
DROP CONTINUOUS VIEW test_stj4;
DROP CONTINUOUS VIEW test_stj5;
DROP CONTINUOUS VIEW test_stj6;
DROP CONTINUOUS VIEW test_stj7;
DROP CONTINUOUS VIEW test_stj8;
DROP CONTINUOUS VIEW stj_no_tl;
DROP CONTINUOUS VIEW test_stj_empty_join;
DROP TABLE test_stj_t0;
DROP TABLE test_stj_t1;
DROP TABLE test_stj_t2;
DROP TABLE test_stj_t3;
DROP TABLE test_stj_location;
DROP TABLE test_stj_blocks;
DROP TABLE test_stj_empty;
