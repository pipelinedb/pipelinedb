SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW test_hll_count AS SELECT COUNT(DISTINCT x::integer) FROM test_hll_count_stream;
CREATE CONTINUOUS VIEW test_hll_sw_count AS SELECT COUNT(DISTINCT x::integer) FROM test_hll_count_stream WHERE (arrival_timestamp > clock_timestamp() - interval '10 seconds');
CREATE CONTINUOUS VIEW test_hll_sw_count_small AS SELECT COUNT(DISTINCT x::integer) FROM test_hll_count_stream WHERE (arrival_timestamp > clock_timestamp() - interval '1 second');;

ACTIVATE test_hll_count, test_hll_sw_count, test_hll_sw_count_small;

INSERT INTO test_hll_count_stream (x, y, z) VALUES (1, 1, '1'), (1, 1, '1'), (4, 4, '4'), (7, 7, '7'), (0, 0, '0'), (2, 2, '2'), (4, 4, '4'), (8, 8, '8'), (8, 8, '8'), (7, 7, '7'), (9, 9, '9'), (6, 6, '6'), (2, 2, '2'), (6, 6, '6'), (5, 5, '5'), (3, 3, '3'), (9, 9, '9'), (0, 0, '0'), (5, 5, '5'), (3, 3, '3');
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('1', 1, 1);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('9', 9, 9);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('9', 9, 9);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('0', 0, 0);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('3', 3, 3);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('4', 4, 4);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('5', 5, 5);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('7', 7, 7);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('0', 0, 0);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('2', 2, 2);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('2', 2, 2);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('3', 3, 3);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('6', 6, 6);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('7', 7, 7);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('6', 6, 6);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('1', 1, 1);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('6', 6, 6);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('7', 7, 7);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('4', 4, 4);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('5', 5, 5);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('4', 4, 4);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('8', 8, 8);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('1', 1, 1);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('8', 8, 8);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('5', 5, 5);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('8', 8, 8);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('3', 3, 3);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('9', 9, 9);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('2', 2, 2);
INSERT INTO test_hll_count_stream (z, y, x) VALUES ('0', 0, 0);
INSERT INTO test_hll_count_stream (x, y, z) VALUES (null, null, null);
INSERT INTO test_hll_count_stream (x, y, z) VALUES (null, null, null);
INSERT INTO test_hll_count_stream (x, y, z) VALUES (null, null, null);
INSERT INTO test_hll_count_stream (z, y, x) VALUES (null, null, null);
INSERT INTO test_hll_count_stream (z, y, x) VALUES (null, null, null);
INSERT INTO test_hll_count_stream (z, y, x) VALUES (null, null, null);

DEACTIVATE test_hll_count, test_hll_sw_count, test_hll_sw_count_small;

SELECT * FROM test_hll_count;
\d+ test_hll_count_mrel0;

SELECT * FROM test_hll_sw_count;
\d+ test_hll_sw_count_mrel0;

SELECT pg_sleep(1);
SELECT * FROM test_hll_sw_count_small;

DROP CONTINUOUS VIEW test_hll_count;
DROP CONTINUOUS VIEW test_hll_sw_count;
DROP CONTINUOUS VIEW test_hll_sw_count_small;
