CREATE FOREIGN TABLE test_sw_os_stream (g int, x int, y int, z int) SERVER pipelinedb;

-- percentile_cont
CREATE VIEW test_sw_pc0 AS SELECT percentile_cont(0.23) WITHIN GROUP (ORDER BY x::integer) FROM test_sw_os_stream WHERE (arrival_timestamp > clock_timestamp() - interval '60 second');
CREATE VIEW test_sw_pc1 AS SELECT g::integer, percentile_cont(ARRAY[0, 0.2, 0.4, 0.6, 0.8, 1.0]) WITHIN GROUP (ORDER BY x::integer - y::integer) FROM test_sw_os_stream WHERE (arrival_timestamp > clock_timestamp() - interval '60 second') GROUP BY g;
CREATE VIEW test_sw_pc2 AS SELECT percentile_cont(0.67) WITHIN GROUP (ORDER BY x::integer) + percentile_cont(0.88) WITHIN GROUP (ORDER BY z::integer) FROM test_sw_os_stream WHERE (arrival_timestamp > clock_timestamp() - interval '60 second');

INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (7, 27, -27, '27'), (2, 32, -32, '32'), (4, 4, -4, '4'), (6, 26, -26, '26'), (1, 11, -11, '11'), (0, 60, -60, '60'), (2, 82, -82, '82'), (0, 40, -40, '40'), (9, 19, -19, '19'), (7, 77, -77, '77'), (8, 18, -18, '18'), (9, 99, -99, '99'), (5, 85, -85, '85'), (8, 98, -98, '98'), (7, 57, -57, '57'), (5, 65, -65, '65'), (3, 43, -43, '43'), (0, 0, 0, '0'), (8, 38, -38, '38'), (6, 36, -36, '36'), (3, 83, -83, '83'), (7, 97, -97, '97'), (6, 86, -86, '86'), (9, 29, -29, '29'), (9, 79, -79, '79'), (4, 24, -24, '24'), (6, 46, -46, '46'), (1, 51, -51, '51'), (5, 45, -45, '45'), (0, 30, -30, '30'), (1, 61, -61, '61'), (7, 87, -87, '87'), (5, 5, -5, '5'), (4, 54, -54, '54'), (2, 22, -22, '22'), (5, 55, -55, '55'), (3, 13, -13, '13'), (2, 2, -2, '2'), (8, 58, -58, '58'), (2, 72, -72, '72'), (8, 48, -48, '48'), (4, 74, -74, '74'), (6, 16, -16, '16'), (9, 39, -39, '39'), (3, 53, -53, '53'), (8, 78, -78, '78'), (8, 88, -88, '88'), (3, 93, -93, '93'), (0, 50, -50, '50'), (1, 21, -21, '21'), (4, 34, -34, '34'), (1, 71, -71, '71'), (9, 49, -49, '49'), (7, 47, -47, '47'), (6, 56, -56, '56'), (6, 76, -76, '76'), (3, 73, -73, '73'), (0, 10, -10, '10'), (8, 28, -28, '28'), (2, 52, -52, '52'), (8, 8, -8, '8'), (3, 33, -33, '33'), (1, 41, -41, '41'), (0, 90, -90, '90'), (4, 94, -94, '94'), (4, 64, -64, '64'), (3, 23, -23, '23'), (0, 20, -20, '20'), (9, 69, -69, '69'), (6, 66, -66, '66'), (5, 95, -95, '95'), (6, 6, -6, '6'), (7, 37, -37, '37'), (5, 15, -15, '15'), (4, 84, -84, '84'), (3, 63, -63, '63'), (1, 31, -31, '31'), (2, 62, -62, '62'), (0, 80, -80, '80'), (5, 25, -25, '25'), (3, 3, -3, '3'), (9, 89, -89, '89'), (2, 42, -42, '42'), (8, 68, -68, '68'), (2, 12, -12, '12'), (1, 81, -81, '81'), (4, 14, -14, '14'), (0, 70, -70, '70'), (7, 67, -67, '67'), (2, 92, -92, '92'), (5, 75, -75, '75'), (7, 17, -17, '17'), (7, 7, -7, '7'), (1, 91, -91, '91'), (5, 35, -35, '35'), (9, 59, -59, '59'), (9, 9, -9, '9'), (1, 1, -1, '1'), (6, 96, -96, '96'), (4, 44, -44, '44');

SELECT pg_sleep(1);

INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (5, 5, -5, '5');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (5, 15, -15, '15');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (6, 6, -6, '6');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (0, 10, -10, '10');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (2, 2, -2, '2');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (4, 4, -4, '4');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (9, 9, -9, '9');

SELECT pg_sleep(1);

INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (7, 17, -17, '17');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (3, 3, -3, '3');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (1, 1, -1, '1');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (3, 13, -13, '13');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (8, 18, -18, '18');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (4, 14, -14, '14');

SELECT pg_sleep(1);

INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (7, 7, -7, '7');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (0, 0, 0, '0');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (6, 16, -16, '16');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (1, 11, -11, '11');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (8, 8, -8, '8');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (9, 19, -19, '19');
INSERT INTO test_sw_os_stream (g, x, y, z) VALUES (2, 12, -12, '12');

SELECT pg_sleep(1);

SELECT * FROM test_sw_pc0;
SELECT * FROM test_sw_pc1 ORDER BY g;
SELECT * FROM test_sw_pc2;

DROP FOREIGN TABLE test_sw_os_stream CASCADE;
