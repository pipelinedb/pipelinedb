CREATE STREAM test_uc_stream (k text, x int, s text, y int);

-- Verify some validation
CREATE CONTINUOUS VIEW test_uc_validation AS SELECT k::text, avg(x::integer) FROM test_uc_stream GROUP BY k;
CREATE TABLE test_uc_table (v numeric);
INSERT INTO test_uc_table (v) VALUES (0), (1), (2);

-- combine only accepts a single colref as an argument
SELECT combine(avg + 1) FROM test_uc_validation;
SELECT combine(avg, avg) FROM test_uc_validation;

-- combine isn't allowed on tables
SELECT combine(v) FROM test_uc_table;

-- combine is only allowed on aggregate columns
SELECT combine(k) FROM test_uc_validation;

-- Column doesn't exist
SELECT combine(nothere) FROM test_uc_validation;

DROP TABLE test_uc_table;
DROP CONTINUOUS VIEW test_uc_validation;

CREATE CONTINUOUS VIEW test_uc0 AS SELECT
s::text,
avg(x::numeric),
sum(y::int),
json_object_agg(x, y),
array_agg(x),
max(x),
min(y),
string_agg(substring(s, 1, 1), ' :: ')
FROM test_uc_stream GROUP BY s;

CREATE CONTINUOUS VIEW test_uc1 AS SELECT
s::text,
dense_rank('20') WITHIN GROUP (ORDER BY s) AS expr0,
regr_r2(x::integer, y::integer) AS expr1
FROM test_uc_stream GROUP BY s;

CREATE STREAM test_uc_systat_stream (t timestamptz, queue_length int);
CREATE CONTINUOUS VIEW test_uc2 AS
SELECT date_trunc('minute', t::timestamptz) AS minute,
         avg(queue_length::integer) AS load_avg
    FROM test_uc_systat_stream
WHERE arrival_timestamp > (clock_timestamp() - interval '1 hour')
GROUP BY minute;

CREATE VIEW test_uc2_view AS
SELECT minute, combine(load_avg) OVER (ORDER BY minute DESC ROWS 4 PRECEDING) AS load_avg
  FROM test_uc2;

INSERT INTO test_uc_stream (x, y, s) VALUES (0, 0, '0');
INSERT INTO test_uc_stream (x, y, s) VALUES (1, 100, '1');
INSERT INTO test_uc_stream (x, y, s) VALUES (2, 200, '2');
INSERT INTO test_uc_stream (x, y, s) VALUES (3, 300, '3');
INSERT INTO test_uc_stream (x, y, s) VALUES (4, 400, '4');
INSERT INTO test_uc_stream (x, y, s) VALUES (5, 500, '5');
INSERT INTO test_uc_stream (x, y, s) VALUES (6, 600, '6');
INSERT INTO test_uc_stream (x, y, s) VALUES (7, 700, '7');
INSERT INTO test_uc_stream (x, y, s) VALUES (8, 800, '8');
INSERT INTO test_uc_stream (x, y, s) VALUES (9, 900, '9');
INSERT INTO test_uc_stream (x, y, s) VALUES (10, 1000, '10');
INSERT INTO test_uc_stream (x, y, s) VALUES (11, 1100, '11');
INSERT INTO test_uc_stream (x, y, s) VALUES (12, 1200, '12');
INSERT INTO test_uc_stream (x, y, s) VALUES (13, 1300, '13');
INSERT INTO test_uc_stream (x, y, s) VALUES (14, 1400, '14');
INSERT INTO test_uc_stream (x, y, s) VALUES (15, 1500, '15');
INSERT INTO test_uc_stream (x, y, s) VALUES (16, 1600, '16');
INSERT INTO test_uc_stream (x, y, s) VALUES (17, 1700, '17');
INSERT INTO test_uc_stream (x, y, s) VALUES (18, 1800, '18');
INSERT INTO test_uc_stream (x, y, s) VALUES (19, 1900, '19');
INSERT INTO test_uc_stream (x, y, s) VALUES (20, 2000, '20');
INSERT INTO test_uc_stream (x, y, s) VALUES (21, 2100, '21');
INSERT INTO test_uc_stream (x, y, s) VALUES (22, 2200, '22');
INSERT INTO test_uc_stream (x, y, s) VALUES (23, 2300, '23');
INSERT INTO test_uc_stream (x, y, s) VALUES (24, 2400, '24');
INSERT INTO test_uc_stream (x, y, s) VALUES (25, 2500, '25');
INSERT INTO test_uc_stream (x, y, s) VALUES (26, 2600, '26');
INSERT INTO test_uc_stream (x, y, s) VALUES (27, 2700, '27');
INSERT INTO test_uc_stream (x, y, s) VALUES (28, 2800, '28');
INSERT INTO test_uc_stream (x, y, s) VALUES (29, 2900, '29');
INSERT INTO test_uc_stream (x, y, s) VALUES (30, 3000, '30');
INSERT INTO test_uc_stream (x, y, s) VALUES (31, 3100, '31');
INSERT INTO test_uc_stream (x, y, s) VALUES (32, 3200, '32');
INSERT INTO test_uc_stream (x, y, s) VALUES (33, 3300, '33');
INSERT INTO test_uc_stream (x, y, s) VALUES (34, 3400, '34');
INSERT INTO test_uc_stream (x, y, s) VALUES (35, 3500, '35');
INSERT INTO test_uc_stream (x, y, s) VALUES (36, 3600, '36');
INSERT INTO test_uc_stream (x, y, s) VALUES (37, 3700, '37');
INSERT INTO test_uc_stream (x, y, s) VALUES (38, 3800, '38');
INSERT INTO test_uc_stream (x, y, s) VALUES (39, 3900, '39');
INSERT INTO test_uc_stream (x, y, s) VALUES (40, 4000, '40');
INSERT INTO test_uc_stream (x, y, s) VALUES (41, 4100, '41');
INSERT INTO test_uc_stream (x, y, s) VALUES (42, 4200, '42');
INSERT INTO test_uc_stream (x, y, s) VALUES (43, 4300, '43');
INSERT INTO test_uc_stream (x, y, s) VALUES (44, 4400, '44');
INSERT INTO test_uc_stream (x, y, s) VALUES (45, 4500, '45');
INSERT INTO test_uc_stream (x, y, s) VALUES (46, 4600, '46');
INSERT INTO test_uc_stream (x, y, s) VALUES (47, 4700, '47');
INSERT INTO test_uc_stream (x, y, s) VALUES (48, 4800, '48');
INSERT INTO test_uc_stream (x, y, s) VALUES (49, 4900, '49');
INSERT INTO test_uc_stream (x, y, s) VALUES (50, 5000, '0');
INSERT INTO test_uc_stream (x, y, s) VALUES (51, 5100, '1');
INSERT INTO test_uc_stream (x, y, s) VALUES (52, 5200, '2');
INSERT INTO test_uc_stream (x, y, s) VALUES (53, 5300, '3');
INSERT INTO test_uc_stream (x, y, s) VALUES (54, 5400, '4');
INSERT INTO test_uc_stream (x, y, s) VALUES (55, 5500, '5');
INSERT INTO test_uc_stream (x, y, s) VALUES (56, 5600, '6');
INSERT INTO test_uc_stream (x, y, s) VALUES (57, 5700, '7');
INSERT INTO test_uc_stream (x, y, s) VALUES (58, 5800, '8');
INSERT INTO test_uc_stream (x, y, s) VALUES (59, 5900, '9');
INSERT INTO test_uc_stream (x, y, s) VALUES (60, 6000, '10');
INSERT INTO test_uc_stream (x, y, s) VALUES (61, 6100, '11');
INSERT INTO test_uc_stream (x, y, s) VALUES (62, 6200, '12');
INSERT INTO test_uc_stream (x, y, s) VALUES (63, 6300, '13');
INSERT INTO test_uc_stream (x, y, s) VALUES (64, 6400, '14');
INSERT INTO test_uc_stream (x, y, s) VALUES (65, 6500, '15');
INSERT INTO test_uc_stream (x, y, s) VALUES (66, 6600, '16');
INSERT INTO test_uc_stream (x, y, s) VALUES (67, 6700, '17');
INSERT INTO test_uc_stream (x, y, s) VALUES (68, 6800, '18');
INSERT INTO test_uc_stream (x, y, s) VALUES (69, 6900, '19');
INSERT INTO test_uc_stream (x, y, s) VALUES (70, 7000, '20');
INSERT INTO test_uc_stream (x, y, s) VALUES (71, 7100, '21');
INSERT INTO test_uc_stream (x, y, s) VALUES (72, 7200, '22');
INSERT INTO test_uc_stream (x, y, s) VALUES (73, 7300, '23');
INSERT INTO test_uc_stream (x, y, s) VALUES (74, 7400, '24');
INSERT INTO test_uc_stream (x, y, s) VALUES (75, 7500, '25');
INSERT INTO test_uc_stream (x, y, s) VALUES (76, 7600, '26');
INSERT INTO test_uc_stream (x, y, s) VALUES (77, 7700, '27');
INSERT INTO test_uc_stream (x, y, s) VALUES (78, 7800, '28');
INSERT INTO test_uc_stream (x, y, s) VALUES (79, 7900, '29');
INSERT INTO test_uc_stream (x, y, s) VALUES (80, 8000, '30');
INSERT INTO test_uc_stream (x, y, s) VALUES (81, 8100, '31');
INSERT INTO test_uc_stream (x, y, s) VALUES (82, 8200, '32');
INSERT INTO test_uc_stream (x, y, s) VALUES (83, 8300, '33');
INSERT INTO test_uc_stream (x, y, s) VALUES (84, 8400, '34');
INSERT INTO test_uc_stream (x, y, s) VALUES (85, 8500, '35');
INSERT INTO test_uc_stream (x, y, s) VALUES (86, 8600, '36');
INSERT INTO test_uc_stream (x, y, s) VALUES (87, 8700, '37');
INSERT INTO test_uc_stream (x, y, s) VALUES (88, 8800, '38');
INSERT INTO test_uc_stream (x, y, s) VALUES (89, 8900, '39');
INSERT INTO test_uc_stream (x, y, s) VALUES (90, 9000, '40');
INSERT INTO test_uc_stream (x, y, s) VALUES (91, 9100, '41');
INSERT INTO test_uc_stream (x, y, s) VALUES (92, 9200, '42');
INSERT INTO test_uc_stream (x, y, s) VALUES (93, 9300, '43');
INSERT INTO test_uc_stream (x, y, s) VALUES (94, 9400, '44');
INSERT INTO test_uc_stream (x, y, s) VALUES (95, 9500, '45');
INSERT INTO test_uc_stream (x, y, s) VALUES (96, 9600, '46');
INSERT INTO test_uc_stream (x, y, s) VALUES (97, 9700, '47');
INSERT INTO test_uc_stream (x, y, s) VALUES (98, 9800, '48');
INSERT INTO test_uc_stream (x, y, s) VALUES (99, 9900, '49');

INSERT INTO test_uc_stream (x, y, s) VALUES (100, 1000, '0'), (101, 1010, '1'), (102, 1020, '2'), (103, 1030, '3'), (104, 1040, '4'), (105, 1050, '5'), (106, 1060, '6'), (107, 1070, '7'), (108, 1080, '8'), (109, 1090, '9'), (110, 1100, '10'), (111, 1110, '11'), (112, 1120, '12'), (113, 1130, '13'), (114, 1140, '14'), (115, 1150, '15'), (116, 1160, '16'), (117, 1170, '17'), (118, 1180, '18'), (119, 1190, '19'), (120, 1200, '20'), (121, 1210, '21'), (122, 1220, '22'), (123, 1230, '23'), (124, 1240, '24'), (125, 1250, '25'), (126, 1260, '26'), (127, 1270, '27'), (128, 1280, '28'), (129, 1290, '29'), (130, 1300, '30'), (131, 1310, '31'), (132, 1320, '32'), (133, 1330, '33'), (134, 1340, '34'), (135, 1350, '35'), (136, 1360, '36'), (137, 1370, '37'), (138, 1380, '38'), (139, 1390, '39'), (140, 1400, '40'), (141, 1410, '41'), (142, 1420, '42'), (143, 1430, '43'), (144, 1440, '44'), (145, 1450, '45'), (146, 1460, '46'), (147, 1470, '47'), (148, 1480, '48'), (149, 1490, '49'), (150, 1500, '0'), (151, 1510, '1'), (152, 1520, '2'), (153, 1530, '3'), (154, 1540, '4'), (155, 1550, '5'), (156, 1560, '6'), (157, 1570, '7'), (158, 1580, '8'), (159, 1590, '9'), (160, 1600, '10'), (161, 1610, '11'), (162, 1620, '12'), (163, 1630, '13'), (164, 1640, '14'), (165, 1650, '15'), (166, 1660, '16'), (167, 1670, '17'), (168, 1680, '18'), (169, 1690, '19'), (170, 1700, '20'), (171, 1710, '21'), (172, 1720, '22'), (173, 1730, '23'), (174, 1740, '24'), (175, 1750, '25'), (176, 1760, '26'), (177, 1770, '27'), (178, 1780, '28'), (179, 1790, '29'), (180, 1800, '30'), (181, 1810, '31'), (182, 1820, '32'), (183, 1830, '33'), (184, 1840, '34'), (185, 1850, '35'), (186, 1860, '36'), (187, 1870, '37'), (188, 1880, '38'), (189, 1890, '39'), (190, 1900, '40'), (191, 1910, '41'), (192, 1920, '42'), (193, 1930, '43'), (194, 1940, '44'), (195, 1950, '45'), (196, 1960, '46'), (197, 1970, '47'), (198, 1980, '48'), (199, 1990, '49');

INSERT INTO test_uc_systat_stream (t, queue_length) VALUES ('2015-03-25T07:52:52Z', 1),
('2015-03-25T07:53:53Z', 1),
('2015-03-25T07:54:53Z', 2),
('2015-03-25T07:55:54Z', 2),
('2015-03-25T07:56:54Z', 3),
('2015-03-25T07:57:54Z', 4);

-- Verify that table-wide combines work
SELECT combine(avg) FROM test_uc0;
SELECT combine(sum) FROM test_uc0;
SELECT combine(max) FROM test_uc0;
SELECT combine(min) FROM test_uc0;

-- Verify the lengths for these since the ordering is nondeterministic and they're really long
SELECT json_object_keys(combine(json_object_agg)) AS k FROM test_uc0 ORDER BY k;
SELECT array_length(combine(array_agg), 1) FROM test_uc0;
SELECT length(combine(string_agg)) FROM test_uc0;

-- Verify that subsets of rows are combined properly
SELECT s, combine(avg) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;
SELECT s, combine(sum) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;
SELECT s, combine(max) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;
SELECT s, combine(min) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;

-- Verify the lengths for these since the ordering is nondeterministic and they're really long
SELECT s, json_object_keys(combine(json_object_agg)) AS k FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s, k;
SELECT array_length(combine(array_agg), 1) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;
SELECT length(combine(string_agg)) FROM test_uc0 WHERE s > '25' GROUP BY s ORDER BY s;

-- Verify that table-wide combines work
SELECT combine(expr0) FROM test_uc1;
SELECT combine(expr1) FROM test_uc1;

-- Verify that subsets of rows are combined properly
SELECT s, combine(expr0) FROM test_uc1 WHERE s < '25' GROUP BY s ORDER BY s;
SELECT s, combine(expr1) FROM test_uc1 WHERE s < '25' GROUP BY s ORDER BY s;

-- Verify that combines work with CV-CV joins
SELECT combine(avg),
combine(sum),
combine(max),
combine(min),
combine(expr0),
combine(expr1)
FROM test_uc0 v0 JOIN test_uc1 v1 ON v0.s = v1.s;

-- Verify that combines work with subsets of CV-CV joins
SELECT v0.s,
combine(avg),
combine(sum),
combine(max),
combine(min),
combine(expr0),
combine(expr1)
FROM test_uc0 v0 JOIN test_uc1 v1 ON v0.s = v1.s WHERE v0.s IN ('0', '1', '2') AND v1.s IN ('0', '1', '2')
GROUP BY v0.s;

CREATE TABLE test_uc_table1 (x integer, y integer, s text);

INSERT INTO test_uc_table1 (x, y, s) VALUES (100, 1000, '0'), (101, 1010, '1'), (102, 1020, '2'), (103, 1030, '3'), (104, 1040, '4'), (105, 1050, '5'), (106, 1060, '6'), (107, 1070, '7'), (108, 1080, '8'), (109, 1090, '9'), (110, 1100, '10'), (111, 1110, '11'), (112, 1120, '12'), (113, 1130, '13'), (114, 1140, '14'), (115, 1150, '15'), (116, 1160, '16'), (117, 1170, '17'), (118, 1180, '18'), (119, 1190, '19'), (120, 1200, '20'), (121, 1210, '21'), (122, 1220, '22'), (123, 1230, '23'), (124, 1240, '24'), (125, 1250, '25'), (126, 1260, '26'), (127, 1270, '27'), (128, 1280, '28'), (129, 1290, '29'), (130, 1300, '30'), (131, 1310, '31'), (132, 1320, '32'), (133, 1330, '33'), (134, 1340, '34'), (135, 1350, '35'), (136, 1360, '36'), (137, 1370, '37'), (138, 1380, '38'), (139, 1390, '39'), (140, 1400, '40'), (141, 1410, '41'), (142, 1420, '42'), (143, 1430, '43'), (144, 1440, '44'), (145, 1450, '45'), (146, 1460, '46'), (147, 1470, '47'), (148, 1480, '48'), (149, 1490, '49');

-- Verify that combines work on CV-table joins
SELECT combine(avg),
combine(sum),
combine(max),
combine(min),
combine(expr0),
combine(expr1),
sum(x),
sum(y)
FROM test_uc0 v0 JOIN test_uc1 v1 ON v0.s = v1.s JOIN test_uc_table1 t0 ON v0.s = t0.s;

-- Verify that combines work on subsets of CV-table joins
SELECT v0.s, combine(expr0), combine(avg) FROM test_uc0 v0
JOIN test_uc1 v1 ON v0.s = v1.s JOIN test_uc_table1 t0 ON v0.s = t0.s
WHERE v0.s > '25' GROUP BY v0.s ORDER BY v0.s;

-- Verify that views containing combines can be created on top of continuous views
SELECT * FROM test_uc2 ORDER BY minute;
SELECT * FROM test_uc2_view ORDER BY minute;

DROP CONTINUOUS VIEW test_uc0;
DROP CONTINUOUS VIEW test_uc1;
DROP TABLE test_uc_table1;
DROP VIEW test_uc2_view;
DROP CONTINUOUS VIEW test_uc2;

CREATE STREAM sysstat (t timestamptz, queue_length float);
CREATE CONTINUOUS VIEW over_1m AS
SELECT date_trunc('minute', t::timestamptz) AS minute, avg(queue_length::float) AS load_avg
FROM sysstat
WHERE arrival_timestamp > clock_timestamp() - interval '1 hour'
GROUP BY minute;

CREATE VIEW over_5m AS
SELECT first_value(minute) OVER w AS minute, combine(load_avg) OVER w AS load_avg
FROM over_1m
WINDOW w AS (ORDER BY minute DESC ROWS 4 PRECEDING);

\d+ over_5m;

DROP VIEW over_5m;
DROP CONTINUOUS VIEW over_1m;

CREATE CONTINUOUS VIEW test_uc3 AS SELECT x, avg(x::numeric), count(distinct s) FROM test_uc_stream GROUP BY x;
INSERT INTO test_uc_stream (x, y, s) SELECT x % 20, x % 10, (x % 5)::text AS s FROM generate_series(1, 100) AS x;
SELECT combine(avg) + combine(avg) + combine(avg), combine(avg), combine(avg), combine(count), combine(count) FROM test_uc3;
SELECT combine(avg) + combine(avg) + combine(avg), combine(avg), combine(avg), combine(count), combine(count) FROM test_uc3 GROUP BY x ORDER BY x;
DROP CONTINUOUS VIEW test_uc3;

DROP STREAM test_uc_stream CASCADE;
DROP STREAM test_uc_systat_stream CASCADE;
DROP STREAM sysstat CASCADE;
