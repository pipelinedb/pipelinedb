CREATE FOREIGN TABLE regr_stream (x integer, y integer) SERVER pipelinedb;

-- regr_sxx
CREATE VIEW test_regr_sxx AS SELECT regr_sxx(x::float8, y::float8) FROM regr_stream;

-- regr_syy
CREATE VIEW test_regr_syy AS SELECT regr_syy(x::integer, y::integer) FROM regr_stream;

-- regr_sxy
CREATE VIEW test_regr_sxy AS SELECT regr_sxy(x::float4, y::float4) FROM regr_stream;

-- regr_avgx
CREATE VIEW test_regr_avgx AS SELECT regr_avgx(x::numeric, y::numeric) FROM regr_stream;

-- regr_avgy
CREATE VIEW test_regr_avgy AS SELECT regr_avgy(x::integer, y::integer) FROM regr_stream;

-- regr_r2
CREATE VIEW test_regr_r2 AS SELECT regr_r2(x::integer, y::integer) FROM regr_stream;

-- regr_slope
CREATE VIEW test_regr_slope AS SELECT regr_slope(x::float8, y::float8) FROM regr_stream;

-- regr_intercept
CREATE VIEW test_regr_intercept AS SELECT regr_intercept(x::integer, y::integer) FROM regr_stream;

-- regr_count
CREATE VIEW test_regr_count AS SELECT regr_count(x::integer, y::float4) FROM regr_stream;

-- covar_pop
CREATE VIEW test_covar_pop AS SELECT covar_pop(x::float4, y::int4) FROM regr_stream;

-- corr
CREATE VIEW test_corr AS SELECT corr(x::float4, y::int4) FROM regr_stream;

INSERT INTO regr_stream (x, y) VALUES (0, 164), (1, 155), (2, 252), (3, 169), (4, 148), (5, 25), (6, 119), (7, 149), (8, 30), (9, 177), (10, 144), (11, 87), (12, 119), (13, 65), (14, 94), (15, 67), (16, 20), (17, 14), (18, 42), (19, 38), (20, 114), (21, 192), (22, 134), (23, 172), (24, 8), (25, 50), (26, 34), (27, 5), (28, 206), (29, 182), (30, 43), (31, 133), (32, 39), (33, 244), (34, 7), (35, 198), (36, 77), (37, 237), (38, 126), (39, 64), (40, 132), (41, 36), (42, 32), (43, 167), (44, 108), (45, 107), (46, 223), (47, 57), (48, 224), (49, 34), (50, 174), (51, 17), (52, 161), (53, 66), (54, 199), (55, 238), (56, 157), (57, 243), (58, 145), (59, 181), (60, 179), (61, 180), (62, 184), (63, 31), (64, 249), (65, 219), (66, 115), (67, 68), (68, 205), (69, 168), (70, 232), (71, 36), (72, 49), (73, 89), (74, 218), (75, 140), (76, 100), (77, 69), (78, 218), (79, 78), (80, 83), (81, 63), (82, 245), (83, 21), (84, 136), (85, 46), (86, 200), (87, 132), (88, 109), (89, 47), (90, 164), (91, 174), (92, 206), (93, 52), (94, 73), (95, 228), (96, 105), (97, 89), (98, 200), (99, 32);

INSERT INTO regr_stream(x, y) VALUES (100, 137), (101, 228), (102, 141), (103, 138), (104, 113), (105, 12), (106, 196), (107, 70), (108, 83), (109, 222), (110, 55), (111, 92), (112, 1), (113, 219), (114, 136), (115, 160), (116, 223), (117, 73), (118, 237), (119, 218), (120, 23), (121, 61), (122, 12), (123, 212), (124, 100), (125, 185), (126, 221), (127, 105), (128, 152), (129, 66), (130, 102), (131, 11), (132, 49), (133, 15), (134, 29), (135, 187), (136, 148), (137, 161), (138, 142), (139, 114), (140, 241), (141, 144), (142, 186), (143, 124), (144, 79), (145, 140), (146, 142), (147, 242), (148, 52), (149, 61), (150, 30), (151, 254), (152, 233), (153, 30), (154, 51), (155, 104), (156, 166), (157, 224), (158, 190), (159, 51), (160, 150), (161, 236), (162, 251), (163, 112), (164, 13), (165, 153), (166, 102), (167, 102), (168, 205), (169, 139), (170, 98), (171, 235), (172, 164), (173, 254), (174, 5), (175, 76), (176, 96), (177, 86), (178, 188), (179, 142), (180, 160), (181, 5), (182, 123), (183, 162), (184, 171), (185, 70), (186, 71), (187, 26), (188, 57), (189, 140), (190, 186), (191, 175), (192, 118), (193, 51), (194, 62), (195, 187), (196, 88), (197, 170), (198, 226), (199, 13);

SELECT * FROM test_regr_sxx;
SELECT * FROM test_regr_syy;
SELECT * FROM test_regr_sxy;
SELECT * FROM test_regr_avgx;
SELECT * FROM test_regr_avgy;
SELECT * FROM test_regr_r2;
SELECT * FROM test_regr_slope;
SELECT * FROM test_regr_count;
SELECT * FROM test_regr_intercept;
SELECT * FROM test_covar_pop;
SELECT * FROM test_corr;

DROP FOREIGN TABLE regr_stream CASCADE;
