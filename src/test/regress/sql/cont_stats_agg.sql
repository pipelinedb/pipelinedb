CREATE FOREIGN TABLE stats_stream (x int) SERVER pipelinedb;

-- variance
-- Note: variance and var_samp are the same thing.
-- variance is the old form of var_samp, but we want to make
-- sure we support both so we test both forms here.
CREATE VIEW test_int8_var_samp AS SELECT var_samp(x::int8), variance(x) FROM stats_stream;
CREATE VIEW test_int4_var_samp AS SELECT var_samp(x::int4), variance(x) FROM stats_stream;
CREATE VIEW test_int2_var_samp AS SELECT var_samp(x::int2), variance(x) FROM stats_stream;
CREATE VIEW test_numeric_var_samp AS SELECT var_samp(x::numeric), variance(x) FROM stats_stream;
CREATE VIEW test_float8_var_samp AS SELECT var_samp(x::float8), variance(x) FROM stats_stream;
CREATE VIEW test_float4_var_samp AS SELECT var_samp(x::float4), variance(x) FROM stats_stream;

-- population variance
CREATE VIEW test_int8_var_pop AS SELECT var_pop(x::int8) FROM stats_stream;
CREATE VIEW test_int4_var_pop AS SELECT var_pop(x::int4) FROM stats_stream;
CREATE VIEW test_int2_var_pop AS SELECT var_pop(x::int2) FROM stats_stream;
CREATE VIEW test_numeric_var_pop AS SELECT var_pop(x::numeric) FROM stats_stream;
CREATE VIEW test_float8_var_pop AS SELECT var_pop(x::float8) FROM stats_stream;
CREATE VIEW test_float4_var_pop AS SELECT var_pop(x::float4) FROM stats_stream;

-- standard deviation
-- Note: stddev and stddev_samp are the same thing.
-- stddev is the old form of stddev_samp, but we want to make
-- sure we support both so we test both forms here.
CREATE VIEW test_int8_stddev_samp AS SELECT stddev_samp(x::int8), stddev(x) FROM stats_stream;
CREATE VIEW test_int4_stddev_samp AS SELECT stddev_samp(x::int4), stddev(x) FROM stats_stream;
CREATE VIEW test_int2_stddev_samp AS SELECT stddev_samp(x::int2), stddev(x) FROM stats_stream;
CREATE VIEW test_numeric_stddev_samp AS SELECT stddev_samp(x::numeric), stddev(x) FROM stats_stream;
CREATE VIEW test_float8_stddev_samp AS SELECT stddev_samp(x::float8), stddev(x) FROM stats_stream;
CREATE VIEW test_float4_stddev_samp AS SELECT stddev_samp(x::float4), stddev(x) FROM stats_stream;

-- population standard deviation
CREATE VIEW test_int8_stddev_pop AS SELECT stddev_pop(x::int8) FROM stats_stream;
CREATE VIEW test_int4_stddev_pop AS SELECT stddev_pop(x::int4) FROM stats_stream;
CREATE VIEW test_int2_stddev_pop AS SELECT stddev_pop(x::int2) FROM stats_stream;
CREATE VIEW test_numeric_stddev_pop AS SELECT stddev_pop(x::numeric) FROM stats_stream;
CREATE VIEW test_float8_stddev_pop AS SELECT stddev_pop(x::float8) FROM stats_stream;
CREATE VIEW test_float4_stddev_pop AS SELECT stddev_pop(x::float4) FROM stats_stream;

INSERT INTO stats_stream (x) VALUES (665), (982), (976), (274), (621), (83), (959), (178), (83), (680), (529), (554), (311), (222), (217), (883), (359), (973), (6), (859), (729), (415), (431), (369), (684), (464), (607), (567), (852), (945), (11), (178), (703), (709), (493), (352), (371), (432), (933), (361), (765), (210), (16), (590), (432), (263), (665), (315), (150), (556), (309), (233), (259), (826), (301), (529), (117), (27), (337), (84), (638), (64), (82), (347), (326), (189), (880), (4), (365), (608), (1000), (592), (945), (90), (444), (812), (71), (231), (999), (631), (500), (752), (664), (297), (507), (321), (881), (725), (211), (16), (285), (934), (602), (329), (93), (793), (912), (511), (408), (736);

INSERT INTO stats_stream (x) VALUES (210), (32), (122), (551), (331), (434), (802), (319), (112), (493), (763), (498), (3), (275), (60), (80), (752), (994), (3), (287), (79), (748), (575), (483), (701), (567), (716), (487), (604), (992), (61), (731), (881), (683), (208), (62), (665), (546), (865), (156), (792), (883), (250), (186), (870), (832), (854), (436), (394), (469), (145), (633), (150), (211), (568), (53), (414), (883), (429), (173), (855), (25), (234), (989), (289), (232), (471), (859), (152), (820), (139), (988), (748), (892), (684), (461), (135), (972), (537), (948), (695), (373), (118), (330), (919), (475), (548), (178), (165), (387), (882), (867), (379), (325), (925), (929), (312), (70), (672), (19);

SELECT * FROM test_int8_var_samp;
SELECT * FROM test_int4_var_samp;
SELECT * FROM test_int2_var_samp;
SELECT * FROM test_numeric_var_samp;
SELECT * FROM test_float8_var_samp;
SELECT * FROM test_float4_var_samp;
SELECT * FROM test_int8_var_pop;
SELECT * FROM test_int4_var_pop;
SELECT * FROM test_int2_var_pop;
SELECT * FROM test_numeric_var_pop;
SELECT * FROM test_float8_var_pop;
SELECT * FROM test_float4_var_pop;
SELECT * FROM test_int8_stddev_samp;
SELECT * FROM test_int4_stddev_samp;
SELECT * FROM test_int2_stddev_samp;
SELECT * FROM test_numeric_stddev_samp;
SELECT * FROM test_float8_stddev_samp;
SELECT * FROM test_float4_stddev_samp;
SELECT * FROM test_int8_stddev_pop;
SELECT * FROM test_int4_stddev_pop;
SELECT * FROM test_int2_stddev_pop;
SELECT * FROM test_numeric_stddev_pop;
SELECT * FROM test_float8_stddev_pop;
SELECT * FROM test_float4_stddev_pop;

DROP FOREIGN TABLE stats_stream CASCADE;
