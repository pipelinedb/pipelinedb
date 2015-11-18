-------------------------------------------------------------------------------
-- Integer averages
CREATE CONTINUOUS VIEW test_int8_avg AS SELECT k::text, AVG(v::int8) FROM int_stream_cqavg GROUP BY k;
CREATE CONTINUOUS VIEW test_int4_avg AS SELECT k::text, AVG(v::int4) FROM int_stream_cqavg GROUP BY k;
CREATE CONTINUOUS VIEW test_int2_avg AS SELECT k::text, AVG(v::int2) FROM int_stream_cqavg GROUP BY k;

INSERT INTO int_stream_cqavg (k, v) VALUES ('x', 1), ('x', 1), ('x', 1);
INSERT INTO int_stream_cqavg (k, v) VALUES ('y', -10000), ('y', 10000), ('y', 0);

SELECT * FROM test_int4_avg_mrel ORDER BY k;
SELECT * FROM test_int2_avg_mrel ORDER BY k;

INSERT INTO int_stream_cqavg (k, v) VALUES ('x', 2), ('x', 2), ('x', 2);
INSERT INTO int_stream_cqavg (k, v) VALUES ('y', 1), ('y', 10000), ('y', 2000);

SELECT * FROM test_int8_avg ORDER BY k;
SELECT * FROM test_int4_avg ORDER BY k;
SELECT * FROM test_int2_avg ORDER BY k;

-------------------------------------------------------------------------------
-- Float averages

CREATE CONTINUOUS VIEW test_float8_avg AS SELECT k::text, AVG(v::float8) FROM float_stream_cqavg GROUP BY k;
CREATE CONTINUOUS VIEW test_float4_avg AS SELECT k::text, AVG(v::float4) FROM float_stream_cqavg GROUP BY k;

INSERT INTO float_stream_cqavg (k, v) VALUES ('x', 1e6), ('x', -1e6), ('x', 10.0000001);
INSERT INTO float_stream_cqavg (k, v) VALUES ('y', 0.0001), ('y', 0.00002), ('y', 155321233.1);

SELECT k, round(avg::numeric, 5) FROM test_float8_avg ORDER BY k;
SELECT k, round(avg::numeric, 5) FROM test_float4_avg ORDER BY k;

INSERT INTO float_stream_cqavg (k, v) VALUES ('x', -1e5), ('x', 1e6), ('x', -10.0000001);
INSERT INTO float_stream_cqavg (k, v) VALUES ('z', 42.42);

SELECT k, round(avg::numeric, 5) FROM test_float8_avg ORDER BY k;
SELECT k, round(avg::numeric, 5) FROM test_float4_avg ORDER BY k;

-------------------------------------------------------------------------------
-- Numeric averages
CREATE CONTINUOUS VIEW test_numeric_avg AS SELECT k::text, AVG(v::numeric) FROM numeric_stream_cqavg GROUP BY k;

INSERT INTO numeric_stream_cqavg (k, v) VALUES ('x', 10000000000000000.233), ('x', -1.000000000333), ('x', 0.00000000001);
INSERT INTO numeric_stream_cqavg (k, v) VALUES ('y', 0.1001), ('y', 0.99999999), ('y', -999999999999999999.999999999999);

INSERT INTO numeric_stream_cqavg (k, v) VALUES ('x', 1), ('y', 2), ('z', 42.42);

SELECT * FROM test_numeric_avg ORDER BY k;

-------------------------------------------------------------------------------
-- Interval averages
CREATE CONTINUOUS VIEW test_interval_avg AS SELECT k::text, AVG(date_trunc('day', ts1::timestamp) - date_trunc('day', ts0::timestamp)) FROM interval_stream_cqavg GROUP BY k;

INSERT INTO interval_stream_cqavg (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 23:00:00');
INSERT INTO interval_stream_cqavg (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 01:00:00');
INSERT INTO interval_stream_cqavg (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 11:00:00');

-- We truncated down to day, so the hours shouldn't have affected the averages
SELECT * FROM test_interval_avg_mrel ORDER BY k;

INSERT INTO interval_stream_cqavg (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-04 04:00:00');
INSERT INTO interval_stream_cqavg (k, ts0, ts1) VALUES ('y', '2014-01-01 23:00:00', '2014-01-02 00:00:00');

SELECT * FROM test_interval_avg ORDER BY k;

DROP CONTINUOUS VIEW test_int8_avg;
DROP CONTINUOUS VIEW test_int4_avg;
DROP CONTINUOUS VIEW test_int2_avg;
DROP CONTINUOUS VIEW test_float8_avg;
DROP CONTINUOUS VIEW test_float4_avg;
DROP CONTINUOUS VIEW test_numeric_avg;
DROP CONTINUOUS VIEW test_interval_avg;
