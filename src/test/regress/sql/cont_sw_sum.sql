-------------------------------------------------------------------------------
-- Integer sums
CREATE FOREIGN TABLE int_stream_cqswsum (k text, v int8) SERVER pipelinedb;

CREATE VIEW test_sw_int8_sum AS SELECT k::text, SUM(v::int8) FROM int_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_sw_int4_sum AS SELECT k::text, SUM(v::int4) FROM int_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_sw_int2_sum AS SELECT k::text, SUM(v::int2) FROM int_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO int_stream_cqswsum (k, v) VALUES ('x', 10), ('x', 10), ('x', 10);
INSERT INTO int_stream_cqswsum (k, v) VALUES ('y', 10), ('y', 10), ('y', 10);

SELECT * FROM test_sw_int8_sum ORDER BY k;
SELECT * FROM test_sw_int4_sum ORDER BY k;
SELECT * FROM test_sw_int2_sum ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO int_stream_cqswsum (k, v) VALUES ('x', 1), ('x', 2), ('x', 3);
INSERT INTO int_stream_cqswsum (k, v) VALUES ('y', 10), ('y', 10), ('z', 10);

SELECT * FROM test_sw_int8_sum ORDER BY k;
SELECT * FROM test_sw_int4_sum ORDER BY k;
SELECT * FROM test_sw_int2_sum ORDER BY k;

DROP FOREIGN TABLE int_stream_cqswsum CASCADE;

-------------------------------------------------------------------------------
-- Float sums
CREATE FOREIGN TABLE float_stream_cqswsum (k text, v float8) SERVER pipelinedb;

CREATE VIEW test_sw_float8_sum AS SELECT k::text, SUM(v::float8) FROM float_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_sw_float4_sum AS SELECT k::text, SUM(v::float4) FROM float_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO float_stream_cqswsum (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 0.000001);
INSERT INTO float_stream_cqswsum (k, v) VALUES ('y', -10.3), ('y', 1.2e6), ('y', '100.4');

SELECT * FROM test_sw_float8_sum ORDER BY k;
SELECT * FROM test_sw_float4_sum ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO float_stream_cqswsum (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 1.2e-4);
INSERT INTO float_stream_cqswsum (k, v) VALUES ('y', -10.3), ('y', 1.2e6), ('y', '100.4');

SELECT * FROM test_sw_float8_sum ORDER BY k;
SELECT * FROM test_sw_float4_sum ORDER BY k;

DROP FOREIGN TABLE float_stream_cqswsum CASCADE;

-------------------------------------------------------------------------------
-- Cash sums
CREATE FOREIGN TABLE cash_stream_cqswsum (k text, v money) SERVER pipelinedb;

CREATE VIEW test_sw_cash_sum AS SELECT k::text, SUM(v::money) FROM cash_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO cash_stream_cqswsum (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 0.10);
INSERT INTO cash_stream_cqswsum (k, v) VALUES ('y', -10), ('y', 10), ('y', 0);

SELECT * FROM test_sw_cash_sum ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO cash_stream_cqswsum (k, v) VALUES ('x', '500.50');
INSERT INTO cash_stream_cqswsum (k, v) VALUES ('y', '0.01');

SELECT * FROM test_sw_cash_sum ORDER BY k;

DROP FOREIGN TABLE cash_stream_cqswsum CASCADE;

-------------------------------------------------------------------------------
-- Numeric sums
CREATE FOREIGN TABLE numeric_stream_cqswsum (k text, v numeric) SERVER pipelinedb;

CREATE VIEW test_sw_numeric_sum AS SELECT k::text, SUM(v::numeric) FROM numeric_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO numeric_stream_cqswsum (k, v) VALUES ('x', 10.0002), ('x', 0.0001), ('x', -0.10);
INSERT INTO numeric_stream_cqswsum (k, v) VALUES ('y', 1.004e5), ('y', 0.4), ('y', 0);

SELECT * FROM test_sw_numeric_sum ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO numeric_stream_cqswsum (k, v) VALUES ('x', '10000000000000000000000000000000000000000000000000000');
INSERT INTO numeric_stream_cqswsum (k, v) VALUES ('y', '-0000000000000000000000000000000000000.00000000000000000000000000000000001');

SELECT * FROM test_sw_numeric_sum ORDER BY k;

DROP FOREIGN TABLE numeric_stream_cqswsum CASCADE;

-------------------------------------------------------------------------------
-- Interval sum
CREATE FOREIGN TABLE interval_stream_cqswsum (k text, ts0 timestamp, ts1 timestamp) SERVER pipelinedb;

CREATE VIEW test_sw_interval_sum AS SELECT k::text, SUM(ts1::timestamp - ts0::timestamp) FROM interval_stream_cqswsum WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO interval_stream_cqswsum (k, ts0, ts1) VALUES ('x', '2014-01-01', '2014-01-02'), ('x', '2014-01-01', '2014-02-01');
INSERT INTO interval_stream_cqswsum (k, ts0, ts1) VALUES ('x', '2014-01-01', '2014-01-02'), ('x', '2014-01-01', '2014-02-01');

SELECT * FROM test_sw_interval_sum ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO interval_stream_cqswsum (k, ts0, ts1) VALUES ('x', '2014-12-31', '2015-01-01'), ('x', '2015-02-28', '2015-03-01');
INSERT INTO interval_stream_cqswsum (k, ts0, ts1) VALUES ('y', '2014-01-01', '2014-01-02'), ('z', '2014-01-01', '2014-02-01');

SELECT * FROM test_sw_interval_sum ORDER BY k;

DROP FOREIGN TABLE interval_stream_cqswsum CASCADE;
