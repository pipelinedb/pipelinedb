-------------------------------------------------------------------------------
-- Integer averages
CREATE FOREIGN TABLE int_swavg_stream (k text, v int8) SERVER pipelinedb;

CREATE VIEW test_int8_avg_sw AS SELECT k::text, AVG(v::int8) FROM int_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_int4_avg_sw AS SELECT k::text, AVG(v::int4) FROM int_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_int2_avg_sw AS SELECT k::text, AVG(v::int2) FROM int_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO int_swavg_stream (k, v) VALUES ('x', 1), ('x', 1), ('x', 1);
INSERT INTO int_swavg_stream (k, v) VALUES ('y', -10000), ('y', 10000), ('y', 0);

SELECT * FROM test_int8_avg_sw ORDER BY k;
SELECT * FROM test_int4_avg_sw ORDER BY k;
SELECT * FROM test_int2_avg_sw ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO int_swavg_stream (k, v) VALUES ('x', 2), ('x', 2), ('x', 2);
INSERT INTO int_swavg_stream (k, v) VALUES ('y', 1), ('y', 10000), ('y', 2000);

SELECT * FROM test_int8_avg_sw ORDER BY k;
SELECT * FROM test_int4_avg_sw ORDER BY k;
SELECT * FROM test_int2_avg_sw ORDER BY k;

DROP FOREIGN TABLE int_swavg_stream CASCADE;

-------------------------------------------------------------------------------
-- Float averages
CREATE FOREIGN TABLE float_swavg_stream (k text, v float8) SERVER pipelinedb;

CREATE VIEW test_float8_avg_sw AS SELECT k::text, AVG(v::float8) FROM float_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;
CREATE VIEW test_float4_avg_sw AS SELECT k::text, AVG(v::float4) FROM float_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO float_swavg_stream (k, v) VALUES ('x', 1e6), ('x', -1e6), ('x', 10.0000001);
INSERT INTO float_swavg_stream (k, v) VALUES ('y', 0.0001), ('y', 0.00002), ('y', 155321233.1);

SELECT k, round(avg::numeric, 5) FROM test_float8_avg_sw ORDER BY k;
SELECT k, round(avg::numeric, 5) FROM test_float4_avg_sw ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO float_swavg_stream (k, v) VALUES ('x', -1e6), ('x', 1e6), ('x', -10.0000001);
INSERT INTO float_swavg_stream (k, v) VALUES ('z', 42.42);

SELECT k, round(avg::numeric, 5) FROM test_float8_avg_sw ORDER BY k;
SELECT k, round(avg::numeric, 5) FROM test_float4_avg_sw ORDER BY k;

DROP FOREIGN TABLE float_swavg_stream CASCADE;

-------------------------------------------------------------------------------
-- Numeric averages
CREATE FOREIGN TABLE numeric_swavg_stream (k text, v numeric) SERVER pipelinedb;

CREATE VIEW test_numeric_avg_sw AS SELECT k::text, AVG(v::numeric) FROM numeric_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO numeric_swavg_stream (k, v) VALUES ('x', 10000000000000000.233), ('x', -1.000000000333), ('x', 0.00000000001);
INSERT INTO numeric_swavg_stream (k, v) VALUES ('y', 0.1001), ('y', 0.99999999), ('y', -999999999999999999.999999999999);

SELECT * FROM test_numeric_avg_sw ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO numeric_swavg_stream (k, v) VALUES ('x', 1), ('y', 2), ('z', 42.42);

SELECT * FROM test_numeric_avg_sw ORDER BY k;

DROP FOREIGN TABLE numeric_swavg_stream CASCADE;

-------------------------------------------------------------------------------
-- Interval averages
CREATE FOREIGN TABLE interval_swavg_stream (k text, ts0 timestamp, ts1 timestamp) SERVER pipelinedb;

CREATE VIEW test_interval_avg_sw AS SELECT k::text, AVG(date_trunc('day', ts1::timestamp) - date_trunc('day', ts0::timestamp)) FROM interval_swavg_stream WHERE arrival_timestamp > clock_timestamp() - interval '5 hour' GROUP BY k;

INSERT INTO interval_swavg_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 23:00:00');
INSERT INTO interval_swavg_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 01:00:00');
INSERT INTO interval_swavg_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 11:00:00');

-- We truncated down to day, so the hours shouldn't have affected the averages
SELECT * FROM test_interval_avg_sw ORDER BY k;

SELECT pg_sleep(1);

INSERT INTO interval_swavg_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-04 04:00:00');
INSERT INTO interval_swavg_stream (k, ts0, ts1) VALUES ('y', '2014-01-01 23:00:00', '2014-01-02 00:00:00');

SELECT * FROM test_interval_avg_sw ORDER BY k;

DROP FOREIGN TABLE interval_swavg_stream CASCADE;

CREATE FOREIGN TABLE heart_rates_stream (patient_id int, value float, description text) SERVER pipelinedb;

CREATE VIEW avg_heart_rates AS
SELECT patient_id::integer, avg(value::float) FROM heart_rates_stream
WHERE description::text = 'HR' AND value > 85
AND (arrival_timestamp > clock_timestamp() - interval '5 minutes') GROUP BY patient_id;

INSERT INTO heart_rates_stream (patient_id, value, description) VALUES (0, 86, 'HR');
INSERT INTO heart_rates_stream (patient_id, value, description) VALUES (0, 87, 'HR');
INSERT INTO heart_rates_stream (patient_id, value, description) VALUES (0, 85, 'HR');
INSERT INTO heart_rates_stream (patient_id, value, description) VALUES (0, 86, 'HZ');

SELECT * FROM avg_heart_rates ORDER BY patient_id;

CREATE TABLE sw_t AS SELECT 0 AS x;

SELECT * FROM avg_heart_rates cv JOIN sw_t ON cv.patient_id = sw_t.x;

DROP FOREIGN TABLE heart_rates_stream CASCADE;

