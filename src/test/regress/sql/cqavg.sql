SET debug_sync_stream_insert = 'on';

-------------------------------------------------------------------------------
-- Integer averages
CREATE CONTINUOUS VIEW test_int8 AS SELECT k::text, AVG(v::int8) FROM int_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_int4 AS SELECT k::text, AVG(v::int4) FROM int_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_int2 AS SELECT k::text, AVG(v::int2) FROM int_stream GROUP BY k;

ACTIVATE test_int8, test_int4, test_int2;

INSERT INTO int_stream (k, v) VALUES ('x', 1), ('x', 1), ('x', 1);
INSERT INTO int_stream (k, v) VALUES ('y', -10000), ('y', 10000), ('y', 0);

DEACTIVATE;

SELECT * FROM test_int8_pdb;
SELECT * FROM test_int4_pdb;
SELECT * FROM test_int2_pdb;

ACTIVATE test_int8, test_int4, test_int2;

INSERT INTO int_stream (k, v) VALUES ('x', 2), ('x', 2), ('x', 2);
INSERT INTO int_stream (k, v) VALUES ('y', 1), ('y', 10000), ('y', 2000);

DEACTIVATE;

SELECT * FROM test_int8;
SELECT * FROM test_int4;
SELECT * FROM test_int2;

-------------------------------------------------------------------------------
-- Float averages
CREATE CONTINUOUS VIEW test_float8 AS SELECT k::text, AVG(v::float8) FROM float_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_float4 AS SELECT k::text, AVG(v::float4) FROM float_stream GROUP BY k;

ACTIVATE test_float8, test_float4;

INSERT INTO float_stream (k, v) VALUES ('x', 1e6), ('x', -1e6), ('x', 10.0000001);
INSERT INTO float_stream (k, v) VALUES ('y', 0.0001), ('y', 0.00002), ('y', 155321233.1);

DEACTIVATE;

SELECT * FROM test_float8_pdb;
SELECT * FROM test_float4_pdb;

ACTIVATE test_float8, test_float4;

INSERT INTO float_stream (k, v) VALUES ('x', -1e6), ('x', 1e6), ('x', -10.0000001);
INSERT INTO float_stream (k, v) VALUES ('z', 42.42);

DEACTIVATE;

SELECT * FROM test_float8;
SELECT * FROM test_float4;

-------------------------------------------------------------------------------
-- Numeric averages
CREATE CONTINUOUS VIEW test_numeric AS SELECT k::text, AVG(v::numeric) FROM numeric_stream GROUP BY k;

ACTIVATE test_numeric;

INSERT INTO numeric_stream (k, v) VALUES ('x', 10000000000000000.233), ('x', -1.000000000333), ('x', 0.00000000001);
INSERT INTO numeric_stream (k, v) VALUES ('y', 0.1001), ('y', 0.99999999), ('y', -999999999999999999.999999999999);

DEACTIVATE;

SELECT * FROM test_numeric_pdb;

ACTIVATE test_numeric;

INSERT INTO numeric_stream (k, v) VALUES ('x', 1), ('y', 2), ('z', 42.42);

DEACTIVATE;

SELECT * FROM test_numeric;

-------------------------------------------------------------------------------
-- Interval averages
CREATE CONTINUOUS VIEW test_interval AS SELECT k::text, AVG(date_trunc('day', ts1::timestamp) - date_trunc('day', ts0::timestamp)) FROM interval_stream GROUP BY k;

--use functions for dates so we can verify nested works
ACTIVATE test_interval;

INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 23:00:00');
INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 01:00:00');
INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-02 11:00:00');

DEACTIVATE;

-- We truncated down to day, so the hours shouldn't have affected the averages
SELECT * FROM test_interval_pdb;

ACTIVATE test_interval;

INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01 00:00:00', '2014-01-04 04:00:00');
INSERT INTO interval_stream (k, ts0, ts1) VALUES ('y', '2014-01-01 23:00:00', '2014-01-02 00:00:00');

DEACTIVATE;

SELECT * FROM test_interval;


