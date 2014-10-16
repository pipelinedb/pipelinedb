SET debug_sync_stream_insert = 'on';

-------------------------------------------------------------------------------
-- Integer sums
CREATE CONTINUOUS VIEW test_int8 AS SELECT k::text, SUM(v::int8) FROM int_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_int4 AS SELECT k::text, SUM(v::int4) FROM int_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_int2 AS SELECT k::text, SUM(v::int2) FROM int_stream GROUP BY k;

ACTIVATE test_int8, test_int4, test_int2;

INSERT INTO int_stream (k, v) VALUES ('x', 10), ('x', 10), ('x', 10);
INSERT INTO int_stream (k, v) VALUES ('y', 10), ('y', 10), ('y', 10);

DEACTIVATE;

SELECT * FROM test_int8;
SELECT * FROM test_int4;
SELECT * FROM test_int2;

ACTIVATE test_int8, test_int4, test_int2;

INSERT INTO int_stream (k, v) VALUES ('x', 1), ('x', 2), ('x', 3);
INSERT INTO int_stream (k, v) VALUES ('y', 10), ('y', 10), ('z', 10);

DEACTIVATE;

SELECT * FROM test_int8;
SELECT * FROM test_int4;
SELECT * FROM test_int2;

-------------------------------------------------------------------------------
-- Float sums
CREATE CONTINUOUS VIEW test_float8 AS SELECT k::text, SUM(v::float8) FROM float_stream GROUP BY k;
CREATE CONTINUOUS VIEW test_float4 AS SELECT k::text, SUM(v::float4) FROM float_stream GROUP BY k;

ACTIVATE test_float8, test_float4;

INSERT INTO float_stream (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 0.000001);
INSERT INTO float_stream (k, v) VALUES ('y', -10.3), ('y', 1.2e6), ('y', '100.4');

DEACTIVATE;

SELECT * FROM test_float8_pdb;
SELECT * FROM test_float4_pdb;

ACTIVATE test_float8, test_float4;

INSERT INTO float_stream (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 1.2e-4);
INSERT INTO float_stream (k, v) VALUES ('y', -10.3), ('y', 1.2e6), ('y', '100.4');

DEACTIVATE;

SELECT * FROM test_float8;
SELECT * FROM test_float4;

-------------------------------------------------------------------------------
-- Cash sums
CREATE CONTINUOUS VIEW test_cash AS SELECT k::text, SUM(v::money) FROM cash_stream GROUP BY k;

ACTIVATE test_cash;

INSERT INTO cash_stream (k, v) VALUES ('x', 10.2), ('x', 1.2), ('x', 0.10);
INSERT INTO cash_stream (k, v) VALUES ('y', -10), ('y', 10), ('y', 0);

DEACTIVATE;

SELECT * FROM test_cash;

ACTIVATE test_cash;

INSERT INTO cash_stream (k, v) VALUES ('x', '500.50');
INSERT INTO cash_stream (k, v) VALUES ('y', '0.01');

DEACTIVATE;

SELECT * FROM test_cash;

-------------------------------------------------------------------------------
-- Numeric sums
CREATE CONTINUOUS VIEW test_numeric AS SELECT k::text, SUM(v::numeric) FROM numeric_stream GROUP BY k;

ACTIVATE test_numeric;

INSERT INTO numeric_stream (k, v) VALUES ('x', 10.0002), ('x', 0.0001), ('x', -0.10);
INSERT INTO numeric_stream (k, v) VALUES ('y', 1.004e5), ('y', 0.4), ('y', 0);

DEACTIVATE;

SELECT * FROM test_numeric;

ACTIVATE test_numeric;

INSERT INTO numeric_stream (k, v) VALUES ('x', '10000000000000000000000000000000000000000000000000000');
INSERT INTO numeric_stream (k, v) VALUES ('y', '-0000000000000000000000000000000000000.00000000000000000000000000000000001');

DEACTIVATE;

SELECT * FROM test_numeric;

-------------------------------------------------------------------------------
-- Interval sum
CREATE CONTINUOUS VIEW test_interval AS SELECT k::text, SUM(ts1::timestamp - ts0::timestamp) FROM interval_stream GROUP BY k;

ACTIVATE test_interval;

INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01', '2014-01-02'), ('x', '2014-01-01', '2014-02-01');
INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-01-01', '2014-01-02'), ('x', '2014-01-01', '2014-02-01');

DEACTIVATE;

SELECT * FROM test_interval;

ACTIVATE test_interval;

INSERT INTO interval_stream (k, ts0, ts1) VALUES ('x', '2014-12-31', '2015-01-01'), ('x', '2015-02-28', '2015-03-01');
INSERT INTO interval_stream (k, ts0, ts1) VALUES ('y', '2014-01-01', '2014-01-02'), ('z', '2014-01-01', '2014-02-01');

DEACTIVATE;

SELECT * FROM test_interval;

