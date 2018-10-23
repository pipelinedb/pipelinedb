CREATE FOREIGN TABLE sw_minmax_stream (
  key text, i8 int8, i4 int4, i2 int2, o oid, f8 float8, f4 float4,
  d date, t time, tz timetz, m money, ts timestamp, tstz timestamptz,
  ts0 timestamp, ts1 timestamp, txt text, n numeric, a int[]) SERVER pipelinedb;

CREATE VIEW test_sw_min_max AS SELECT
key::text,
min(i8::int8) AS i8min, max(i8) AS i8max,
min(i4::int4) AS i4min, max(i4) AS i4max,
min(i2::int2) AS i2min, max(i2) AS i2max,
min(o::oid) AS omin, max(o) AS omax,
min(f8::float8) AS f8min, max(f8) AS f8max,
min(f4::float4) AS f4min, max(f4) AS f4max,
min(d::date) AS dmin, max(d) AS dmax,
min(t::time) AS tmin, max(t) AS tmax,
min(tz::timetz) AS tzmin, max(tz) AS tzmax,
min(m::money) AS mmin, max(m) AS mmax,
min(ts::timestamp) AS tsmin, max(ts) AS tsmax,
min(tstz::timestamptz) AS tstzmin, max(tstz) AS tstzmax,
min(ts0::timestamp - ts1::timestamp) AS intervalmin, max(ts0::timestamp - ts1::timestamp) AS intervalmax,
min(txt::text) AS txtmin, max(txt) AS txtmax,
min(n::numeric) AS nmin, max(n) AS nmax,
min(a::int[]) AS amin, max(a) AS amax
FROM sw_minmax_stream WHERE arrival_timestamp > clock_timestamp() - interval '60 second' GROUP BY key;

INSERT INTO sw_minmax_stream
(key, i8, i4, i2, o, f8, f4, d, t, tz, m, ts, tstz, ts1, ts0, txt, n, a) VALUES
('x', 0, 1, 2, 443, 42.33, 80003.00001, '2014-12-31', '2014-02-28 00:00:00', '2014-02-28 00:00:00-08', 999023.39, '2014-01-01 00:00:00', '2014-01-01 00:00:01-08', '2014-01-01 00:00:00', '2014-01-02 00:00:00', 'first row', -0.00000000042, '{-1, 0, 1}');

INSERT INTO sw_minmax_stream
(key, i8, i4, i2, o, f8, f4, d, t, tz, m, ts, tstz, ts1, ts0, txt, n, a) VALUES
('x', 10000, -1, 2, 100000, 442.33, 1e-9, '2014-12-31', '2014-02-28 00:00:00', '2014-02-28 00:00:00-08', 0.389, '2014-01-01 00:00:00', '2014-01-01 00:00:01-08', '2014-01-01 00:00:00', '2014-01-02 00:00:00', 'second row', 0.00000000042, '{-1, 0, 2, 3}');

SELECT pg_sleep(1);

INSERT INTO sw_minmax_stream
(key, i8, i4, i2, o, f8, f4, d, t, tz, m, ts, tstz, ts1, ts0, txt, n, a) VALUES
('y', -1, 1, 2, 443, -0.00001, 1e12, '2010-12-31', '2014-02-28 00:00:00', '2014-02-28 00:00:00-08', -23.40, '2014-01-01 00:00:00', '2014-01-01 00:00:01-08', '2014-01-01 00:00:00', '2014-01-01 00:00:01', 'third row', -0.00000000041, '{-10}');

INSERT INTO sw_minmax_stream
(key, i8, i4, i2, o, f8, f4, d, t, tz, m, ts, tstz, ts1, ts0, txt, n, a) VALUES
('y', 0, 1, 2, 443, 442.33, 80003.00001, '2014-12-31', '2014-02-28 00:00:00', '2014-02-28 00:00:00-08', 999023.399, '2014-01-01 00:00:00', '2014-01-01 00:00:01-08', '2014-01-01 00:00:00', '2015-01-02 00:00:00', 'fourth row', 1.00000000042, '{-1, 0, 1}');

-- It's hard to read all of the columns of this thing at once, so SELECT a few subsets
SELECT key, i8min, i8max, i4min, i4max, i2min, i2max FROM test_sw_min_max ORDER BY key;
SELECT key, omin, omax, f8min, f8max, f4min, f4max FROM test_sw_min_max ORDER BY key;
SELECT key, dmin, dmax, tmin, tmax, tzmin, tzmax, mmin FROM test_sw_min_max ORDER BY key;
SELECT key, mmax, tsmin, tsmax, tstzmin, tstzmax, intervalmin FROM test_sw_min_max ORDER BY key;
SELECT key, intervalmax, txtmin, txtmax, nmin, nmax, amin, amax FROM test_sw_min_max ORDER BY key;

DROP FOREIGN TABLE sw_minmax_stream CASCADE;
