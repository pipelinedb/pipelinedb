-- Sanity checks
CREATE CONTINUOUS VIEW cqanalyze0 AS SELECT substring(url::text, 1, 2) FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqanalyze1 AS SELECT CASE WHEN x::text = '1' THEN 1 END FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqanalyze2 AS SELECT a::integer FROM analyze_cont_stream GROUP BY a;
CREATE CONTINUOUS VIEW cqanalyze3 AS SELECT a::integer FROM analyze_cont_stream WHERE a > 10 GROUP BY a;
CREATE CONTINUOUS VIEW cqanalyze4 AS SELECT a::integer, b::integer FROM analyze_cont_stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;
CREATE CONTINUOUS VIEW cqanalyze5 AS SELECT substring(url::text, 1, 2) AS g, COUNT(*) FROM analyze_cont_stream GROUP BY g;
CREATE CONTINUOUS VIEW cqanalyze6 AS SELECT s.id::integer FROM analyze_cont_stream s WHERE s.id < 10 AND s.value::integer > 10;

-- Verify that we can infer types for columns appearing outside of the target list
CREATE CONTINUOUS VIEW cqanalyze7 AS SELECT id::integer FROM analyze_cont_stream ORDER BY f::integer DESC;
CREATE CONTINUOUS VIEW cqanalyze8 AS SELECT id::integer FROM analyze_cont_stream GROUP BY id, url::text;
CREATE CONTINUOUS VIEW cqanalyze9 AS SELECT a::integer, b::integer FROM analyze_cont_stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;

-- Windows
CREATE CONTINUOUS VIEW cqanalyze10 AS SELECT ts::timestamp, SUM(val::numeric) OVER (ORDER BY ts) FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqanalyze11 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqanalyze12 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY (ts + INTERVAL '1 day')) FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqanalyze13 AS SELECT ts::text, AVG(val::numeric) OVER (ORDER BY ts) FROM analyze_cont_stream;

-- Stream-table JOINs
CREATE TABLE t0 (id INTEGER);
CREATE CONTINUOUS VIEW cqanalyze16 AS SELECT s0.id::integer AS s0_id, t0.id AS t0_id FROM s0 JOIN t0 ON s0.id = t0.id;
CREATE CONTINUOUS VIEW cqanalyze17 AS SELECT s.x::integer, t0.id FROM analyze_cont_stream2 s JOIN t0 ON s.x = t0.id;

-- Now let's verify our error handling and messages
-- Stream column doesn't have a type
CREATE CONTINUOUS VIEW cqanalyze23 AS SELECT col FROM analyze_cont_stream;

-- Column not qualified with a stream
CREATE CONTINUOUS VIEW cqanalyze24 AS SELECT id::integer FROM t0, s1;

-- Column has multiple types
CREATE CONTINUOUS VIEW cqanalyze25 AS SELECT id::integer AS id0, id::text AS id1 FROM analyze_cont_stream;

-- Another untyped column with an aliased stream
CREATE CONTINUOUS VIEW cqanalyze26 AS SELECT s.id FROM analyze_cont_stream s WHERE s.id < 10;

-- Verify that NOTICEs are properly shown when joining on unindexed columns
CREATE TABLE tnotice (x integer, y integer);
CREATE CONTINUOUS VIEW cvnotice0 AS SELECT stream.x::integer FROM analyze_cont_stream2 JOIN tnotice ON stream.x = tnotice.x;

-- tnotice.x NOTICE should only be shown once
CREATE CONTINUOUS VIEW cvnotice1 AS SELECT stream.x::integer FROM analyze_cont_stream2 JOIN tnotice ON stream.x = tnotice.x AND stream.x = tnotice.x;
CREATE CONTINUOUS VIEW cvnotice2 AS SELECT stream.x::integer FROM analyze_cont_stream2, tnotice WHERE tnotice.x = stream.x;

CREATE INDEX tnotice_idx ON tnotice(x);

-- No NOTICE should be given now that an index exists
CREATE CONTINUOUS VIEW cvnotice3 AS SELECT stream.x::integer FROM analyze_cont_stream, tnotice WHERE tnotice.x = stream.x;

DROP TABLE tnotice;

-- Verify all relevant types are recognized
CREATE CONTINUOUS VIEW cqanalyze27 AS SELECT
a::bigint,
b::bit[2],
c::varbit(5),
d::boolean,
c0::box,
d0::bytea,
e::char(42),
f::varchar(32),
g::cidr,
h::circle,
i::date,
j::float8,
k::inet,
l::integer,
m::json,
n::jsonb,
o::line,
p::lseg,
q::macaddr,
r::money,
s::numeric(1, 1),
t::path,
u::point,
v::polygon,
w::real,
x::smallint,
y::text,
z::time,
aa::timetz,
bb::timestamp,
cc::timestamptz,
dd::tsquery,
ee::tsvector,
ff::uuid,
gg::xml
FROM analyze_cont_stream2 WHERE aa > '2014-01-01 00:00:00' AND n @> '{"key": "value"}'::jsonb AND r > 42.3::money;

-- Verify that type cast for arrival_timestamp is optional
CREATE CONTINUOUS VIEW cqanalyze28 AS SELECT arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW cqanalyze29 AS SELECT key::text, arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW cqanalyze30 AS SELECT key::text FROM test_stream WHERE arrival_timestamp < TIMESTAMP '2004-10-19 10:23:54+02';
CREATE CONTINUOUS VIEW cqanalyze31 AS SELECT key::text, arrival_timestamp::timestamptz FROM test_stream;

-- Verify that we can't do wildcard selections in continuous queries
CREATE CONTINUOUS VIEW cqanalyze32 AS SELECT * from stream;
CREATE CONTINUOUS VIEW cqanalyze33 AS SELECT * from stream, t0;
CREATE CONTINUOUS VIEW cqanalyze34 AS SELECT t0.* from stream, t0;
CREATE CONTINUOUS VIEW cqanalyze35 AS SELECT stream.* from stream, t0;

-- Disallow sorting streams
CREATE CONTINUOUS VIEW cqanalyze36 AS SELECT key::text from stream ORDER BY key;
CREATE CONTINUOUS VIEW cqanalyze37 AS SELECT key::text from stream ORDER BY arrival_time;

-- Sliding window queries
CREATE CONTINUOUS VIEW cqanalyze38 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp < clock_timestamp() - interval '1 hour';
CREATE CONTINUOUS VIEW cqanalyze39 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp < clock_timestamp() - interval '1 hour' AND key::text='pipelinedb';
CREATE CONTINUOUS VIEW cqanalyze40 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE NOT arrival_timestamp < clock_timestamp() - interval '1 hour';
CREATE CONTINUOUS VIEW cqanalyze41 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp < clock_timestamp() - interval '1 hour' OR key::text='pipelinedb';
CREATE CONTINUOUS VIEW cqanalyze42 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp < clock_timestamp() - interval '1 hour' AND arrival_timestamp > clock_timestamp() - interval '5 hour';

-- Hypothetical-set aggregates
CREATE CONTINUOUS VIEW cqanalyze45 AS SELECT g::integer, percent_rank(1 + 3, 2, substring('xxx', 1, 2)) WITHIN GROUP (ORDER BY x::integer, y::integer, z::text) + rank(4, 5, 'x') WITHIN GROUP (ORDER BY x, y, substring(z, 1, 2))  FROM analyze_cont_stream3 GROUP BY g;
CREATE CONTINUOUS VIEW cqanalyze46 AS SELECT rank(0, 1) WITHIN GROUP (ORDER BY x::integer, y::integer) + rank(0) WITHIN GROUP (ORDER BY x) FROM analyze_cont_stream3;

-- Number of arguments to HS function is inconsistent with the number of GROUP columns
CREATE CONTINUOUS VIEW error_not_created AS SELECT percent_rank(1) WITHIN GROUP (ORDER BY x::integer, y::integer, z::integer) FROM analyze_cont_stream;

-- Types of arguments to HS function are inconsistent with GROUP column types
CREATE CONTINUOUS VIEW error_not_created AS SELECT g::integer, dense_rank(2, 3, 4) WITHIN GROUP (ORDER BY x::integer, y::integer, z::text) FROM analyze_cont_stream GROUP BY g;

CREATE CONTINUOUS VIEW cqanalyze47 AS SELECT g::integer, rank(2, 3, 4) WITHIN GROUP (ORDER BY x::integer, y::integer, z::integer), sum(x + y + z) FROM analyze_cont_stream4 GROUP BY g;

-- Sliding windows
CREATE CONTINUOUS VIEW cqanalyze48 AS SELECT cume_dist(2) WITHIN GROUP (ORDER BY x::integer DESC) FROM analyze_cont_stream3 WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');
CREATE CONTINUOUS VIEW cqanalyze49 AS SELECT percent_rank(2) WITHIN GROUP (ORDER BY x::integer DESC), rank(2) WITHIN GROUP (ORDER BY x) FROM analyze_cont_stream3 WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');

-- Verify that we get an error if we try to create a CV that only selects from tables
CREATE TABLE cqanalyze_table (id integer);
CREATE CONTINUOUS VIEW error_not_created AS SELECT cqanalyze_table.id::integer FROM cqanalyze_table;
DROP TABLE cqanalyze_table;

-- Verify that for stream-table joins, the correct error message is generated when the table is missing
CREATE CONTINUOUS VIEW  error_not_created AS SELECT s.id::integer, t.tid FROM analyze_cont_stream s JOIN not_a_table t ON s.id = t.tid;
CREATE CONTINUOUS VIEW  error_not_created AS SELECT s.id::integer, tid FROM analyze_cont_stream s JOIN not_a_table ON s.id = tid;

-- Ordered-set aggregates
CREATE CONTINUOUS VIEW cqanalyze50 AS SELECT g::integer, percentile_cont(ARRAY[0.2, 0.8]) WITHIN GROUP (ORDER BY x::float), percentile_cont(0.9) WITHIN GROUP (ORDER BY y::integer) + percentile_cont(0.1) WITHIN GROUP (ORDER BY z::numeric) AS col FROM analyze_cont_stream5 GROUP BY g;
CREATE CONTINUOUS VIEW cqanalyze51 AS SELECT g::integer, percentile_cont(0.1) WITHIN GROUP (ORDER BY x::float + y::integer) FROM analyze_cont_stream3 GROUP BY g;

-- Can only sort on a numeric expression
CREATE CONTINUOUS VIEW error_not_created AS SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY x::text) FROM analyze_cont_stream;

-- Sliding windows
CREATE CONTINUOUS VIEW cqanalyze52 AS SELECT g::integer, percentile_cont(ARRAY[0.2, 0.8]) WITHIN GROUP (ORDER BY x::float), percentile_cont(0.9) WITHIN GROUP (ORDER BY y::integer) + percentile_cont(0.1) WITHIN GROUP (ORDER BY z::numeric) AS col FROM analyze_cont_stream6 WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes') GROUP BY g;
CREATE CONTINUOUS VIEW cqanalyze53 AS SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY x::float + y::integer) FROM analyze_cont_stream3 WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');

CREATE CONTINUOUS VIEW over_1m AS
SELECT date_trunc('minute', t::timestamptz) AS minute, avg(queue_length::float) AS load_avg
FROM sysstat
WHERE arrival_timestamp > clock_timestamp() - interval '1 hour'
GROUP BY minute;

CREATE OR REPLACE VIEW over_5m AS
SELECT first_value(minute) OVER w AS minute, combine(load_avg) OVER w AS load_avg
FROM over_1m
WINDOW w AS (ORDER BY minute DESC ROWS 4 PRECEDING);

\d+ over_5m;

CREATE CONTINUOUS VIEW funcs AS SELECT floor(x::float8), date_trunc('day', arrival_timestamp), COUNT(*) FROM stream GROUP BY floor, date_trunc;

DROP CONTINUOUS VIEW cqanalyze0;
DROP CONTINUOUS VIEW cqanalyze1;
DROP CONTINUOUS VIEW cqanalyze2;
DROP CONTINUOUS VIEW cqanalyze3;
DROP CONTINUOUS VIEW cqanalyze4;
DROP CONTINUOUS VIEW cqanalyze5;
DROP CONTINUOUS VIEW cqanalyze6;
DROP CONTINUOUS VIEW cqanalyze7;
DROP CONTINUOUS VIEW cqanalyze8;
DROP CONTINUOUS VIEW cqanalyze9;
DROP CONTINUOUS VIEW cqanalyze10;
DROP CONTINUOUS VIEW cqanalyze11;
DROP CONTINUOUS VIEW cqanalyze12;
DROP CONTINUOUS VIEW cqanalyze13;
DROP CONTINUOUS VIEW cqanalyze16;
DROP CONTINUOUS VIEW cqanalyze17;
DROP CONTINUOUS VIEW cqanalyze23;
DROP CONTINUOUS VIEW cqanalyze24;
DROP CONTINUOUS VIEW cqanalyze25;
DROP CONTINUOUS VIEW cqanalyze26;
DROP CONTINUOUS VIEW cqanalyze27;
DROP CONTINUOUS VIEW cqanalyze28;
DROP CONTINUOUS VIEW cqanalyze29;
DROP CONTINUOUS VIEW cqanalyze30;
DROP CONTINUOUS VIEW cqanalyze31;
DROP CONTINUOUS VIEW cqanalyze32;
DROP CONTINUOUS VIEW cqanalyze33;
DROP CONTINUOUS VIEW cqanalyze34;
DROP CONTINUOUS VIEW cqanalyze35;
DROP CONTINUOUS VIEW cqanalyze36;
DROP CONTINUOUS VIEW cqanalyze37;
DROP CONTINUOUS VIEW cqanalyze38;
DROP CONTINUOUS VIEW cqanalyze39;
DROP CONTINUOUS VIEW cqanalyze40;
DROP CONTINUOUS VIEW cqanalyze41;
DROP CONTINUOUS VIEW cqanalyze42;
DROP CONTINUOUS VIEW cqanalyze45;
DROP CONTINUOUS VIEW cqanalyze46;
DROP CONTINUOUS VIEW cqanalyze47;
DROP CONTINUOUS VIEW cqanalyze48;
DROP CONTINUOUS VIEW cqanalyze49;
DROP CONTINUOUS VIEW cqanalyze50;
DROP CONTINUOUS VIEW cqanalyze51;
DROP CONTINUOUS VIEW cqanalyze52;
DROP CONTINUOUS VIEW cqanalyze53;
DROP CONTINUOUS VIEW cvnotice0;
DROP CONTINUOUS VIEW cvnotice1;
DROP CONTINUOUS VIEW cvnotice2;
DROP CONTINUOUS VIEW cvnotice3;
DROP CONTINUOUS VIEW funcs;
DROP CONTINUOUS VIEW over_1m CASCADE;
DROP TABLE t0;

-- Regression
CREATE CONTINUOUS VIEW cqregress1 AS SELECT id::integer + avg(id) FROM analyze_cont_stream GROUP BY id;
CREATE CONTINUOUS VIEW cqregress2 AS SELECT date_trunc('hour', ts) AS ts FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqregress3 AS SELECT stream.sid::integer FROM analyze_cont_stream;
CREATE CONTINUOUS VIEW cqregress4 AS SELECT x::int FROM cqregress4;

DROP CONTINUOUS VIEW cqregress1;
DROP CONTINUOUS VIEW cqregress2;
DROP CONTINUOUS VIEW cqregress3;
DROP CONTINUOUS VIEW cqregress4;
