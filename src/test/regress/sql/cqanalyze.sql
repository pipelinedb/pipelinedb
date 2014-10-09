-- Sanity checks
CREATE CONTINUOUS VIEW cqanalyze0 AS SELECT substring(url::text, 1, 2) FROM stream;
CREATE CONTINUOUS VIEW cqanalyze1 AS SELECT CASE WHEN x::text = '1' THEN 1 END FROM stream;
CREATE CONTINUOUS VIEW cqanalyze2 AS SELECT a::integer FROM stream GROUP BY a;
CREATE CONTINUOUS VIEW cqanalyze3 AS SELECT a::integer FROM stream WHERE a > 10 GROUP BY a;
CREATE CONTINUOUS VIEW cqanalyze4 AS SELECT a::integer, b::integer FROM stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;
CREATE CONTINUOUS VIEW cqanalyze5 AS SELECT substring(url::text, 1, 2) AS g, COUNT(*) FROM stream GROUP BY g;
CREATE CONTINUOUS VIEW cqanalyze6 AS SELECT s.id::integer FROM stream s WHERE s.id < 10 AND s.value::integer > 10;

-- Verify that we can infer types for columns appearing outside of the target list
CREATE CONTINUOUS VIEW cqanalyze7 AS SELECT id::integer FROM stream ORDER BY f::integer DESC;
CREATE CONTINUOUS VIEW cqanalyze8 AS SELECT id::integer FROM stream GROUP BY id, url::text;
CREATE CONTINUOUS VIEW cqanalyze9 AS SELECT a::integer, b::integer FROM stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;

-- Windows
CREATE CONTINUOUS VIEW cqanalyze10 AS SELECT ts::timestamp, SUM(val::numeric) OVER (ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW cqanalyze11 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW cqanalyze12 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW cqanalyze13 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;

-- Multiple streams
CREATE CONTINUOUS VIEW cqanalyze14 AS SELECT s0.a::integer, s1.b::integer FROM s0, s1;
CREATE CONTINUOUS VIEW cqanalyze15 AS SELECT s0.a::integer, s1.b::integer, s2.c::text FROM s0, s1, s2;

-- Stream-table JOINs
CREATE TABLE t0 (id INTEGER);
CREATE CONTINUOUS VIEW cqanalyze16 AS SELECT s0.id::integer AS s0_id, t0.id AS t0_id FROM s0 JOIN t0 ON s0.id = t0.id;
CREATE CONTINUOUS VIEW cqanalyze17 AS SELECT s.x::integer, t0.id FROM stream s JOIN t0 ON s.x = t0.id;

-- Stream-stream JOINs
CREATE CONTINUOUS VIEW cqanalyze18 AS SELECT s0.id::integer as id0, s1.id::integer as id1 FROM s0 JOIN s1 ON s0.id = s1.id;
CREATE CONTINUOUS VIEW cqanalyze19 AS SELECT s0.id::integer AS id0, s1.id::integer AS id1 FROM stream s0 JOIN another_stream s1 ON s0.id = s1.id WHERE s0.id > 10 ORDER BY s1.id DESC;

-- Stream-table-stream JOINs
CREATE table sts (id INTEGER);
CREATE CONTINUOUS VIEW cqanalyze20 AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM s0 JOIN sts ON s0.id = sts.id JOIN s1 ON sts.id = s1.x;
CREATE CONTINUOUS VIEW cqanalyze21 AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM stream s0 JOIN sts ON s0.id = sts.id JOIN s1 ON sts.id = s1.id::integer WHERE sts.id > 42;
CREATE CONTINUOUS VIEW cqanalyze22 AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM stream s0 INNER JOIN sts ON s0.id = sts.id RIGHT OUTER JOIN s1 ON sts.id = s1.id::integer WHERE sts.id > 42;

-- Now let's verify our error handling and messages
-- Stream column doesn't have a type
CREATE CONTINUOUS VIEW cqanalyze23 AS SELECT col FROM stream;

-- Column not qualified with a stream
CREATE CONTINUOUS VIEW cqanalyze24 AS SELECT id::integer FROM s0, s1;

-- Column has multiple types
CREATE CONTINUOUS VIEW cqanalyze25 AS SELECT id::integer AS id0, id::text AS id1 FROM stream;

-- Another untyped column with an aliased stream
CREATE CONTINUOUS VIEW cqanalyze26 AS SELECT s.id FROM stream s WHERE s.id < 10;

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
FROM stream WHERE aa > '2014-01-01 00:00:00' AND n @> '{"key": "value"}'::jsonb AND r > 42.3::money;

-- Verify that type cast for arrival_timestamp is optional
CREATE CONTINUOUS VIEW cqanalyze28 AS SELECT arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW cqanalyze29 AS SELECT key::text, arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW cqanalyze30 AS SELECT key::text FROM test_stream WHERE arrival_timestamp < clock_timestamp();
CREATE CONTINUOUS VIEW cqanalyze31 AS SELECT key::text, arrival_timestamp::timestamptz FROM test_stream;
CREATE CONTINUOUS VIEW cqanalyze32 AS SELECT key::text, arrival_timestamp FROM test_stream WHERE arrival_timestamp::timestamptz < clock_timestamp();

-- Verify that we can't do wildcard selections from streams
CREATE CONTINUOUS VIEW cqanalyze33 AS SELECT * from stream;
CREATE CONTINUOUS VIEW cqanalyze34 AS SELECT * from stream, t0;
CREATE CONTINUOUS VIEW cqanalyze35 AS SELECT t0.* from stream, t0;
CREATE CONTINUOUS VIEW cqanalyze36 AS SELECT stream.* from stream, t0;

-- Disallow sorting streams
CREATE CONTINUOUS VIEW cqanalyze37 AS SELECT key::text from stream ORDER BY key;
CREATE CONTINUOUS VIEW cqanalyze38 AS SELECT key::text from stream ORDER BY arrival_time;

-- Regression tests
CREATE CONTINUOUS VIEW cqanalyze39 AS SELECT s0.ts::timestamp AS a, s0.id::timestamp, s1.id2::timestamp FROM stream s0, stream2 s1 WHERE s0.arrival_timestamp > (clock_timestamp() - interval '1 hour') AND s1.ts::timestamp > clock_timestamp() AND s1.id2 < clock_timestamp();
