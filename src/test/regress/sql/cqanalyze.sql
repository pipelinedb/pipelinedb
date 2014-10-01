-- Sanity checks
CREATE CONTINUOUS VIEW v0 AS SELECT substring(url::text, 1, 2) FROM stream;
CREATE CONTINUOUS VIEW v1 AS SELECT CASE WHEN x::text = '1' THEN 1 END FROM stream;
CREATE CONTINUOUS VIEW v2 AS SELECT a::integer FROM stream GROUP BY a;
CREATE CONTINUOUS VIEW v3 AS SELECT a::integer FROM stream WHERE a > 10 GROUP BY a;
CREATE CONTINUOUS VIEW v4 AS SELECT a::integer, b::integer FROM stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;
CREATE CONTINUOUS VIEW v5 AS SELECT substring(url::text, 1, 2) AS g, COUNT(*) FROM stream GROUP BY g;

-- Verify that we can infer types for columns appearing outside of the target list
CREATE CONTINUOUS VIEW v6 AS SELECT id::integer FROM stream ORDER BY f::integer DESC;
CREATE CONTINUOUS VIEW v7 AS SELECT id::integer FROM stream GROUP BY id, url::text;
CREATE CONTINUOUS VIEW v8 AS SELECT a::integer, b::integer FROM stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c::integer > 2;

-- Windows
CREATE CONTINUOUS VIEW v9 AS SELECT ts::timestamp, SUM(val::numeric) OVER (ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW v10 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW v11 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;
CREATE CONTINUOUS VIEW v12 AS SELECT ts::timestamp, AVG(val::numeric) OVER (PARTITION BY ts ORDER BY ts) FROM stream;

-- Multiple streams
CREATE CONTINUOUS VIEW v13 AS SELECT s0.a::integer, s1.b::integer FROM s0, s1;
CREATE CONTINUOUS VIEW v14 AS SELECT s0.a::integer, s1.b::integer, s2.c::text FROM s0, s1, s2;

-- Stream-table JOINs
CREATE TABLE t0 (id INTEGER);
CREATE CONTINUOUS VIEW j0 AS SELECT s0.id::integer AS s0_id, t0.id AS t0_id FROM s0 JOIN t0 ON s0.id = t0.id;
CREATE CONTINUOUS VIEW j1 AS SELECT s.x::integer, t0.id FROM stream s JOIN t0 ON s.x = t0.id;

-- Stream-stream JOINs
CREATE CONTINUOUS VIEW v17 AS SELECT s0.id::integer as id0, s1.id::integer as id1 FROM s0 JOIN s1 ON s0.id = s1.id;
CREATE CONTINUOUS VIEW v18 AS SELECT s0.id::integer AS id0, s1.id::integer AS id1 FROM stream s0 JOIN another_stream s1 ON s0.id = s1.id WHERE s0.id > 10 ORDER BY s1.id DESC;

-- Stream-table-stream JOINs
CREATE table sts (id INTEGER);
CREATE CONTINUOUS VIEW stsjoin AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM s0 JOIN sts ON s0.id = sts.id JOIN s1 ON sts.id = s1.x;
CREATE CONTINUOUS VIEW stsjoin2 AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM stream s0 JOIN sts ON s0.id = sts.id JOIN s1 ON sts.id = s1.id::integer WHERE sts.id > 42;
CREATE CONTINUOUS VIEW stsjoin2 AS SELECT s0.id::integer AS id0, s1.x::integer, sts.id AS id1 FROM stream s0 INNER JOIN sts ON s0.id = sts.id RIGHT OUTER JOIN s1 ON sts.id = s1.id::integer WHERE sts.id > 42;

-- Now let's verify our error handling and messages
-- Stream column doesn't have a type
CREATE CONTINUOUS VIEW v19 AS SELECT col FROM stream;

-- Column not qualified with a stream
CREATE CONTINUOUS VIEW v20 AS SELECT id::integer FROM s0, s1;

-- Column has multiple types
CREATE CONTINUOUS VIEW v21 AS SELECT id::integer AS id0, id::text AS id1 FROM stream;

-- Verify all relevant types are recognized
CREATE CONTINUOUS VIEW types AS SELECT
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
CREATE CONTINUOUS VIEW v22 AS SELECT arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW v23 AS SELECT key::text, arrival_timestamp FROM test_stream;
CREATE CONTINUOUS VIEW v24 AS SELECT key::text FROM test_stream WHERE arrival_timestamp < clock_timestamp();
CREATE CONTINUOUS VIEW v25 AS SELECT key::text, arrival_timestamp::timestamptz FROM test_stream;
CREATE CONTINUOUS VIEW v26 AS SELECT key::text, arrival_timestamp FROM test_stream WHERE arrival_timestamp::timestamptz < clock_timestamp();
