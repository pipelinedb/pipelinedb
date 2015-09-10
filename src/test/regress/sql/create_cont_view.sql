SET IntervalStyle to postgres;

-- Simple ones
CREATE CONTINUOUS VIEW cqcreate0 AS SELECT key::integer FROM create_cont_stream1;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate0';
SELECT gc FROM pipeline_query WHERE name = 'cqcreate0';
\d+ cqcreate0;
\d+ cqcreate0_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate0');

CREATE CONTINUOUS VIEW cqcreate1 AS SELECT substring(url::text, 1, 2) FROM create_cont_stream1;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate1';
\d+ cqcreate1;
\d+ cqcreate1_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate1');

CREATE CONTINUOUS VIEW cqcreate2 AS SELECT key::integer, substring(value::text, 1, 2) AS s FROM create_cont_stream1;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate2';
\d+ cqcreate2;
\d+ cqcreate2_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate2');

-- Group by projections
CREATE CONTINUOUS VIEW cqcreate3 AS SELECT key::text, COUNT(*), SUM(value::int8) FROM cont_create_stream2 GROUP BY key;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate3';
\d+ cqcreate3;
\d+ cqcreate3_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate3');

CREATE CONTINUOUS VIEW cqcreate4 AS SELECT COUNT(*), SUM(value::int8) FROM cont_create_stream2 GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate4';
\d+ cqcreate4;
\d+ cqcreate4_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate4');

-- Sliding window queries
CREATE CONTINUOUS VIEW cqcreate5 AS SELECT key::text FROM cont_create_stream2 WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds');
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate5';
SELECT gc FROM pipeline_query WHERE name = 'cqcreate5';
\d+ cqcreate5;
\d+ cqcreate5_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate5');

CREATE CONTINUOUS VIEW cqcreate6 AS SELECT COUNT(*) FROM cont_create_stream2 WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_query WHERE name = 'cqcreate6';
SELECT gc FROM pipeline_query WHERE name = 'cqcreate6';
\d+ cqcreate6;
\d+ cqcreate6_mrel0;
SELECT pipeline_get_overlay_viewdef('cqcreate6');

-- These use a combine state column
CREATE CONTINUOUS VIEW cvavg AS SELECT key::text, AVG(x::float8) AS float_avg, AVG(y::integer) AS int_avg, AVG(ts0::timestamp - ts1::timestamp) AS internal_avg FROM cont_create_stream2 GROUP BY key;
\d+ cvavg;
\d+ cvavg_mrel0;
SELECT pipeline_get_overlay_viewdef('cvavg');

CREATE CONTINUOUS VIEW cvjson AS SELECT json_agg(x::text) AS count_col FROM create_cont_stream1;
\d+ cvjson;
\d+ cvjson_mrel0;
SELECT pipeline_get_overlay_viewdef('cvjson');

CREATE CONTINUOUS VIEW cvjsonobj AS SELECT json_object_agg(key::text, value::integer) FROM cont_create_stream2;
\d+ cvjsonobj;
\d+ cvjsonobj_mrel0;
SELECT pipeline_get_overlay_viewdef('cvjsonobj');

-- But these aggregates don't
CREATE CONTINUOUS VIEW cvcount AS SELECT SUM(x::integer + y::float8) AS sum_col FROM cont_create_stream2;
\d+ cvcount;
\d+ cvcount_mrel0;
SELECT pipeline_get_overlay_viewdef('cvcount');

CREATE CONTINUOUS VIEW cvarray AS SELECT COUNT(*) as count_col FROM create_cont_stream1;
\d+ cvarray;
\d+ cvarray_mrel0;
SELECT pipeline_get_overlay_viewdef('cvarray');

CREATE CONTINUOUS VIEW cvtext AS SELECT key::text, string_agg(substring(s::text, 1, 2), ',') FROM cont_create_stream2 GROUP BY key;
\d+ cvtext;
\d+ cvtext_mrel0;
SELECT pipeline_get_overlay_viewdef('cvtext');

-- Check for expressions containing aggregates
CREATE CONTINUOUS VIEW cqaggexpr1 AS SELECT COUNT(*) + SUM(x::int) FROM cont_create_stream2;
\d+ cqaggexpr1;
\d+ cqaggexpr1_mrel0;
SELECT pipeline_get_overlay_viewdef('cqaggexpr1');

CREATE CONTINUOUS VIEW cqaggexpr2 AS SELECT key::text, AVG(x::float) + MAX(y::integer) AS value FROM cont_create_stream2 GROUP BY key;
\d+ cqaggexpr2;
\d+ cqaggexpr2_mrel0;
SELECT pipeline_get_overlay_viewdef('cqaggexpr2');

CREATE CONTINUOUS VIEW cqaggexpr3 AS SELECT key::text, COUNT(*) AS value FROM cont_create_stream2 WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key;
\d+ cqaggexpr3;
\d+ cqaggexpr3_mrel0;
SELECT pipeline_get_overlay_viewdef('cqaggexpr3');

CREATE CONTINUOUS VIEW cqaggexpr4 AS SELECT key::text, floor(AVG(x::float)) AS value FROM cont_create_stream2 GROUP BY key;
\d+ cqaggexpr4;
\d+ cqaggexpr4_mrel0;
SELECT pipeline_get_overlay_viewdef('cqaggexpr4');

CREATE CONTINUOUS VIEW cqgroupby AS SELECT k0::text, k1::integer, COUNT(*) FROM create_cont_stream1 GROUP BY k0, k1;
\d+ cqgroupby
\d+ cqgroupby_mrel0;
SELECT pipeline_get_overlay_viewdef('cqgroupby');

CREATE CONTINUOUS VIEW multigroupindex AS SELECT a::text, b::int8, c::int4, d::int2, e::float8, COUNT(*) FROM create_cont_stream1
GROUP BY a, b, c, d, e;

\d+ multigroupindex;
\d+ multigroupindex_mrel0;
SELECT pipeline_get_overlay_viewdef('multigroupindex');

-- A user-specified fillfactor should override the default
CREATE CONTINUOUS VIEW withff WITH (fillfactor = 42) AS SELECT COUNT(*) FROM stream;
\d+ withff_mrel0;

-- It shouldn't be possible to DROP a continuous view with DROP VIEW, and vice-versa
CREATE VIEW ccvv AS SELECT * FROM generate_series(1, 10);
DROP CONTINUOUS VIEW ccvv;
DROP VIEW ccvv;

CREATE CONTINUOUS VIEW ccvv AS SELECT COUNT(*) FROM stream;
DROP VIEW ccvv;
DROP CONTINUOUS VIEW ccvv;

-- Subselects aren't allowed in a CV's target list
CREATE TABLE ccvt (x integer);
CREATE CONTINUOUS VIEW noss AS SELECT y::integer, (SELECT x FROM ccvt WHERE x = 1) FROM stream;
DROP TABLE ccvt;

-- arrival_timestamp is reserved
CREATE CONTINUOUS VIEW arrts AS SELECT x::integer AS arrival_timestamp FROM stream;
CREATE CONTINUOUS VIEW arrts AS SELECT arrival_timestamp AS arrival_timestamp FROM stream;
DROP CONTINUOUS VIEW arrts;

DROP CONTINUOUS VIEW cqcreate0;
DROP CONTINUOUS VIEW cqcreate1;
DROP CONTINUOUS VIEW cqcreate2;
DROP CONTINUOUS VIEW cqcreate3;
DROP CONTINUOUS VIEW cqcreate4;
DROP CONTINUOUS VIEW cqcreate5;
DROP CONTINUOUS VIEW cqcreate6;
DROP CONTINUOUS VIEW cvavg;
DROP CONTINUOUS VIEW cvjson;
DROP CONTINUOUS VIEW cvjsonobj;
DROP CONTINUOUS VIEW cvcount;
DROP CONTINUOUS VIEW cvarray;
DROP CONTINUOUS VIEW cvtext;
DROP CONTINUOUS VIEW cqaggexpr1;
DROP CONTINUOUS VIEW cqaggexpr2;
DROP CONTINUOUS VIEW cqaggexpr3;
DROP CONTINUOUS VIEW cqaggexpr4;
DROP CONTINUOUS VIEW cqgroupby;
DROP CONTINUOUS VIEW multigroupindex;
DROP CONTINUOUS VIEW withff;
