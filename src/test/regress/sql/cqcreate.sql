-- Simple ones
CREATE CONTINUOUS VIEW cqcreate0 AS SELECT key::integer FROM stream;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate0';
SELECT gc FROM pipeline_query WHERE name='cqcreate0';
\d+ cqcreate0;
\d+ cqcreate0_pdb;
CREATE CONTINUOUS VIEW cqcreate1 AS SELECT substring(url::text, 1, 2) FROM stream;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate1';
\d+ cqcreate1;
\d+ cqcreate1_pdb;
CREATE CONTINUOUS VIEW cqcreate2 AS SELECT key::integer, substring(value::text, 1, 2) AS s FROM stream;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate2';
\d+ cqcreate2;
\d+ cqcreate2_pdb;

-- Group by projections
CREATE CONTINUOUS VIEW cqcreate3 AS SELECT key::text, COUNT(*), SUM(value::int8) FROM stream GROUP BY key;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate3';
\d+ cqcreate3;
\d+ cqcreate3_pdb;
CREATE CONTINUOUS VIEW cqcreate4 AS SELECT COUNT(*), SUM(value::int8) FROM stream GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate4';
\d+ cqcreate4;
\d+ cqcreate4_pdb;

-- Sliding window queries
CREATE CONTINUOUS VIEW cqcreate5 AS SELECT key::text FROM stream WHERE arrival_timestamp > (clock_timestamp() - interval '5' second);
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate5';
SELECT gc FROM pipeline_query WHERE name='cqcreate5';
\d+ cqcreate5;
\d+ cqcreate5_pdb;
CREATE CONTINUOUS VIEW cqcreate6 AS SELECT COUNT(*) FROM stream WHERE arrival_timestamp > (clock_timestamp() - interval '5' second) GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_query WHERE name='cqcreate6';
SELECT gc FROM pipeline_query WHERE name='cqcreate5';
\d+ cqcreate6;
\d+ cqcreate6_pdb;

-- These use a combine state column
CREATE CONTINUOUS VIEW cvavg AS SELECT key::text, AVG(x::float8) AS float_avg, AVG(y::integer) AS int_avg, AVG(ts0::timestamp - ts1::timestamp) AS internal_avg FROM stream GROUP BY key;
\d+ cvavg;
\d+ cvavg_pdb;

CREATE CONTINUOUS VIEW cvjson AS SELECT json_agg(x::text) AS count_col FROM stream;
\d+ cvjson;
\d+ cvjson_pdb;

CREATE CONTINUOUS VIEW cvjsonobj AS SELECT json_object_agg(key::text, value::integer) FROM stream;
\d+ cvjsonobj;
\d+ cvjsonobj_pdb;

-- But these aggregates don't
CREATE CONTINUOUS VIEW cvcount AS SELECT SUM(x::integer + y::float8) AS sum_col FROM stream;
\d+ cvcount;
\d+ cvcount_pdb;

CREATE CONTINUOUS VIEW cvarray AS SELECT COUNT(*) as count_col FROM stream;
\d+ cvarray;
\d+ cvarray_pdb;

CREATE CONTINUOUS VIEW cvtext AS SELECT key::text, string_agg(substring(s::text, 1, 2), ',') FROM stream GROUP BY key;
\d+ cvtext;
\d+ cvtext_pdb;

-- Check for expressions containing aggregates
CREATE CONTINUOUS VIEW cqaggexpr1 AS SELECT COUNT(*) + SUM(x::int) FROM stream;
\d+ cqaggexpr1;
\d+ cqaggexpr1_pdb;

CREATE CONTINUOUS VIEW cqaggexpr2 AS SELECT key::text, AVG(x::float) + MAX(y::integer) AS value FROM stream GROUP BY key;
\d+ cqaggexpr2;
\d+ cqaggexpr2_pdb;

CREATE CONTINUOUS VIEW cqaggexpr3 AS SELECT key::text, COUNT(*) AS value FROM stream WHERE arrival_timestamp > (clock_timestamp() - interval '5' second) GROUP BY key;
\d+ cqaggexpr3;
\d+ cqaggexpr3_pdb;

CREATE CONTINUOUS VIEW cqaggexpr4 AS SELECT key::text, floor(AVG(x::float)) AS value FROM stream GROUP BY key;
\d+ cqaggexpr4;
\d+ cqaggexpr4_pdb;

CREATE CONTINUOUS VIEW cqgroupby AS SELECT k0::text, k1::integer, COUNT(*) FROM stream GROUP BY k0, k1;
\d+ cqgroupby
\d+ cqgroupby_pdb;

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
