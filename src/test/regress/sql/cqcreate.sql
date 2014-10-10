-- Simple ones
CREATE CONTINUOUS VIEW cqcreate0 AS SELECT key::integer FROM stream;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate0';
\d+ cqcreate0;
\d+ cqcreate0_pdb;
CREATE CONTINUOUS VIEW cqcreate1 AS SELECT substring(url::text, 1, 2) FROM stream;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate1';
\d+ cqcreate1;
\d+ cqcreate1_pdb;
CREATE CONTINUOUS VIEW cqcreate2 AS SELECT key::integer, substring(value::text, 1, 2) AS s FROM stream;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate2';
\d+ cqcreate2;
\d+ cqcreate2_pdb;

-- Group by projections
CREATE CONTINUOUS VIEW cqcreate3 AS SELECT key::text, COUNT(*), SUM(value::int8) FROM stream GROUP BY key;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate3';
\d+ cqcreate3;
\d+ cqcreate3_pdb;
CREATE CONTINUOUS VIEW cqcreate4 AS SELECT COUNT(*), SUM(value::int8) FROM stream GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate4';
\d+ cqcreate4;
\d+ cqcreate4_pdb;

-- Sliding window queries
CREATE CONTINUOUS VIEW cqcreate5 AS SELECT key::text FROM stream WHERE arrival_timestamp > (clock_timestamp() - interval '5' second);
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate5';
\d+ cqcreate5;
\d+ cqcreate5_pdb;
CREATE CONTINUOUS VIEW cqcreate6 AS SELECT COUNT(*) FROM stream WHERE arrival_timestamp > (clock_timestamp() - interval '5' second) GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_queries WHERE name='cqcreate6';
\d+ cqcreate6;
\d+ cqcreate6_pdb;

-- AVG needs a combine state column
CREATE CONTINUOUS VIEW cvavg AS SELECT key::text, AVG(x::float8) AS avg_col FROM stream GROUP BY key;
\d+ cvavg;
\d+ cvavg_pdb;

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

CREATE CONTINUOUS VIEW cvjson AS SELECT json_agg(x::text) AS count_col FROM stream;
\d+ cvjson;
\d+ cvjson_pdb;

CREATE CONTINUOUS VIEW cvjsonobj AS SELECT json_object_agg(key::text, value::integer) FROM stream;
\d+ cvjsonobj;
\d+ cvjsonobj_pdb;
