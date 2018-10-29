SET IntervalStyle to postgres;

CREATE FOREIGN TABLE create_cont_stream (key integer, url text, value text) SERVER pipelinedb;

-- Simple ones
CREATE VIEW cqcreate0 AS SELECT key FROM create_cont_stream;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate0';
SELECT pg_get_viewdef('cqcreate0');
\d+ cqcreate0_mrel;
\d+ cqcreate0_osrel;

CREATE VIEW cqcreate1 AS SELECT substring(url, 1, 2) FROM create_cont_stream;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate1';
SELECT pg_get_viewdef('cqcreate1');
\d+ cqcreate1_mrel;
\d+ cqcreate1_osrel;

CREATE VIEW cqcreate2 AS SELECT key, substring(value, 1, 2) AS s FROM create_cont_stream;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate2';
SELECT pg_get_viewdef('cqcreate2');
\d+ cqcreate2_mrel;
\d+ cqcreate2_osrel;

DROP FOREIGN TABLE create_cont_stream CASCADE;

CREATE FOREIGN TABLE create_cont_stream (key text, value int8, x float8, y int8, z int2, ts0 timestamp, ts1 timestamp) SERVER pipelinedb;
-- Group by projections
CREATE VIEW cqcreate3 AS SELECT key, COUNT(*), SUM(value) FROM create_cont_stream GROUP BY key;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate3';
SELECT pg_get_viewdef('cqcreate3');
\d+ cqcreate3_mrel;
\d+ cqcreate3_osrel;

CREATE VIEW cqcreate4 AS SELECT COUNT(*), SUM(value) FROM create_cont_stream GROUP BY key;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate4';
SELECT pg_get_viewdef('cqcreate4');
\d+ cqcreate4_mrel;
\d+ cqcreate4_osrel;

-- Sliding window queries
CREATE VIEW cqcreate5 AS SELECT key FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds');
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate5';
SELECT pg_get_viewdef('cqcreate5');
\d+ cqcreate5_mrel;
\d+ cqcreate5_osrel;

CREATE VIEW cqcreate6 AS SELECT COUNT(*) FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key::text;
SELECT COUNT(*) FROM pipelinedb.get_views() WHERE name = 'cqcreate6';
SELECT pg_get_viewdef('cqcreate6');
\d+ cqcreate6_mrel;
\d+ cqcreate6_osrel;

-- These use a combine state column
CREATE VIEW cvavg AS SELECT key, AVG(x) AS float_avg, AVG(y) AS int_avg, AVG(ts0 - ts1) AS internal_avg FROM create_cont_stream GROUP BY key;
SELECT pg_get_viewdef('cvavg');
\d+ cvavg_mrel;
\d+ cvavg_osrel;

CREATE VIEW cvjson AS SELECT json_agg(key) AS count_col FROM create_cont_stream;
SELECT pg_get_viewdef('cvjson');
\d+ cvjson_mrel;
\d+ cvjson_osrel;

CREATE VIEW cvjsonobj AS SELECT json_object_agg(key, value) FROM create_cont_stream;
SELECT pg_get_viewdef('cvjsonobj');
\d+ cvjsonobj_mrel;
\d+ cvjsonobj_osrel;

-- But these aggregates don't
CREATE VIEW cvcount AS SELECT SUM(z + y) AS sum_col FROM create_cont_stream;
SELECT pg_get_viewdef('cvcount');
\d+ cvcount_mrel;
\d+ cvcount_osrel;

CREATE VIEW cvarray AS SELECT COUNT(*) as count_col FROM create_cont_stream;
SELECT pg_get_viewdef('cvarray');
\d+ cvarray_mrel;
\d+ cvarray_osrel;

CREATE VIEW cvtext AS SELECT key, string_agg(substring(key, 1, 2), ',') FROM create_cont_stream GROUP BY key;
SELECT pg_get_viewdef('cvtext');
\d+ cvtext_mrel;
\d+ cvtext_osrel;

-- Check for expressions containing aggregates
CREATE VIEW cqaggexpr1 AS SELECT COUNT(*) + SUM(y) FROM create_cont_stream;
SELECT pg_get_viewdef('cqaggexpr1');
\d+ cqaggexpr1_mrel;
\d+ cqaggexpr1_osrel;

CREATE VIEW cqaggexpr2 AS SELECT key, AVG(x) + MAX(y) AS value FROM create_cont_stream GROUP BY key;
SELECT pg_get_viewdef('cqaggexpr2');
\d+ cqaggexpr2_mrel;
\d+ cqaggexpr2_osrel;

CREATE VIEW cqaggexpr3 AS SELECT key, COUNT(*) AS value FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key;
SELECT pg_get_viewdef('cqaggexpr3');
\d+ cqaggexpr3_mrel;
\d+ cqaggexpr3_osrel;

CREATE VIEW cqaggexpr4 AS SELECT key, floor(AVG(x)) AS value FROM create_cont_stream GROUP BY key;
SELECT pg_get_viewdef('cqaggexpr4');
\d+ cqaggexpr4_mrel;
\d+ cqaggexpr4_osrel;

CREATE VIEW cqgroupby AS SELECT key, x, COUNT(*) FROM create_cont_stream GROUP BY key, x;
SELECT pg_get_viewdef('cqgroupby');
\d+ cqgroupby_mrel;
\d+ cqgroupby_osrel;

CREATE VIEW multigroupindex AS SELECT key, x, y, z, value, COUNT(*) FROM create_cont_stream
GROUP BY key, x, y, z, value;

SELECT pg_get_viewdef('multigroupindex');
\d+ multigroupindex_mrel;
\d+ multigroupindex_osrel;

DROP FOREIGN TABLE create_cont_stream CASCADE;

CREATE FOREIGN TABLE create_cont_stream (x int, y int) SERVER pipelinedb;
-- A user-specified fillfactor should override the default
CREATE VIEW withff WITH (fillfactor = 42) AS SELECT COUNT(*) FROM create_cont_stream;
\d+ withff_mrel;

-- It shouldn't be possible to DROP a continuous view with DROP VIEW, and vice-versa
CREATE VIEW ccvv AS SELECT * FROM generate_series(1, 10);
DROP VIEW ccvv;
DROP VIEW ccvv;

CREATE VIEW ccvv AS SELECT COUNT(*) FROM create_cont_stream;
DROP VIEW ccvv;
DROP VIEW ccvv;

-- Subselects aren't allowed in a CV's target list
CREATE TABLE ccvt (x integer);
CREATE VIEW noss AS SELECT y, (SELECT x FROM ccvt WHERE x = 1) FROM create_cont_stream;
DROP TABLE ccvt;

-- arrival_timestamp is reserved
CREATE VIEW arrts AS SELECT x AS arrival_timestamp FROM create_cont_stream;
CREATE VIEW arrts AS SELECT arrival_timestamp AS arrival_timestamp FROM create_cont_stream;
DROP VIEW arrts;

-- WITH sw
CREATE VIEW ma0 WITH (sw = '1 day') AS SELECT COUNT(*) FROM create_cont_stream;
SELECT pg_get_viewdef('ma0');

-- sw must be a valid interval string
CREATE VIEW mainvalid WITH (sw = 42) AS SELECT COUNT(*) FROM create_cont_stream;
CREATE VIEW mainvalid WITH (sw = 42.1) AS SELECT COUNT(*) FROM create_cont_stream;
CREATE VIEW mainvalid WITH (sw = 'not an interval') AS SELECT COUNT(*) FROM create_cont_stream;

CREATE VIEW mawhere WITH (sw = '1 day') AS SELECT COUNT(*) FROM create_cont_stream
WHERE x::integer = 1;
SELECT pg_get_viewdef('mawhere');

DROP VIEW mawhere;

DROP FOREIGN TABLE create_cont_stream CASCADE;
CREATE FOREIGN TABLE create_cont_stream (key integer, url text, value text) SERVER pipelinedb;

CREATE VIEW ccv0 AS SELECT 1, count(*) FROM create_cont_stream;
CREATE VIEW ccv1 AS SELECT 1 FROM create_cont_stream;
CREATE VIEW ccv2 AS SELECT 1 AS x, 2 AS y FROM create_cont_stream;
CREATE VIEW ccv3 AS SELECT 1 AS x, 2 AS y, count(*) FROM create_cont_stream;
CREATE VIEW ccv4 AS SELECT 1 + 1 AS x, 2 + 2 AS y, count(*) FROM create_cont_stream;
CREATE VIEW ccv5 AS SELECT 1 + ('1'::integer + 1), count(*) FROM create_cont_stream;
CREATE VIEW ccv6 AS SELECT 1 + ('1'::integer + 1), count(*) FROM create_cont_stream GROUP BY key % 2;
CREATE VIEW ccv7 AS SELECT 1 + ('1'::integer + 1) AS col, key, count(*) FROM create_cont_stream GROUP BY key;
CREATE VIEW ccv8 AS SELECT 1 + ('1'::integer + 1) AS col, key + 1 AS g, count(*) FROM create_cont_stream GROUP BY g;

INSERT INTO create_cont_stream (key) VALUES (0), (1), (2);
INSERT INTO create_cont_stream (key) VALUES (0), (1), (2);

SELECT * FROM ccv0;
SELECT * FROM ccv1;
SELECT * FROM ccv2;
SELECT * FROM ccv3;
SELECT * FROM ccv4;
SELECT * FROM ccv5;
SELECT * FROM ccv6 ORDER BY count;
SELECT * FROM ccv7 ORDER BY key;
SELECT * FROM ccv8 ORDER BY g;

DROP FOREIGN TABLE create_cont_stream CASCADE;

-- Custom type
CREATE TYPE custom_type AS (integerone integer, integertwo integer);
CREATE FOREIGN TABLE create_cont_stream(val custom_type, x float8) SERVER pipelinedb;
CREATE VIEW type_cv as SELECT val, count(*) FROM create_cont_stream GROUP BY val;

CREATE VIEW tts AS SELECT COUNT(*) FROM create_cont_stream WHERE to_timestamp(x) > clock_timestamp() - interval '3 months';

-- Verify that we can't create triggers on continuous views
CREATE VIEW trigcv AS SELECT COUNT(*) FROM create_cont_stream;
CREATE TRIGGER trig AFTER INSERT OR UPDATE ON trigcv EXECUTE PROCEDURE pipelinedb.insert_into_stream('create_cont_stream');

CREATE VIEW altercv AS SELECT COUNT(*) FROM create_cont_stream;

-- Verify that we can't alter a CV's columns
ALTER VIEW altercv ALTER COUNT SET DEFAULT 1;

-- Verify that we can't alter a CV's options
ALTER VIEW altercv SET (security_barrier);

-- Verify that we can rename a CV
ALTER VIEW altercv RENAME TO altercv_renamed;

-- Verify that we can change a CV's schema
CREATE SCHEMA altertest;
ALTER VIEW altercv_renamed SET SCHEMA altertest;
SELECT * FROM altertest.altercv_renamed;

-- Verify that we cannot replace an existing CV
CREATE OR REPLACE VIEW altercv_renamed WITH (action=materialize) AS SELECT COUNT(*) FROM create_cont_stream;

DROP SCHEMA altertest CASCADE;
DROP FOREIGN TABLE create_cont_stream CASCADE;

-- Verify that sw_column parameters are validated correctly
CREATE FOREIGN TABLE create_sw_col_s (x integer, ts timestamptz) SERVER pipelinedb;

-- No sw option
CREATE VIEW create_sw_col0 WITH (sw_column = 'ts') AS SELECT count(*) FROM create_sw_col_s;

-- Wrong argument types
CREATE VIEW create_sw_col0 WITH (sw = '1 day', sw_column = wrong_type) AS SELECT count(*) FROM create_sw_col_s;
CREATE VIEW create_sw_col0 WITH (sw = '1 day', sw_column = 0) AS SELECT count(*) FROM create_sw_col_s;

-- Ok
CREATE VIEW create_sw_col0 WITH (sw = '1 day', sw_column = 'ts') AS SELECT count(*) FROM create_sw_col_s;

DROP FOREIGN TABLE create_sw_col_s CASCADE;

-- Verify that we don't allow CREATE OR REPLACE semantics on CQs
CREATE FOREIGN TABLE cor_s (x integer) SERVER pipelinedb;

CREATE VIEW cor0 AS SELECT count(*) FROM cor_s;
CREATE OR REPLACE VIEW cor0 AS SELECT count(*) FROM cor_s;

DROP FOREIGN TABLE cor_s CASCADE;
