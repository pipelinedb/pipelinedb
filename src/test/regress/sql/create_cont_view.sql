SET IntervalStyle to postgres;

CREATE STREAM create_cont_stream (key integer, url text, value text);

-- Simple ones
CREATE CONTINUOUS VIEW cqcreate0 AS SELECT key FROM create_cont_stream;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate0';
\d+ cqcreate0;
\d+ cqcreate0_mrel;
\d+ cqcreate0_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate0');

CREATE CONTINUOUS VIEW cqcreate1 AS SELECT substring(url, 1, 2) FROM create_cont_stream;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate1';
\d+ cqcreate1;
\d+ cqcreate1_mrel;
\d+ cqcreate1_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate1');

CREATE CONTINUOUS VIEW cqcreate2 AS SELECT key, substring(value, 1, 2) AS s FROM create_cont_stream;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate2';
\d+ cqcreate2;
\d+ cqcreate2_mrel;
\d+ cqcreate2_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate2');

DROP STREAM create_cont_stream CASCADE;

CREATE STREAM create_cont_stream (key text, value int8, x float8, y int8, z int2, ts0 timestamp, ts1 timestamp);
-- Group by projections
CREATE CONTINUOUS VIEW cqcreate3 AS SELECT key, COUNT(*), SUM(value) FROM create_cont_stream GROUP BY key;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate3';
\d+ cqcreate3;
\d+ cqcreate3_mrel;
\d+ cqcreate3_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate3');

CREATE CONTINUOUS VIEW cqcreate4 AS SELECT COUNT(*), SUM(value) FROM create_cont_stream GROUP BY key;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate4';
\d+ cqcreate4;
\d+ cqcreate4_mrel;
\d+ cqcreate4_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate4');

-- Sliding window queries
CREATE CONTINUOUS VIEW cqcreate5 AS SELECT key FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds');
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate5';
\d+ cqcreate5;
\d+ cqcreate5_mrel;
\d+ cqcreate5_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate5');

CREATE CONTINUOUS VIEW cqcreate6 AS SELECT COUNT(*) FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key::text;
SELECT COUNT(*) FROM pipeline_views() WHERE name = 'cqcreate6';
\d+ cqcreate6;
\d+ cqcreate6_mrel;
\d+ cqcreate6_osrel;
SELECT pipeline_get_overlay_viewdef('cqcreate6');

-- These use a combine state column
CREATE CONTINUOUS VIEW cvavg AS SELECT key, AVG(x) AS float_avg, AVG(y) AS int_avg, AVG(ts0 - ts1) AS internal_avg FROM create_cont_stream GROUP BY key;
\d+ cvavg;
\d+ cvavg_mrel;
\d+ cvavg_osrel;
SELECT pipeline_get_overlay_viewdef('cvavg');

CREATE CONTINUOUS VIEW cvjson AS SELECT json_agg(key) AS count_col FROM create_cont_stream;
\d+ cvjson;
\d+ cvjson_mrel;
\d+ cvjson_osrel;
SELECT pipeline_get_overlay_viewdef('cvjson');

CREATE CONTINUOUS VIEW cvjsonobj AS SELECT json_object_agg(key, value) FROM create_cont_stream;
\d+ cvjsonobj;
\d+ cvjsonobj_mrel;
\d+ cvjsonobj_osrel;
SELECT pipeline_get_overlay_viewdef('cvjsonobj');

-- But these aggregates don't
CREATE CONTINUOUS VIEW cvcount AS SELECT SUM(z + y) AS sum_col FROM create_cont_stream;
\d+ cvcount;
\d+ cvcount_mrel;
\d+ cvcount_osrel;
SELECT pipeline_get_overlay_viewdef('cvcount');

CREATE CONTINUOUS VIEW cvarray AS SELECT COUNT(*) as count_col FROM create_cont_stream;
\d+ cvarray;
\d+ cvarray_mrel;
\d+ cvarray_osrel;
SELECT pipeline_get_overlay_viewdef('cvarray');

CREATE CONTINUOUS VIEW cvtext AS SELECT key, string_agg(substring(key, 1, 2), ',') FROM create_cont_stream GROUP BY key;
\d+ cvtext;
\d+ cvtext_mrel;
\d+ cvtext_osrel;
SELECT pipeline_get_overlay_viewdef('cvtext');

-- Check for expressions containing aggregates
CREATE CONTINUOUS VIEW cqaggexpr1 AS SELECT COUNT(*) + SUM(y) FROM create_cont_stream;
\d+ cqaggexpr1;
\d+ cqaggexpr1_mrel;
\d+ cqaggexpr1_osrel;
SELECT pipeline_get_overlay_viewdef('cqaggexpr1');

CREATE CONTINUOUS VIEW cqaggexpr2 AS SELECT key, AVG(x) + MAX(y) AS value FROM create_cont_stream GROUP BY key;
\d+ cqaggexpr2;
\d+ cqaggexpr2_mrel;
\d+ cqaggexpr2_osrel;
SELECT pipeline_get_overlay_viewdef('cqaggexpr2');

CREATE CONTINUOUS VIEW cqaggexpr3 AS SELECT key, COUNT(*) AS value FROM create_cont_stream WHERE arrival_timestamp > (clock_timestamp() - interval '5 seconds') GROUP BY key;
\d+ cqaggexpr3;
\d+ cqaggexpr3_mrel;
\d+ cqaggexpr3_osrel;
SELECT pipeline_get_overlay_viewdef('cqaggexpr3');

CREATE CONTINUOUS VIEW cqaggexpr4 AS SELECT key, floor(AVG(x)) AS value FROM create_cont_stream GROUP BY key;
\d+ cqaggexpr4;
\d+ cqaggexpr4_mrel;
\d+ cqaggexpr4_osrel;
SELECT pipeline_get_overlay_viewdef('cqaggexpr4');

CREATE CONTINUOUS VIEW cqgroupby AS SELECT key, x, COUNT(*) FROM create_cont_stream GROUP BY key, x;
\d+ cqgroupby
\d+ cqgroupby_mrel;
\d+ cqgroupby_osrel;
SELECT pipeline_get_overlay_viewdef('cqgroupby');

CREATE CONTINUOUS VIEW multigroupindex AS SELECT key, x, y, z, value, COUNT(*) FROM create_cont_stream
GROUP BY key, x, y, z, value;

\d+ multigroupindex;
\d+ multigroupindex_mrel;
\d+ multigroupindex_osrel;
SELECT pipeline_get_overlay_viewdef('multigroupindex');

DROP STREAM create_cont_stream CASCADE;

CREATE STREAM create_cont_stream (x int, y int);
-- A user-specified fillfactor should override the default
CREATE CONTINUOUS VIEW withff WITH (fillfactor = 42) AS SELECT COUNT(*) FROM create_cont_stream;
\d+ withff_mrel;

-- It shouldn't be possible to DROP a continuous view with DROP VIEW, and vice-versa
CREATE VIEW ccvv AS SELECT * FROM generate_series(1, 10);
DROP CONTINUOUS VIEW ccvv;
DROP VIEW ccvv;

CREATE CONTINUOUS VIEW ccvv AS SELECT COUNT(*) FROM create_cont_stream;
DROP VIEW ccvv;
DROP CONTINUOUS VIEW ccvv;

-- Subselects aren't allowed in a CV's target list
CREATE TABLE ccvt (x integer);
CREATE CONTINUOUS VIEW noss AS SELECT y, (SELECT x FROM ccvt WHERE x = 1) FROM create_cont_stream;
DROP TABLE ccvt;

-- arrival_timestamp is reserved
CREATE CONTINUOUS VIEW arrts AS SELECT x AS arrival_timestamp FROM create_cont_stream;
CREATE CONTINUOUS VIEW arrts AS SELECT arrival_timestamp AS arrival_timestamp FROM create_cont_stream;
DROP CONTINUOUS VIEW arrts;

-- WITH max_age
CREATE CONTINUOUS VIEW ma0 WITH (max_age = '1 day') AS SELECT COUNT(*) FROM create_cont_stream;
\d+ ma0;
CREATE VIEW ma1 WITH (max_age = '10 hours') AS SELECT COUNT(*) FROM ma0;
\d+ ma1;

-- max_age must be a valid interval string
CREATE CONTINUOUS VIEW mainvalid WITH (max_age = 42) AS SELECT COUNT(*) FROM create_cont_stream;
CREATE CONTINUOUS VIEW mainvalid WITH (max_age = 42.1) AS SELECT COUNT(*) FROM create_cont_stream;
CREATE CONTINUOUS VIEW mainvalid WITH (max_age = 'not an interval') AS SELECT COUNT(*) FROM create_cont_stream;

CREATE CONTINUOUS VIEW mawhere WITH (max_age = '1 day') AS SELECT COUNT(*) FROM create_cont_stream
WHERE x::integer = 1;
\d+ mawhere;

DROP CONTINUOUS VIEW mawhere;

-- max_age can't be used on non-sliding window continuous views
CREATE VIEW manosw WITH (max_age = '1 day') AS SELECT COUNT(*) FROM withff;

-- or in conjunction with another sliding-window predicate
CREATE VIEW manosw WITH (max_age = '1 day') AS SELECT COUNT(*) FROM create_cont_stream
WHERE arrival_timestamp > clock_timestamp() - interval '1 day';

DROP STREAM create_cont_stream CASCADE;

-- Custom type
CREATE TYPE custom_type AS (integerone integer, integertwo integer);
CREATE STREAM create_cont_stream(val custom_type, x float8);
CREATE CONTINUOUS VIEW type_cv as SELECT val, count(*) FROM create_cont_stream GROUP BY val;

CREATE CONTINUOUS VIEW tts AS SELECT COUNT(*) FROM create_cont_stream WHERE to_timestamp(x) > clock_timestamp() - interval '3 months';

DROP STREAM create_cont_stream CASCADE;
