-- Sanity checks
CREATE VIEW cqanalyze0 AS SELECT substring(url, 1, 2) FROM analyze_cont_stream;
CREATE VIEW cqanalyze1 AS SELECT CASE WHEN x = '1' THEN 1 END FROM analyze_cont_stream;
CREATE VIEW cqanalyze2 AS SELECT a FROM analyze_cont_stream GROUP BY a;
CREATE VIEW cqanalyze3 AS SELECT a FROM analyze_cont_stream WHERE a > 10 GROUP BY a;
CREATE VIEW cqanalyze4 AS SELECT a, b FROM analyze_cont_stream WHERE a > 10 GROUP BY a, b, c HAVING a < 12 AND c > 2;
CREATE VIEW cqanalyze5 AS SELECT substring(url, 1, 2) AS g, COUNT(*) FROM analyze_cont_stream GROUP BY g;
CREATE VIEW cqanalyze6 AS SELECT s.id FROM analyze_cont_stream s WHERE s.id < 10 AND s.value > 10;

-- Windows
CREATE VIEW cqanalyze10 AS SELECT ts, SUM(val) OVER (ORDER BY ts) FROM analyze_cont_stream;
CREATE VIEW cqanalyze11 AS SELECT ts, AVG(val) OVER (PARTITION BY ts ORDER BY ts) FROM analyze_cont_stream;
CREATE VIEW cqanalyze12 AS SELECT ts, AVG(val) OVER (PARTITION BY ts ORDER BY (ts + INTERVAL '1 day')) FROM analyze_cont_stream;

-- Stream-table JOINs
CREATE TABLE t0 (id INTEGER);
CREATE VIEW cqanalyze16 AS SELECT s0.id AS s0_id, t0.id AS t0_id FROM analyze_cont_stream s0 JOIN t0 ON s0.id = t0.id;
CREATE VIEW cqanalyze17 AS SELECT s.a, t0.id FROM analyze_cont_stream s JOIN t0 ON s.a = t0.id;

DROP FOREIGN TABLE analyze_cont_stream CASCADE;

-- Now let's verify our error handling and messages
CREATE FOREIGN TABLE analyze_cont_stream (x integer, key text) SERVER pipelinedb;

-- Stream column doesn't exist
CREATE VIEW cqanalyze23 AS SELECT col FROM analyze_cont_stream;

-- Verify that NOTICEs are properly shown when joining on unindexed columns
CREATE TABLE tnotice (x integer, y integer);
CREATE VIEW cvnotice0 AS SELECT s.x FROM analyze_cont_stream AS s JOIN tnotice ON s.x = tnotice.x;

-- tnotice.x NOTICE should only be shown once
CREATE VIEW cvnotice1 AS SELECT s.x FROM analyze_cont_stream AS s JOIN tnotice ON s.x = tnotice.x AND s.x = tnotice.x;
CREATE VIEW cvnotice2 AS SELECT s.x FROM analyze_cont_stream AS s, tnotice WHERE tnotice.x = s.x;

CREATE INDEX tnotice_idx ON tnotice(x);

-- No NOTICE should be given now that an index exists
CREATE VIEW cvnotice3 AS SELECT s.x FROM analyze_cont_stream AS s, tnotice WHERE tnotice.x = s.x;

DROP TABLE tnotice CASCADE;

-- Verify that we can't do wildcard selections in continuous queries
CREATE VIEW cqanalyze32 AS SELECT * from analyze_cont_stream;
CREATE VIEW cqanalyze33 AS SELECT * from analyze_cont_stream, t0;
CREATE VIEW cqanalyze34 AS SELECT t0.* from analyze_cont_stream, t0;
CREATE VIEW cqanalyze35 AS SELECT s.* from analyze_cont_stream s, t0;

-- Disallow sorting streams
CREATE VIEW cqanalyze36 AS SELECT key from analyze_cont_stream ORDER BY key;
CREATE VIEW cqanalyze37 AS SELECT key from analyze_cont_stream ORDER BY arrival_time;

-- Sliding window queries
CREATE VIEW cqanalyze38 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';
CREATE VIEW cqanalyze39 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' AND key::text='pipelinedb';
CREATE VIEW cqanalyze40 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE NOT arrival_timestamp > clock_timestamp() - interval '1 hour';
CREATE VIEW cqanalyze41 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' OR key::text='pipelinedb';
CREATE VIEW cqanalyze42 AS SELECT COUNT(*) FROM analyze_cont_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' AND arrival_timestamp > clock_timestamp() - interval '5 hour';

DROP FOREIGN TABLE analyze_cont_stream CASCADE;

CREATE FOREIGN TABLE analyze_cont_stream (g int, x int, y int, z text, a int) SERVER pipelinedb;
-- Hypothetical-set aggregates
CREATE VIEW cqanalyze45 AS SELECT g, percent_rank((1 + 3, 2, substring('xxx', 1, 2))) WITHIN GROUP (ORDER BY (x, y, z)) + rank((4, 5, 'x')) WITHIN GROUP (ORDER BY (x, y, substring(z, 1, 2))) FROM analyze_cont_stream GROUP BY g;
CREATE VIEW cqanalyze46 AS SELECT rank(0, 1) WITHIN GROUP (ORDER BY x, y) + rank(0) WITHIN GROUP (ORDER BY x) FROM analyze_cont_stream;

-- Number of arguments to HS function is inconsistent with the number of GROUP columns
CREATE VIEW error_not_created AS SELECT percent_rank(1) WITHIN GROUP (ORDER BY x, y, z) FROM analyze_cont_stream;

-- Types of arguments to HS function are inconsistent with GROUP column types
CREATE VIEW error_not_created AS SELECT g, dense_rank(2, 3, 4) WITHIN GROUP (ORDER BY x, y, z) FROM analyze_cont_stream GROUP BY g;

CREATE VIEW cqanalyze47 AS SELECT g, rank(2, 3, 4), sum(x + y + a) FROM analyze_cont_stream GROUP BY g;

-- Sliding windows
CREATE VIEW cqanalyze48 AS SELECT cume_dist(2) FROM analyze_cont_stream WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');
CREATE VIEW cqanalyze49 AS SELECT percent_rank(2) WITHIN GROUP (ORDER BY x), rank(2) WITHIN GROUP (ORDER BY x) FROM analyze_cont_stream WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');

DROP FOREIGN TABLE analyze_cont_stream CASCADE;

-- Verify that we get an error if we try to create a CV that only selects from tables
CREATE TABLE cqanalyze_table (id integer);
CREATE VIEW error_not_created WITH (action=materialize) AS SELECT cqanalyze_table.id::integer FROM cqanalyze_table;
DROP TABLE cqanalyze_table;

CREATE FOREIGN TABLE analyze_cont_stream (id int, x float, y int, z numeric, g int, k text) SERVER pipelinedb;
-- Verify that for stream-table joins, the correct error message is generated when the table is missing
CREATE VIEW  error_not_created AS SELECT s.id, t.tid FROM analyze_cont_stream s JOIN not_a_table t ON s.id = t.tid;
CREATE VIEW  error_not_created AS SELECT s.id, tid FROM analyze_cont_stream s JOIN not_a_table ON s.id = tid;

-- Ordered-set aggregates
CREATE VIEW cqanalyze50 AS SELECT g, percentile_cont(ARRAY[0.2, 0.8]) WITHIN GROUP (ORDER BY x), percentile_cont(0.9) WITHIN GROUP (ORDER BY y) + percentile_cont(0.1) WITHIN GROUP (ORDER BY z) AS col FROM analyze_cont_stream GROUP BY g;
CREATE VIEW cqanalyze51 AS SELECT g, percentile_cont(0.1) WITHIN GROUP (ORDER BY x + y) FROM analyze_cont_stream GROUP BY g;

-- Can only sort on a numeric expression
CREATE VIEW error_not_created AS SELECT percentile_cont(0.1, k) FROM analyze_cont_stream;

-- Sliding windows
CREATE VIEW cqanalyze52 AS SELECT g, percentile_cont(ARRAY[0.2, 0.8]) WITHIN GROUP (ORDER BY x), percentile_cont(0.9) WITHIN GROUP (ORDER BY y) + percentile_cont(0.1) WITHIN GROUP (ORDER BY z) AS col FROM analyze_cont_stream WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes') GROUP BY g;
CREATE VIEW cqanalyze53 AS SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY x + y) FROM analyze_cont_stream WHERE (arrival_timestamp > clock_timestamp() - interval '5 minutes');

CREATE VIEW funcs AS SELECT floor(x::float8), date_trunc('day', arrival_timestamp), COUNT(*) FROM analyze_cont_stream GROUP BY floor, date_trunc;
DROP FOREIGN TABLE analyze_cont_stream CASCADE;
DROP TABLE t0;

CREATE FOREIGN TABLE analyze_cont_stream (id int, ts timestamp, sid int, x int) SERVER pipelinedb;
-- Regression
CREATE VIEW cqregress1 AS SELECT id + avg(id) FROM analyze_cont_stream GROUP BY id;
CREATE VIEW cqregress2 AS SELECT date_trunc('hour', ts) AS ts FROM analyze_cont_stream;
CREATE VIEW cqregress3 AS SELECT stream0.sid::integer FROM analyze_cont_stream;
CREATE VIEW cqregress4 AS SELECT x FROM cqregress4;
CREATE VIEW cqregress5 AS SELECT count(DISTINCT x), percentile_cont(0.1) WITHIN GROUP (ORDER BY x) FROM analyze_cont_stream;
SELECT pg_get_viewdef('cqregress5');
CREATE VIEW cqregress6 AS SELECT id, avg(x) FROM analyze_cont_stream GROUP BY id;
SELECT pg_get_viewdef('cqregress6');
CREATE VIEW cqregress7 AS SELECT combine(avg) FROM cqregress6;
SELECT pg_get_viewdef('cqregress7');

CREATE VIEW cqregress8 WITH (action=transform) AS SELECT id FROM analyze_cont_stream;

DROP FOREIGN TABLE analyze_cont_stream CASCADE;

SELECT relname FROM pipelinedb.cont_query cq JOIN pg_class c ON cq.relid = c.oid;
