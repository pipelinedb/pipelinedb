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
