SELECT pipelinedb.hash_group(0::int, 0::int, 0::int, 0::int);
SELECT pipelinedb.hash_group('0'::text, '2015-02-01'::timestamp, 1.2);
SELECT pipelinedb.hash_group(null::text);
SELECT pipelinedb.hash_group(null::text, null::text);

SELECT pipelinedb.hash_group(1::int8, 2::int4);
SELECT pipelinedb.hash_group(1::int4, 2::int8);
SELECT pipelinedb.hash_group(0::int2, null::int2);
SELECT pipelinedb.hash_group(null::int2, null::int2);

SELECT date_trunc('second', '2015-01-01'::timestamp) + i * interval '1 second' AS ts, pipelinedb.ls_hash_group(date_trunc('second', '2015-01-01'::timestamp) + i * interval '1 second', 0::int4) AS key
FROM generate_series(1, 100) AS i ORDER BY key;

-- Ensure that hash index is created and cannot be dropped
CREATE FOREIGN TABLE hash_group_stream (x int, y timestamptz) SERVER pipelinedb;
CREATE VIEW hash_group AS SELECT x::int, COUNT(*) FROM hash_group_stream GROUP BY x;
CREATE VIEW ls_hash_group1 AS SELECT x::int, minute(y::timestamptz), COUNT(*) FROM hash_group_stream WHERE ( arrival_timestamp > clock_timestamp() - interval '5 hour' ) GROUP BY x, minute;
CREATE VIEW ls_hash_group2 AS SELECT x::int, y::timestamptz, COUNT(*) FROM hash_group_stream GROUP BY x, y;

\d+ hash_group_mrel;
\d+ ls_hash_group1_mrel;
\d+ ls_hash_group2_mrel;

DROP INDEX hash_group_mrel_expr_idx;
DROP INDEX ls_hash_group1_mrel_expr_idx;
DROP INDEX ls_hash_group2_mrel_expr_idx;

DROP FOREIGN TABLE hash_group_stream CASCADE;
