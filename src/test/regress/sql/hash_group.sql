SELECT hash_group(0::int, 0::int, 0::int, 0::int);
SELECT hash_group('0'::text, '2015-02-01'::timestamp, 1.2);
SELECT hash_group(null::text);
SELECT hash_group(null::text, null::text);

SELECT hash_group(1::int8, 2::int4);
SELECT hash_group(1::int4, 2::int8);
SELECT hash_group(0::int2, null::int2);
SELECT hash_group(null::int2, null::int2);

SELECT date_trunc('second', '2015-01-01'::timestamp) + i * interval '1 second' AS ts, ls_hash_group(date_trunc('second', '2015-01-01'::timestamp) + i * interval '1 second', 0::int4) AS key
FROM generate_series(1, 100) AS i ORDER BY key;

-- Ensure that hash index is created and cannot be dropped
CREATE CONTINUOUS VIEW hash_group AS SELECT x::int, COUNT(*) FROM hash_group_stream GROUP BY x;
CREATE CONTINUOUS VIEW ls_hash_group1 AS SELECT x::int, minute(y::timestamptz), COUNT(*) FROM hash_group_stream WHERE ( arrival_timestamp > clock_timestamp() - interval '5 hour' ) GROUP BY x, minute;
CREATE CONTINUOUS VIEW ls_hash_group2 AS SELECT x::int, y::timestamptz, COUNT(*) FROM hash_group_stream GROUP BY x, y;

\d+ hash_group_mrel;
\d+ ls_hash_group1_mrel;
\d+ ls_hash_group2_mrel;

DROP INDEX hash_group_mrel_expr_idx;
DROP INDEX ls_hash_group1_mrel_expr_idx;
DROP INDEX ls_hash_group2_mrel_expr_idx;

DROP CONTINUOUS VIEW hash_group;
DROP CONTINUOUS VIEW ls_hash_group1;
DROP CONTINUOUS VIEW ls_hash_group2;
