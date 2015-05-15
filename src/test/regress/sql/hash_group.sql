
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

