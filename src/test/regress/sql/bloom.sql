SELECT bloom_contains((SELECT bloom_agg(g) FROM generate_series(1, 100) AS g), x) FROM generate_series(1, 110) AS x;

SELECT bloom_contains(
    (SELECT bloom_union_agg(bf) FROM  
      (SELECT bloom_agg(g) AS bf FROM generate_series(1, 100) AS g
        UNION ALL
       SELECT bloom_agg(g) AS bf FROM generate_series(101, 200) AS g) _), x)
FROM generate_series(1, 210) AS x;

SELECT bloom_cardinality(
    bloom_union(
      (SELECT bloom_agg(x) FROM generate_series(1, 1000) AS x),
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x),
      NULL));

-- Different BF dimension
SELECT bloom_cardinality(
    bloom_union(
      (SELECT bloom_agg(x, 0.5, 1000) FROM generate_series(1, 1000) AS x),
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x)));

-- Wrong type
SELECT bloom_cardinality(
    bloom_union(
      'not a bloom filter',
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x)));

SELECT bloom_cardinality(
    bloom_intersection(
      (SELECT bloom_agg(x) FROM generate_series(1, 1000) AS x),
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x),
      NULL));

-- Different BF dimension
SELECT bloom_cardinality(
    bloom_intersection(
      (SELECT bloom_agg(x, 0.5, 1000) FROM generate_series(1, 1000) AS x),
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x)));

-- Wrong type
SELECT bloom_cardinality(
    bloom_intersection(
      'not a bloom filter',
      (SELECT bloom_agg(x) FROM generate_series(100, 1100) AS x)));
