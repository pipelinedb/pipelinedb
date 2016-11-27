SELECT hll_cardinality(hll_agg('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' || x)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x * 1.1)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x::numeric)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x)) FROM generate_series(1, 10000) AS x;
SELECT hll_cardinality(hll_agg(x)) FROM generate_series(1, 100000) AS x;

SELECT hll_cardinality(hll_union_agg(hll_agg)) FROM (SELECT hll_agg(x) FROM generate_series(1, 100) AS x UNION ALL SELECT hll_agg(x) FROM generate_series(101, 200) AS x) _;
SELECT hll_cardinality(hll_union_agg(hll_agg)) FROM (SELECT hll_agg(x) FROM generate_series(1, 100) AS x UNION ALL SELECT hll_agg(x) FROM generate_series(101, 2000) AS x) _;

SELECT hll_cardinality(
    hll_union(
      (SELECT hll_agg(x) FROM generate_series(1, 1000) AS x),
      (SELECT hll_agg(x) FROM generate_series(100, 1100) AS x),
      NULL));

-- Different HLL dimensions
SELECT hll_cardinality(
    hll_union(
      (SELECT hll_agg(x, 12) FROM generate_series(1, 1000) AS x),
      (SELECT hll_agg(x) FROM generate_series(100, 1100) AS x)));

-- Wrong type
SELECT hll_cardinality(
    hll_union(
      'not an hll',
      (SELECT hll_agg(x) FROM generate_series(100, 1100) AS x)));
