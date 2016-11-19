SELECT hll_cardinality(hll_agg('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' || x)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x * 1.1)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x::numeric)) FROM generate_series(1, 100) AS x;
SELECT hll_cardinality(hll_agg(x)) FROM generate_series(1, 10000) AS x;
SELECT hll_cardinality(hll_agg(x)) FROM generate_series(1, 100000) AS x;

SELECT hll_cardinality(hll_union_agg(hll_agg)) FROM (SELECT hll_agg(x) FROM generate_series(1, 100) AS x UNION ALL SELECT hll_agg(x) FROM generate_series(101, 200) AS x) _;
SELECT hll_cardinality(hll_union_agg(hll_agg)) FROM (SELECT hll_agg(x) FROM generate_series(1, 100) AS x UNION ALL SELECT hll_agg(x) FROM generate_series(101, 2000) AS x) _;
