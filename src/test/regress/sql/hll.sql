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

-- Byref but not varlena types
CREATE TABLE byref (uuid uuid, name name);
INSERT INTO byref (uuid, name) VALUES ('fe636e28-db82-43af-ac48-df26a4cda1f3', 'alice');
INSERT INTO byref (uuid, name) VALUES ('fe636e28-db82-43af-ac48-df26a4cda1f3', 'alice');

SELECT hll_cardinality(hll_agg(uuid)) FROM byref;
SELECT hll_cardinality(hll_agg(name)) FROM byref;

INSERT INTO byref (uuid, name) VALUES ('fff36e28-db82-43af-ac48-df26a4cda1f3', 'bob');
INSERT INTO byref (uuid, name) VALUES ('fff36e28-db82-43af-ac48-df26a4cda1f3', 'bob');

SELECT hll_cardinality(hll_agg(uuid)) FROM byref;
SELECT hll_cardinality(hll_agg(name)) FROM byref;
