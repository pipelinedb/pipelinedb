SELECT cmsketch_frequency(cmsketch_agg(x), 10) FROM generate_series(1, 100) AS x;
SELECT cmsketch_frequency(cmsketch_agg(x), 10) FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT cmsketch_frequency(cmsketch_agg(x::text), '10') FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT cmsketch_frequency(cmsketch_agg(x::text), 'not here') FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT cmsketch_frequency(cmsketch_agg(x::text), 'first') FROM (SELECT 'first' AS x FROM generate_series(1, 100) UNION ALL SELECT 'second' AS x FROM generate_series(1, 100)) _;

SELECT cmsketch_frequency(cmsketch_merge_agg, 50)
  FROM (SELECT cmsketch_merge_agg(cmsketch_agg) FROM
    (SELECT cmsketch_agg(x) FROM generate_series(1, 100) AS x
      UNION ALL
     SELECT cmsketch_agg(x) FROM generate_series(1, 100) AS x) _) _;