SELECT freq(freq_agg(x), 10) FROM generate_series(1, 100) AS x;
SELECT freq(freq_agg(x), 10) FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT freq(freq_agg(x::text), '10') FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT freq(freq_agg(x::text), 'not here') FROM (SELECT generate_series(1, 100) AS x UNION ALL SELECT generate_series(1, 100) AS x) _;
SELECT freq(freq_agg(x::text), 'first') FROM (SELECT 'first' AS x FROM generate_series(1, 100) UNION ALL SELECT 'second' AS x FROM generate_series(1, 100)) _;

SELECT freq(freq_merge_agg, 50)
  FROM (SELECT freq_merge_agg(freq_agg) FROM
    (SELECT freq_agg(x) FROM generate_series(1, 100) AS x
      UNION ALL
     SELECT freq_agg(x) FROM generate_series(1, 100) AS x) _) _;