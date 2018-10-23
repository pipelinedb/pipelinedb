SELECT topk(topk_agg(x, 10)) FROM generate_series(1, 100) AS x;
SELECT topk_values(topk_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;

SELECT topk_freqs(topk_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;

SELECT topk(topk_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;
