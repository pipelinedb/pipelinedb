SELECT fss_topk(fss_agg(x, 10)) FROM generate_series(1, 100) AS x;
SELECT fss_topk_values(fss_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;

SELECT fss_topk_freqs(fss_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;

SELECT fss_topk(fss_agg(x, 10)) FROM ( 
  SELECT generate_series(1, 100)
    UNION ALL
  SELECT generate_series(1, 10)
    UNION ALL
  SELECT generate_series(1, 30)) AS x;
