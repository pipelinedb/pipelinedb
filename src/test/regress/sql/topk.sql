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

-- hashed_topk_agg
CREATE TABLE hashed_topk_t AS SELECT generate_series(1, 1000) x;
INSERT INTO hashed_topk_t (x) SELECT 1 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 2 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 3 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 4 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 5 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 6 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 7 FROM generate_series(1, 10);
INSERT INTO hashed_topk_t (x) SELECT 42 FROM generate_series(1, 1000);
SELECT topk(hashed_topk_agg(x, 7, 1)) FROM hashed_topk_t;

DROP TABLE hashed_topk_t;
