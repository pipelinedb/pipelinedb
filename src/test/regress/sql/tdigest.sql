SELECT tdigest_quantile(tdigest_agg(x), 0.50) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_quantile(tdigest_agg(x), 0.10) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_quantile(tdigest_agg(x), 0.05) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_quantile(tdigest_agg(x), 0.90) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_quantile(tdigest_agg(x), 0.99) FROM generate_serieS(1, 1000) AS x;

SELECT tdigest_cdf(tdigest_agg(x), 1) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 5) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 10) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 500) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 999) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 1000) FROM generate_serieS(1, 1000) AS x;
SELECT tdigest_cdf(tdigest_agg(x), 2000) FROM generate_serieS(1, 1000) AS x;
