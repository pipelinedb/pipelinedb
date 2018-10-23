CREATE FOREIGN TABLE cont_freq_agg_stream (x integer, y text) SERVER pipelinedb;

CREATE VIEW test_freq_agg0 AS
  SELECT
    x % 10 AS g,
    freq_agg(x) AS x_freq,
    freq_agg(y) AS y_freq
 FROM cont_freq_agg_stream
GROUP BY g;

INSERT INTO cont_freq_agg_stream (x, y) SELECT x, x::text FROM generate_series(1, 1000) x;
INSERT INTO cont_freq_agg_stream (x, y) SELECT 42, '42'::text FROM generate_series(1, 1000) x;
INSERT INTO cont_freq_agg_stream (x, y) SELECT 0, '00'::text FROM generate_series(1, 100) x;
INSERT INTO cont_freq_agg_stream (x, y) SELECT 1, '01'::text FROM generate_series(1, 100) x;
INSERT INTO cont_freq_agg_stream (x, y) SELECT 2, '02'::text FROM generate_series(1, 100) x;
INSERT INTO cont_freq_agg_stream (x, y) SELECT 3, '03'::text FROM generate_series(1, 100) x;

SELECT g, freq(x_freq, 42) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '42') FROM test_freq_agg0 ORDER BY g;

SELECT g, freq(x_freq, 0) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '00') FROM test_freq_agg0 ORDER BY g;

SELECT g, freq(x_freq, 1) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '01') FROM test_freq_agg0 ORDER BY g;

SELECT g, freq(x_freq, 2) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '02') FROM test_freq_agg0 ORDER BY g;

SELECT g, freq(x_freq, 3) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '03') FROM test_freq_agg0 ORDER BY g;

INSERT INTO cont_freq_agg_stream (x, y) SELECT 42, '42'::text FROM generate_series(1, 1000) x;

SELECT g, freq(x_freq, 42) FROM test_freq_agg0 ORDER BY g;
SELECT g, freq(y_freq, '42') FROM test_freq_agg0 ORDER BY g;

DROP FOREIGN TABLE cont_freq_agg_stream CASCADE;
