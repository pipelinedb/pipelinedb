CREATE DATABASE pipeline_stats_db;
\c pipeline_stats_db

CREATE CONTINUOUS VIEW test_pipeline_stats0 AS SELECT COUNT(*) FROM test_pipeline_stats_stream;

SELECT pg_sleep(2);
SELECT output_rows, errors, cv_create, cv_drop FROM pipeline_stats WHERE type = 'combiner';
SELECT input_rows, input_bytes, errors FROM pipeline_stats WHERE type = 'worker';

DROP CONTINUOUS VIEW test_pipeline_stats0;

SELECT pg_sleep(2);
SELECT output_rows, errors, cv_create, cv_drop FROM pipeline_stats WHERE type = 'combiner';
SELECT input_rows, input_bytes, errors FROM pipeline_stats WHERE type = 'worker';

CREATE CONTINUOUS VIEW test_pipeline_stats0 AS SELECT COUNT(*) FROM test_pipeline_stats_stream;
CREATE CONTINUOUS VIEW test_pipeline_stats1 AS SELECT COUNT(*) FROM test_pipeline_stats_stream;
INSERT INTO test_pipeline_stats_stream (x) SELECT generate_series(1, 1000) AS x;

SELECT pg_sleep(2);
SELECT output_rows, errors, cv_create, cv_drop FROM pipeline_stats WHERE type = 'combiner';
SELECT input_rows, input_bytes, errors FROM pipeline_stats WHERE type = 'worker';

DROP CONTINUOUS VIEW test_pipeline_stats0;
DROP CONTINUOUS VIEW test_pipeline_stats1;

SELECT pg_sleep(2);
SELECT output_rows, errors, cv_create, cv_drop FROM pipeline_stats WHERE type = 'combiner';
SELECT input_rows, input_bytes, errors FROM pipeline_stats WHERE type = 'worker';

DEACTIVATE;
SELECT pg_sleep(2);

\c regression
DROP DATABASE pipeline_stats_db;
