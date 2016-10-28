CREATE STREAM cqtablesample_stream (x int);

CREATE CONTINUOUS VIEW cqtablesample AS SELECT count(*) FROM cqtablesample_stream TABLESAMPLE SYSTEM(10);
CREATE CONTINUOUS VIEW cqtablesample AS SELECT count(*) FROM cqtablesample_stream TABLESAMPLE BERNOULLI('a');

CREATE CONTINUOUS VIEW cqtablesample AS SELECT count(*) FROM cqtablesample_stream TABLESAMPLE BERNOULLI(10);

INSERT INTO cqtablesample_stream SELECT generate_series(1, 100000) x;

SELECT count > 9500 AND count < 10500 FROM cqtablesample;
