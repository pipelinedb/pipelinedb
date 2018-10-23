CREATE FOREIGN TABLE test_pk_stream (x int) SERVER pipelinedb;

CREATE VIEW test_pk0 WITH (pk='x') AS SELECT x::integer, COUNT(*) FROM test_pk_stream GROUP BY x;
\d+ test_pk0_mrel

INSERT INTO test_pk_stream (x) SELECT generate_series(1, 20) AS x;
INSERT INTO test_pk_stream (x) SELECT generate_series(1, 30) AS x;

SELECT * FROM test_pk0 ORDER BY x;
DROP VIEW test_pk0;

CREATE VIEW test_pk1 WITH (pk='count') AS SELECT x::integer, COUNT(*) FROM test_pk_stream GROUP BY x;
\d+ test_pk1_mrel

INSERT INTO test_pk_stream (x) VALUES (0);
INSERT INTO test_pk_stream (x) VALUES (0);
INSERT INTO test_pk_stream (x) VALUES (1);
INSERT INTO test_pk_stream (x) VALUES (1);

SELECT * FROM test_pk1 ORDER BY x;

DROP VIEW test_pk1;

CREATE VIEW wrong_arg_type WITH (pk=1) AS SELECT COUNT(*) FROM test_pk_stream;
CREATE VIEW no_column WITH (pk='not_here') AS SELECT COUNT(*) FROM test_pk_stream;

DROP FOREIGN TABLE test_pk_stream CASCADE;
