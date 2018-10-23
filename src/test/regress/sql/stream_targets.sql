CREATE FOREIGN TABLE test_stream_targets_stream (x int) SERVER pipelinedb;

CREATE VIEW test_stream_targets0 AS SELECT COUNT(*) FROM test_stream_targets_stream;
CREATE VIEW test_stream_targets1 AS SELECT COUNT(*) FROM test_stream_targets_stream;
CREATE VIEW test_stream_targets2 AS SELECT COUNT(*) FROM test_stream_targets_stream;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION pipelinedb.stream_targets TO "test_stream_targets1, test_stream_targets2";

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION pipelinedb.stream_targets TO test_stream_targets2;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION pipelinedb.stream_targets TO DEFAULT;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SELECT * FROM test_stream_targets0;
SELECT * FROM test_stream_targets1;
SELECT * FROM test_stream_targets2;

DROP FOREIGN TABLE test_stream_targets_stream CASCADE;
