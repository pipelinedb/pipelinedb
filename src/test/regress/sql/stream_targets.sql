CREATE CONTINUOUS VIEW test_stream_targets0 AS SELECT COUNT(*) FROM test_stream_targets_stream;
CREATE CONTINUOUS VIEW test_stream_targets1 AS SELECT COUNT(*) FROM test_stream_targets_stream;
CREATE CONTINUOUS VIEW test_stream_targets2 AS SELECT COUNT(*) FROM test_stream_targets_stream;

ACTIVATE test_stream_targets0, test_stream_targets1, test_stream_targets2;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION stream_targets TO "test_stream_targets1, test_stream_targets2";

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION stream_targets TO test_stream_targets2;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

SET SESSION stream_targets TO DEFAULT;

INSERT INTO test_stream_targets_stream (x) VALUES (1);

DEACTIVATE test_stream_targets0, test_stream_targets1, test_stream_targets2;

SELECT * FROM test_stream_targets0;
SELECT * FROM test_stream_targets1;
SELECT * FROM test_stream_targets2;

DROP CONTINUOUS VIEW test_stream_targets0;
DROP CONTINUOUS VIEW test_stream_targets1;
DROP CONTINUOUS VIEW test_stream_targets2;
