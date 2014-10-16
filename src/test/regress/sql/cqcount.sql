SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM stream GROUP BY k;

ACTIVATE test_count;

INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE;

SELECT * FROM test_count;

ACTIVATE test_count;

INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

DEACTIVATE;

SELECT * FROM test_count;

DROP CONTINUOUS VIEW test_count;
