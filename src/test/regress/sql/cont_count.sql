CREATE CONTINUOUS VIEW test_count AS SELECT k::text, COUNT(*) FROM stream_cqcount GROUP BY k;

INSERT INTO stream_cqcount (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream_cqcount (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

INSERT INTO stream_cqcount (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x');
INSERT INTO stream_cqcount (k) VALUES ('x'), ('x'), ('x'), ('x'), ('x'), ('x'), ('y'), ('y'), ('y'), ('y'), ('y'), ('y');

SELECT * FROM test_count ORDER BY k;

DROP CONTINUOUS VIEW test_count;
