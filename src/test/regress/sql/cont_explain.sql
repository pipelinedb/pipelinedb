CREATE CONTINUOUS VIEW test_cont_explain1 AS SELECT COUNT(*) FROM test_cont_explain_stream;
CREATE CONTINUOUS VIEW test_cont_explain2 AS SELECT x::int, COUNT(*) FROM test_cont_explain_stream GROUP BY x;
CREATE CONTINUOUS VIEW test_cont_explain3 AS SELECT x::int, y::text, SUM(x) FROM test_cont_explain_stream GROUP BY x, y;

EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain1;
EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain2;
EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain3;

INSERT INTO test_cont_explain_stream (x, y) VALUES (1, 'hello');

EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain1;
EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain2;
EXPLAIN CONTINUOUS VIEW (COSTS off, VERBOSE on) test_cont_explain3;

DROP CONTINUOUS VIEW test_cont_explain1;
DROP CONTINUOUS VIEW test_cont_explain2;
DROP CONTINUOUS VIEW test_cont_explain3;
