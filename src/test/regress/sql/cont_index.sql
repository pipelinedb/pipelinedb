CREATE FOREIGN TABLE cont_idx_stream (x int, y int) SERVER pipelinedb;

CREATE VIEW test_cont_index0 AS SELECT x::integer, COUNT(*), AVG(x) FROM cont_idx_stream GROUP BY x;

CREATE INDEX test_ci_idx0 ON test_cont_index0 (x);
\d+ test_cont_index0_mrel

CREATE INDEX test_ci_idx1 ON test_cont_index0 (avg);
\d+ test_cont_index0_mrel

CREATE INDEX test_ci_idx2 ON test_cont_index0 (x, avg);
\d+ test_cont_index0_mrel

DROP VIEW test_cont_index0;

CREATE VIEW test_cont_index1 WITH (sw = '1 hour') AS SELECT x::integer, y::integer, COUNT(*), AVG(x) FROM cont_idx_stream GROUP BY x, y;

CREATE INDEX test_ci_idx0 ON test_cont_index1 (x);
\d+ test_cont_index1_mrel

CREATE INDEX test_ci_idx1 ON test_cont_index1 (avg);
\d+ test_cont_index1_mrel

CREATE INDEX test_ci_idx2 ON test_cont_index1 (x, avg);
\d+ test_cont_index1_mrel

CREATE INDEX test_ci_idx3 ON test_cont_index1 (x, y);
\d+ test_cont_index1_mrel

DROP VIEW test_cont_index1;

DROP FOREIGN TABLE cont_idx_stream CASCADE;
