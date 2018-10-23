CREATE FOREIGN TABLE test_gs_stream (x int, y int, z int) SERVER pipelinedb;

CREATE VIEW test_gs0 AS SELECT x::integer, y::integer, COUNT(*) FROM test_gs_stream
	GROUP BY CUBE (x, y);

SELECT pipelinedb.get_worker_querydef('test_gs0');
SELECT pipelinedb.get_combiner_querydef('test_gs0');
SELECT pg_get_viewdef('test_gs0');

CREATE VIEW test_gs1 AS SELECT x::integer, y::integer, z::integer, COUNT(*) FROM test_gs_stream
	GROUP BY GROUPING SETS (x), (x, y), (x, y, z), (x, z);

SELECT pipelinedb.get_worker_querydef('test_gs1');
SELECT pipelinedb.get_combiner_querydef('test_gs1');
SELECT pg_get_viewdef('test_gs1');

CREATE VIEW test_gs2 AS SELECT x::integer, y::integer, COUNT(*) FROM test_gs_stream
	GROUP BY GROUPING SETS (x), (x, y);

SELECT pipelinedb.get_worker_querydef('test_gs2');
SELECT pipelinedb.get_combiner_querydef('test_gs2');
SELECT pg_get_viewdef('test_gs2');

CREATE VIEW test_gs3 WITH (sw = '5 seconds') AS SELECT x::integer, y::integer, z::integer, COUNT(*) FROM test_gs_stream
	GROUP BY GROUPING SETS (x, y, z);

SELECT pipelinedb.get_worker_querydef('test_gs3');
SELECT pipelinedb.get_combiner_querydef('test_gs3');
SELECT pg_get_viewdef('test_gs3');

INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;
INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;
INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;

SELECT * FROM test_gs0 ORDER BY x, y;
SELECT * FROM test_gs1 ORDER BY x, y, z;
SELECT * FROM test_gs2 ORDER BY x, y;
SELECT * FROM test_gs3 ORDER BY x, y, z;

INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;
INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;
INSERT INTO test_gs_stream (x, y, z) SELECT x, x * 2 AS y, -x AS z FROM generate_series(1, 100) AS x;

SELECT * FROM test_gs0 ORDER BY x, y;
SELECT * FROM test_gs1 ORDER BY x, y, z;
SELECT * FROM test_gs2 ORDER BY x, y;
SELECT * FROM test_gs3 ORDER BY x, y, z;

SELECT pg_sleep(5);
SELECT * FROM test_gs3 ORDER BY x, y, z;

DROP VIEW test_gs0;
DROP VIEW test_gs1;
DROP VIEW test_gs2;
DROP VIEW test_gs3;

DROP FOREIGN TABLE test_gs_stream CASCADE;
