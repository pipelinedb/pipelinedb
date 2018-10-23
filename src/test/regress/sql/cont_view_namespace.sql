CREATE FOREIGN TABLE test_ns_stream (x int) SERVER pipelinedb;

CREATE VIEW test_cvn0 AS SELECT COUNT(*) FROM test_ns_stream;
CREATE VIEW nonexistent.cv AS SELECT COUNT(*) FROM test_ns_stream;

CREATE SCHEMA test_cvn_schema0;
CREATE FOREIGN TABLE test_cvn_schema0.test_ns_stream (x int) SERVER pipelinedb;
CREATE VIEW test_cvn_schema0.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema0.test_ns_stream;

CREATE SCHEMA test_cvn_schema1;
CREATE FOREIGN TABLE test_cvn_schema1.test_ns_stream (x int) SERVER pipelinedb;
CREATE VIEW test_cvn_schema1.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema1.test_ns_stream;

SELECT schema, name FROM pipelinedb.get_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

ALTER SCHEMA test_cvn_schema0 RENAME TO test_cvn_schema0_new;

CREATE VIEW test_cvn_schema0_new.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema0_new.test_ns_stream;
CREATE VIEW test_cvn_schema0_new.test_cvn1 AS SELECT COUNT(*) FROM test_cvn_schema0_new.test_ns_stream;

SELECT schema, name FROM pipelinedb.get_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

DROP SCHEMA test_cvn_schema0_new;
DROP SCHEMA test_cvn_schema0_new CASCADE;

SELECT schema, name FROM pipelinedb.get_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

DROP VIEW test_cvn0;
DROP SCHEMA test_cvn_schema1 CASCADE;

SELECT schema, name FROM pipelinedb.get_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

CREATE FOREIGN TABLE test_cvn_stream (x int) SERVER pipelinedb;
CREATE VIEW test_cvn0 AS SELECT x::int FROM test_cvn_stream;

CREATE SCHEMA test_cvn_schema0;
CREATE FOREIGN TABLE test_cvn_schema0.test_cvn_stream (x int, y text) SERVER pipelinedb;
CREATE VIEW test_cvn_schema0.test_cvn0 AS SELECT x::int, y::text FROM test_cvn_schema0.test_cvn_stream;

SELECT schema, name FROM pipelinedb.get_streams() WHERE name='test_cvn_stream' ORDER BY schema;

INSERT INTO test_cvn_stream (x) VALUES (1);
INSERT INTO test_cvn_schema0.test_cvn_stream (x, y) VALUES (2, 2), (3, 3);

SELECT * FROM test_cvn0;
SELECT * FROM test_cvn_schema0.test_cvn0 ORDER BY x;

DROP FOREIGN TABLE test_ns_stream CASCADE;
DROP FOREIGN TABLE test_cvn_stream CASCADE;
DROP SCHEMA test_cvn_schema0 CASCADE;
