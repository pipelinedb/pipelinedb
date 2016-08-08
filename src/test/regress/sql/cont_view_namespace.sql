CREATE STREAM test_ns_stream (x int);

CREATE CONTINUOUS VIEW test_cvn0 AS SELECT COUNT(*) FROM test_ns_stream;
CREATE CONTINUOUS VIEW nonexistent.cv AS SELECT COUNT(*) FROM test_ns_stream;

CREATE SCHEMA test_cvn_schema0;
CREATE STREAM test_cvn_schema0.test_ns_stream (x int);
CREATE CONTINUOUS VIEW test_cvn_schema0.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema0.test_ns_stream;

CREATE SCHEMA test_cvn_schema1;
CREATE STREAM test_cvn_schema1.test_ns_stream (x int);
CREATE CONTINUOUS VIEW test_cvn_schema1.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema1.test_ns_stream;

SELECT schema, name FROM pipeline_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

ALTER SCHEMA test_cvn_schema0 RENAME TO test_cvn_schema0_new;

CREATE CONTINUOUS VIEW test_cvn_schema0_new.test_cvn0 AS SELECT COUNT(*) FROM test_cvn_schema0_new.test_ns_stream;
CREATE CONTINUOUS VIEW test_cvn_schema0_new.test_cvn1 AS SELECT COUNT(*) FROM test_cvn_schema0_new.test_ns_stream;

SELECT schema, name FROM pipeline_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

DROP SCHEMA test_cvn_schema0_new;
DROP SCHEMA test_cvn_schema0_new CASCADE;

SELECT schema, name FROM pipeline_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

DROP CONTINUOUS VIEW test_cvn0;
DROP SCHEMA test_cvn_schema1 CASCADE;

SELECT schema, name FROM pipeline_views() WHERE name LIKE '%test_cvn%' ORDER BY schema, name;

CREATE STREAM test_cvn_stream (x int);
CREATE CONTINUOUS VIEW test_cvn0 AS SELECT x::int FROM test_cvn_stream;

CREATE SCHEMA test_cvn_schema0;
CREATE STREAM test_cvn_schema0.test_cvn_stream (x int, y text);
CREATE CONTINUOUS VIEW test_cvn_schema0.test_cvn0 AS SELECT x::int, y::text FROM test_cvn_schema0.test_cvn_stream;

SELECT schema, name FROM pipeline_streams() WHERE name='test_cvn_stream' ORDER BY schema;

INSERT INTO test_cvn_stream (x) VALUES (1);
INSERT INTO test_cvn_schema0.test_cvn_stream (x, y) VALUES (2, 2), (3, 3);

SELECT * FROM test_cvn0;
SELECT * FROM test_cvn_schema0.test_cvn0 ORDER BY x;

DROP STREAM test_ns_stream CASCADE;
DROP STREAM test_cvn_stream CASCADE;
DROP SCHEMA test_cvn_schema0 CASCADE;
