CREATE CONTINUOUS VIEW test_cvn0 AS SELECT COUNT(*) FROM stream;
CREATE CONTINUOUS VIEW nonexistent.cv AS SELECT COUNT(*) FROM stream;

CREATE SCHEMA test_cvn_schema0;
CREATE CONTINUOUS VIEW test_cvn_schema0.test_cvn0 AS SELECT COUNT(*) FROM stream;

CREATE SCHEMA test_cvn_schema1;
CREATE CONTINUOUS VIEW test_cvn_schema1.test_cvn0 AS SELECT COUNT(*) FROM stream;

SELECT n.nspname, pq.name FROM pipeline_query pq JOIN pg_namespace n ON pq.namespace = n.oid WHERE pq.name LIKE '%test_cvn%' ORDER BY n.nspname, pq.name;

ALTER SCHEMA test_cvn_schema0 RENAME TO test_cvn_schema0_new;

CREATE CONTINUOUS VIEW test_cvn_schema0_new.test_cvn0 AS SELECT COUNT(*) FROM stream;
CREATE CONTINUOUS VIEW test_cvn_schema0_new.test_cvn1 AS SELECT COUNT(*) FROM stream;

SELECT n.nspname, pq.name FROM pipeline_query pq JOIN pg_namespace n ON pq.namespace = n.oid WHERE pq.name LIKE '%test_cvn%' ORDER BY n.nspname, pq.name;

DROP SCHEMA test_cvn_schema0_new;
DROP SCHEMA test_cvn_schema0_new CASCADE;

SELECT n.nspname, pq.name FROM pipeline_query pq JOIN pg_namespace n ON pq.namespace = n.oid WHERE pq.name LIKE '%test_cvn%' ORDER BY n.nspname, pq.name;

DROP CONTINUOUS VIEW test_cvn0;
DROP SCHEMA test_cvn_schema1 CASCADE;

SELECT n.nspname, pq.name FROM pipeline_query pq JOIN pg_namespace n ON pq.namespace = n.oid WHERE pq.name LIKE '%test_cvn%' ORDER BY n.nspname, pq.name;

CREATE CONTINUOUS VIEW test_cvn0 AS SELECT x::int FROM test_cvn_stream;

CREATE SCHEMA test_cvn_schema0;
CREATE CONTINUOUS VIEW test_cvn_schema0.test_cvn0 AS SELECT x::int, y::text FROM test_cvn_stream;

SELECT namespace, name, "desc" FROM pipeline_stream WHERE name='test_cvn_stream';

INSERT INTO test_cvn_stream (x) VALUES (1);
INSERT INTO test_cvn_schema0.test_cvn_stream (x, y) VALUES (2, 2), (3, 3);

SELECT * FROM test_cvn0;
SELECT * FROM test_cvn_schema0.test_cvn0;

DROP CONTINUOUS VIEW test_cvn0;
DROP SCHEMA test_cvn_schema0 CASCADE;
