CREATE FOREIGN TABLE prep_insert_stream (x float8, y int, z int) SERVER pipelinedb;

CREATE VIEW prep_insert0 AS SELECT COUNT(*) FROM prep_insert_stream;
CREATE VIEW prep_insert1 AS SELECT sum(x::float8) AS fsum, sum(y::int8) AS isum FROM prep_insert_stream;
CREATE VIEW prep_insert2 AS SELECT sum(x::integer) AS isum, sum(y::int4) AS i4sum FROM prep_insert_stream;

CREATE TABLE prep_insert_t (x integer, y integer, z integer);

PREPARE prep0 AS INSERT INTO prep_insert_stream (x) VALUES ($1);
PREPARE prep1 AS INSERT INTO prep_insert_stream (x, y) VALUES ($1, $2);
PREPARE prep_t AS INSERT INTO prep_insert_t (x, y, z) VALUES ($1, $2, $3);

EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);
EXECUTE prep0(1.5);

EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);
EXECUTE prep1(1, 1.1);

EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);
EXECUTE prep_t(0, 0, 0);

SELECT * FROM prep_insert0;
SELECT * FROM prep_insert1;
SELECT * FROM prep_insert2;

DROP FOREIGN TABLE prep_insert_stream CASCADE;
DROP TABLE prep_insert_t;
