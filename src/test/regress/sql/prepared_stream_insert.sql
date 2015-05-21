CREATE CONTINUOUS VIEW prep_insert0 AS SELECT COUNT(*) FROM prep_insert_stream;
CREATE CONTINUOUS VIEW prep_insert1 AS SELECT sum(x::float8) AS fsum, sum(y::int8) AS isum FROM prep_insert_stream;
CREATE CONTINUOUS VIEW prep_insert2 AS SELECT sum(x::integer) AS isum, sum(y::int4) AS i4sum FROM prep_insert_stream;

PREPARE prep0 AS INSERT INTO prep_insert_stream (x) VALUES ($1);
PREPARE prep1 AS INSERT INTO prep_insert_stream (x, y) VALUES ($1, $2);

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

SELECT * FROM prep_insert0;
SELECT * FROM prep_insert1;
SELECT * FROM prep_insert2;

DROP CONTINUOUS VIEW prep_insert0;
DROP CONTINUOUS VIEW prep_insert1;
DROP CONTINUOUS VIEW prep_insert2;
