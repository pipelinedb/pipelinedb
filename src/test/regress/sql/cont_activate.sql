CREATE CONTINUOUS VIEW cont_activate_v0 AS SELECT count(*) FROM cont_activate_s;
CREATE CONTINUOUS VIEW cont_activate_v1 AS SELECT count(*) FROM cont_activate_s;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

DEACTIVATE cont_activate_v0;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

DEACTIVATE cont_activate_v1;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

ACTIVATE cont_activate_v0;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

ACTIVATE cont_activate_v1;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

DROP CONTINUOUS VIEW cont_activate_v0;
DROP CONTINUOUS VIEW cont_activate_v1;
