CREATE FOREIGN TABLE cont_activate_s (x int) SERVER pipelinedb;

CREATE VIEW cont_activate_v0 AS SELECT count(*) FROM cont_activate_s;
CREATE VIEW cont_activate_v1 AS SELECT count(*) FROM cont_activate_s;

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

SELECT pipelinedb.deactivate('cont_activate_v0');

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

SELECT pipelinedb.deactivate('cont_activate_v1');

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

SELECT pipelinedb.activate('cont_activate_v0');

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

SELECT pipelinedb.activate('cont_activate_v1');

INSERT INTO cont_activate_s (x) SELECT generate_series(1, 100) AS x;

SELECT * FROM cont_activate_v0;
SELECT * FROM cont_activate_v1;

DROP VIEW cont_activate_v0;
DROP VIEW cont_activate_v1;

DROP FOREIGN TABLE cont_activate_s CASCADE;
