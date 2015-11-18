CREATE CONTINUOUS VIEW cont_matrel AS SELECT COUNT(*) FROM cont_matrel_stream;

INSERT INTO cont_matrel_stream (x) VALUES (1);
SELECT * FROM cont_matrel;

INSERT INTO cont_matrel_mrel (count) VALUES (1);
UPDATE cont_matrel_mrel SET count = 2;
DELETE FROM cont_matrel_mrel;

SET continuous_query_materialization_table_updatable TO ON;

UPDATE cont_matrel_mrel SET count = 2;
SELECT * FROM cont_matrel;
INSERT INTO cont_matrel_stream (x) VALUES (1);
SELECT * FROM cont_matrel;

DELETE FROM cont_matrel_mrel;
SELECT * FROM cont_matrel;
INSERT INTO cont_matrel_stream (x) VALUES (1);
SELECT * FROM cont_matrel;

TRUNCATE CONTINUOUS VIEW cont_matrel;
INSERT INTO cont_matrel_mrel (count) VALUES (5);
SELECT * FROM cont_matrel;
INSERT INTO cont_matrel_stream (x) VALUES (1);
SELECT * FROM cont_matrel;
