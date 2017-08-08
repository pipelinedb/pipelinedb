CREATE STREAM cont_matrel_stream (x int);
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

SELECT truncate_continuous_view('cont_matrel');
INSERT INTO cont_matrel_mrel (count, "$pk") VALUES (5, 1);
INSERT INTO cont_matrel_mrel (count, "$pk") VALUES (10, 1);
SELECT * FROM cont_matrel;
INSERT INTO cont_matrel_stream (x) VALUES (1);
SELECT * FROM cont_matrel;

DROP CONTINUOUS VIEW cont_matrel;
DROP STREAM cont_matrel_stream CASCADE;
