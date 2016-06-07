CREATE CONTINUOUS VIEW cont_tg_cv AS SELECT x::int, count(*) FROM cont_tg_stream group by x;
CREATE TABLE cont_tg_t (count int);
CREATE OR REPLACE FUNCTION cont_tg_func()
RETURNS trigger AS
$$
BEGIN
 INSERT INTO cont_tg_t (count) VALUES (NEW.count);
 RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION bad_tg_func()
RETURNS trigger AS
$$
BEGIN
 INSERT INTO does_not_exist(count) VALUES (NEW.count);
 RETURN NEW;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER bad_tg AFTER INSERT OR UPDATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE bad_tg_func();
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
DROP TRIGGER bad_tg on cont_tg_cv;

TRUNCATE CONTINUOUS VIEW cont_tg_cv;
TRUNCATE TABLE cont_tg_t;

-- Invalid triggers
CREATE TRIGGER cont_tg BEFORE INSERT ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER DELETE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER INSERT ON cont_tg_cv FOR EACH STATEMENT EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER TRUNCATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();

-- Custom trigger function
CREATE TRIGGER cont_tg AFTER INSERT ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
-- This shouldn't do anything
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

DROP TRIGGER cont_tg ON cont_tg_cv;
TRUNCATE CONTINUOUS VIEW cont_tg_cv;
TRUNCATE TABLE cont_tg_t;
CREATE TRIGGER cont_tg AFTER UPDATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
-- This shouldn't do anything
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

DROP TRIGGER cont_tg ON cont_tg_cv;
TRUNCATE CONTINUOUS VIEW cont_tg_cv;
TRUNCATE TABLE cont_tg_t;
CREATE TRIGGER cont_tg AFTER INSERT OR UPDATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

CREATE CONTINUOUS VIEW cont_tg_v2 AS SELECT count(DISTINCT x::int) FROM stream;
TRUNCATE TABLE cont_tg_t;

CREATE OR REPLACE FUNCTION tgfunc()
 RETURNS trigger AS
 $$
 BEGIN
  INSERT INTO cont_tg_t (count) VALUES (NEW.count);
  RETURN NEW;
 END;
 $$
 LANGUAGE plpgsql;

CREATE TRIGGER tg AFTER UPDATE OR INSERT ON cont_tg_v2 FOR EACH ROW EXECUTE PROCEDURE tgfunc();
INSERT INTO stream (x) VALUES (4);
INSERT INTO stream (x) VALUES (5);
INSERT INTO stream (x) VALUES (6);
SELECT pg_sleep(2);

SELECT * FROM cont_tg_t;

DROP TRIGGER tg ON cont_tg_v2;
TRUNCATE CONTINUOUS VIEW cont_tg_v2;
TRUNCATE TABLE cont_tg_t;

CREATE TRIGGER tg AFTER UPDATE OR INSERT ON cont_tg_v2 FOR EACH ROW WHEN (NEW.count > 5) EXECUTE PROCEDURE tgfunc();

INSERT INTO stream (x) VALUES (4);
INSERT INTO stream (x) VALUES (5);
INSERT INTO stream (x) VALUES (6);
INSERT INTO stream (x) VALUES (7);
INSERT INTO stream (x) VALUES (8);
INSERT INTO stream (x) VALUES (9);
INSERT INTO stream (x) VALUES (10);
INSERT INTO stream (x) VALUES (11);
SELECT pg_sleep(2);

SELECT * FROM cont_tg_t;

DROP CONTINUOUS VIEW cont_tg_cv CASCADE;
DROP CONTINUOUS VIEW cont_tg_v2 CASCADE;
