CREATE CONTINUOUS VIEW cont_tg_cv AS SELECT count(*) FROM cont_tg_stream;
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

select pg_sleep(2);

-- Invalid triggers
CREATE TRIGGER cont_tg BEFORE INSERT ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER DELETE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER INSERT ON cont_tg_cv FOR EACH STATEMENT EXECUTE PROCEDURE cont_tg_func();
CREATE TRIGGER cont_tg AFTER TRUNCATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();

-- Custom trigger function
CREATE TRIGGER cont_tg AFTER INSERT ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
select pipeline_trigger_debug('sync');

INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

-- This shouldn't do anything
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

DROP TRIGGER cont_tg ON cont_tg_cv;
TRUNCATE CONTINUOUS VIEW cont_tg_cv;
TRUNCATE TABLE cont_tg_t;
CREATE TRIGGER cont_tg AFTER UPDATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
select pipeline_trigger_debug('sync');
-- This shouldn't do anything
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;

DROP TRIGGER cont_tg ON cont_tg_cv;
TRUNCATE CONTINUOUS VIEW cont_tg_cv;
TRUNCATE TABLE cont_tg_t;
CREATE TRIGGER cont_tg AFTER INSERT OR UPDATE ON cont_tg_cv FOR EACH ROW EXECUTE PROCEDURE cont_tg_func();
select pipeline_trigger_debug('sync');
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
INSERT INTO cont_tg_stream (x) VALUES (1), (1), (1); SELECT pg_sleep(2);
SELECT * FROM cont_tg_t;
