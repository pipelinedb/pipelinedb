--
-- XC_TRIGGER
--

-- Creation of a trigger-based method to improve run of count queries
-- by incrementation and decrementation of statement-based and row-based counters

-- Create tables
CREATE TABLE xc_trigger_rep_tab (a int, b int) DISTRIBUTE BY REPLICATION;
CREATE TABLE xc_trigger_hash_tab (a int, b int) DISTRIBUTE BY HASH(a);
CREATE TABLE xc_trigger_rr_tab (a int, b int) DISTRIBUTE BY ROUNDROBIN;
CREATE TABLE xc_trigger_modulo_tab (a int, b int) DISTRIBUTE BY MODULO(a);
CREATE TABLE table_stats (table_name text primary key,
num_insert_query int DEFAULT 0,
num_update_query int DEFAULT 0,
num_delete_query int DEFAULT 0,
num_truncate_query int DEFAULT 0,
num_insert_row int DEFAULT 0,
num_update_row int DEFAULT 0,
num_delete_row int DEFAULT 0);

-- Insert default entries
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_rep_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_hash_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_rr_tab');
INSERT INTO table_stats (table_name) VALUES ('xc_trigger_modulo_tab');

-- Functions to manage stats of table
-- Count the number of queries run
CREATE FUNCTION count_insert_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_insert_query = num_insert_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_update_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_update_query = num_update_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_delete_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_delete_query = num_delete_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_truncate_query() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_truncate_query = num_truncate_query + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
-- Count the number of rows used
CREATE FUNCTION count_insert_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_insert_row = num_insert_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_update_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_update_row = num_update_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION count_delete_row() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE table_stats SET num_delete_row = num_delete_row + 1  WHERE table_name = TG_TABLE_NAME;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';

-- Define the events for each table
-- Replicated table
CREATE TRIGGER rep_count_insert_query AFTER INSERT ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER rep_count_update_query BEFORE UPDATE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER rep_count_delete_query BEFORE DELETE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER rep_count_truncate_query BEFORE TRUNCATE ON xc_trigger_rep_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER rep_count_insert_row BEFORE INSERT ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER rep_count_update_row BEFORE UPDATE ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER rep_count_delete_row BEFORE DELETE ON xc_trigger_rep_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Renaming of trigger based on a table
ALTER TRIGGER repcount_update_row ON my_table RENAME TO repcount_update_row2;
-- Hash table
CREATE TRIGGER hash_count_insert_query AFTER INSERT ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER hash_count_update_query BEFORE UPDATE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER hash_count_delete_query BEFORE DELETE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER hash_count_truncate_query BEFORE TRUNCATE ON xc_trigger_hash_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER hash_count_insert_row BEFORE INSERT ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER hash_count_update_row BEFORE UPDATE ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER hash_count_delete_row BEFORE DELETE ON xc_trigger_hash_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Round robin table
CREATE TRIGGER rr_count_insert_query AFTER INSERT ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER rr_count_update_query BEFORE UPDATE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER rr_count_delete_query BEFORE DELETE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER rr_count_truncate_query BEFORE TRUNCATE ON xc_trigger_rr_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER rr_count_insert_row BEFORE INSERT ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER rr_count_update_row BEFORE UPDATE ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER rr_count_delete_row BEFORE DELETE ON xc_trigger_rr_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();
-- Modulo table
CREATE TRIGGER modulo_count_insert_query AFTER INSERT ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_insert_query();
CREATE TRIGGER modulo_count_update_query BEFORE UPDATE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_update_query();
CREATE TRIGGER modulo_count_delete_query BEFORE DELETE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_delete_query();
CREATE TRIGGER modulo_count_truncate_query BEFORE TRUNCATE ON xc_trigger_modulo_tab
	FOR EACH STATEMENT EXECUTE PROCEDURE count_truncate_query();
CREATE TRIGGER modulo_count_insert_row BEFORE INSERT ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_insert_row();
CREATE TRIGGER modulo_count_update_row BEFORE UPDATE ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_update_row();
CREATE TRIGGER modulo_count_delete_row BEFORE DELETE ON xc_trigger_modulo_tab
	FOR EACH ROW EXECUTE PROCEDURE count_delete_row();

-- Insert some values to test the INSERT triggers
INSERT INTO xc_trigger_rep_tab VALUES (1,2);
INSERT INTO xc_trigger_rep_tab VALUES (3,4);
INSERT INTO xc_trigger_rep_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_hash_tab VALUES (1,2);
INSERT INTO xc_trigger_hash_tab VALUES (3,4);
INSERT INTO xc_trigger_hash_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_rr_tab VALUES (1,2);
INSERT INTO xc_trigger_rr_tab VALUES (3,4);
INSERT INTO xc_trigger_rr_tab VALUES (5,6),(7,8),(9,10);
INSERT INTO xc_trigger_modulo_tab VALUES (1,2);
INSERT INTO xc_trigger_modulo_tab VALUES (3,4);
INSERT INTO xc_trigger_modulo_tab VALUES (5,6),(7,8),(9,10);
SELECT * FROM table_stats ORDER BY table_name;

-- Update some values to test the UPDATE triggers
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_rep_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_hash_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_rr_tab SET b = b+1 WHERE a IN (5,7,9);
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a = 1;
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a = 3;
UPDATE xc_trigger_modulo_tab SET b = b+1 WHERE a IN (5,7,9);
SELECT * FROM table_stats ORDER BY table_name;

-- Delete some values to test the DELETE triggers
DELETE FROM xc_trigger_rep_tab WHERE a = 1;
DELETE FROM xc_trigger_rep_tab WHERE a = 3;
DELETE FROM xc_trigger_rep_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_hash_tab WHERE a = 1;
DELETE FROM xc_trigger_hash_tab WHERE a = 3;
DELETE FROM xc_trigger_hash_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_rr_tab WHERE a = 1;
DELETE FROM xc_trigger_rr_tab WHERE a = 3;
DELETE FROM xc_trigger_rr_tab WHERE a IN (5,7,9);
DELETE FROM xc_trigger_modulo_tab WHERE a = 1;
DELETE FROM xc_trigger_modulo_tab WHERE a = 3;
DELETE FROM xc_trigger_modulo_tab WHERE a IN (5,7,9);
SELECT * FROM table_stats ORDER BY table_name;

-- Truncate the table to test the TRUNCATE triggers
TRUNCATE xc_trigger_rep_tab;
TRUNCATE xc_trigger_hash_tab;
TRUNCATE xc_trigger_rr_tab;
TRUNCATE xc_trigger_modulo_tab;
SELECT * FROM table_stats ORDER BY table_name;

-- Clean up everything
DROP TABLE xc_trigger_rep_tab, xc_trigger_hash_tab, xc_trigger_rr_tab, xc_trigger_modulo_tab, table_stats;
DROP FUNCTION count_tuple_increment();
DROP FUNCTION count_tuple_decrement();
DROP FUNCTION count_insert_query();
DROP FUNCTION count_update_query();
DROP FUNCTION count_delete_query();
DROP FUNCTION count_truncate_query();
DROP FUNCTION count_insert_row();
DROP FUNCTION count_update_row();
DROP FUNCTION count_delete_row();

-- Tests for INSTEAD OF
-- Replace operations on a view by operations on a table
CREATE TABLE real_table (a int, b int) distribute by replication;
CREATE VIEW real_view AS SELECT a,b FROM real_table;
CREATE FUNCTION insert_real() RETURNS TRIGGER AS $_$
	BEGIN
		INSERT INTO real_table VALUES (NEW.a, NEW.b);
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION update_real() RETURNS TRIGGER AS $_$
	BEGIN
		UPDATE real_table SET a = NEW.a, b = NEW.b WHERE a = OLD.a AND b = OLD.b;
		RETURN NEW;
	END $_$ LANGUAGE 'plpgsql';
CREATE FUNCTION delete_real() RETURNS TRIGGER AS $_$
	BEGIN
		DELETE FROM real_table WHERE a = OLD.a AND b = OLD.b;
		RETURN OLD;
	END $_$ LANGUAGE 'plpgsql';
CREATE TRIGGER insert_real_trig INSTEAD OF INSERT ON real_view FOR EACH ROW EXECUTE PROCEDURE insert_real();
CREATE TRIGGER update_real_trig INSTEAD OF UPDATE ON real_view FOR EACH ROW EXECUTE PROCEDURE update_real();
CREATE TRIGGER delete_real_trig INSTEAD OF DELETE ON real_view FOR EACH ROW EXECUTE PROCEDURE delete_real();
-- Renaming of trigger based on a view
ALTER TRIGGER delete_real_trig ON real_view RENAME TO delete_real_trig2;
-- Actions
INSERT INTO real_view VALUES(1,1);
SELECT * FROM real_table;
UPDATE real_view SET b = 4 WHERE a = 1;
SELECT * FROM real_table;
DELETE FROM real_view WHERE a = 1;
SELECT * FROM real_table;
-- Clean up
DROP VIEW real_view CASCADE;
DROP FUNCTION insert_real() cascade;
DROP FUNCTION update_real() cascade;
DROP FUNCTION delete_real() cascade;
DROP TABLE real_table;


-- Test if distribution column value can be updated by a non-immutable trigger function.
CREATE TABLE xc_trig_dist(id int, id2 int, id3 int, v varchar) distribute by replication;
insert into xc_trig_dist values (NULL, 1, 1, 'ppppppppppp'), (2, 2, 2, 'qqqqqqqqqqq'), (3, 3, 3, 'rrrrrrrrrrr');
CREATE FUNCTION xc_func_dist() RETURNS trigger AS $$
declare
    BEGIN
		NEW.v = regexp_replace(NEW.v , 'pp$', 'xx');
		if NEW.id is NULL then
			NEW.id = 999;
		else
			NEW.id = NULL;
		end if;

		return NEW;
    END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER xc_dist_trigger BEFORE UPDATE ON xc_trig_dist
    FOR EACH ROW EXECUTE PROCEDURE xc_func_dist();

--This should pass because the table is replicated.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
SELECT * from xc_trig_dist order by id2;

-- Reinitialize the rows
truncate table xc_trig_dist;
insert into xc_trig_dist values (NULL, 1, 1, 'ppppppppppp'), (2, 2, 2, 'qqqqqqqqqqq'), (3, 3, 3, 'rrrrrrrrrrr');

-- Now make the table distributed
ALTER TABLE xc_trig_dist distribute by hash(v);
--This should fail because v is a distributed col and trigger updates it.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';

ALTER TABLE xc_trig_dist distribute by hash(id);
--This should fail because id is a distributed col and trigger updates it.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';

ALTER TABLE xc_trig_dist distribute by hash(id2);
--This should pass because id2 is not updated.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%qqq%';
SELECT * from xc_trig_dist order by id2;

ALTER TABLE xc_trig_dist distribute by hash(v);
--This should still pass because v does not get modified by the regexp_replace()
-- function in the trigger function.
UPDATE xc_trig_dist set id3 = id3 * 2 where v like '%pppp%';
SELECT * from xc_trig_dist order by id2;

DROP table xc_trig_dist cascade;
DROP function xc_func_dist() cascade;
