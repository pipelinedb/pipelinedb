--
-- Test for various ALTER statements
--

-- clean-up in case a prior regression run failed
SET client_min_messages TO 'warning';
DROP DATABASE IF EXISTS regtest_sepgsql_test_database_1;
DROP DATABASE IF EXISTS regtest_sepgsql_test_database;
DROP USER IF EXISTS regtest_sepgsql_test_user;
RESET client_min_messages;

-- @SECURITY-CONTEXT=unconfined_u:unconfined_r:unconfined_t:s0

--
-- CREATE Objects to be altered (with debug_audit being silent)
--
CREATE DATABASE regtest_sepgsql_test_database_1;

CREATE USER regtest_sepgsql_test_user;

CREATE SCHEMA regtest_schema_1;
CREATE SCHEMA regtest_schema_2;

GRANT ALL ON SCHEMA regtest_schema_1 TO public;
GRANT ALL ON SCHEMA regtest_schema_2  TO public;

SET search_path = regtest_schema_1, regtest_schema_2, public;

CREATE TABLE regtest_table_1 (a int, b text);

CREATE TABLE regtest_table_2 (c text) inherits (regtest_table_1);

CREATE TABLE regtest_table_3 (x int primary key, y text);

CREATE SEQUENCE regtest_seq_1;

CREATE VIEW regtest_view_1 AS SELECT * FROM regtest_table_1 WHERE a > 0;

CREATE FUNCTION regtest_func_1 (text) RETURNS bool
  AS 'BEGIN RETURN true; END' LANGUAGE 'plpgsql';

-- switch on debug_audit
SET sepgsql.debug_audit = true;
SET client_min_messages = LOG;

--
-- ALTER xxx OWNER TO
--
-- XXX: It should take db_xxx:{setattr} permission checks even if
--      owner is not actually changed.
--
ALTER DATABASE regtest_sepgsql_test_database_1 OWNER TO regtest_sepgsql_test_user;
ALTER DATABASE regtest_sepgsql_test_database_1 OWNER TO regtest_sepgsql_test_user;
ALTER SCHEMA regtest_schema_1 OWNER TO regtest_sepgsql_test_user;
ALTER SCHEMA regtest_schema_1 OWNER TO regtest_sepgsql_test_user;
ALTER TABLE regtest_table_1 OWNER TO regtest_sepgsql_test_user;
ALTER TABLE regtest_table_1 OWNER TO regtest_sepgsql_test_user;
ALTER SEQUENCE regtest_seq_1 OWNER TO regtest_sepgsql_test_user;
ALTER SEQUENCE regtest_seq_1 OWNER TO regtest_sepgsql_test_user;
ALTER VIEW regtest_view_1 OWNER TO regtest_sepgsql_test_user;
ALTER VIEW regtest_view_1 OWNER TO regtest_sepgsql_test_user;
ALTER FUNCTION regtest_func_1(text) OWNER TO regtest_sepgsql_test_user;
ALTER FUNCTION regtest_func_1(text) OWNER TO regtest_sepgsql_test_user;

--
-- ALTER xxx SET SCHEMA
--
ALTER TABLE regtest_table_1 SET SCHEMA regtest_schema_2;
ALTER SEQUENCE regtest_seq_1 SET SCHEMA regtest_schema_2;
ALTER VIEW regtest_view_1 SET SCHEMA regtest_schema_2;
ALTER FUNCTION regtest_func_1(text) SET SCHEMA regtest_schema_2;

--
-- ALTER xxx RENAME TO
--
ALTER DATABASE regtest_sepgsql_test_database_1 RENAME TO regtest_sepgsql_test_database;
ALTER SCHEMA regtest_schema_1 RENAME TO regtest_schema;
ALTER TABLE regtest_table_1 RENAME TO regtest_table;
ALTER SEQUENCE regtest_seq_1 RENAME TO regtest_seq;
ALTER VIEW regtest_view_1 RENAME TO regtest_view;
ALTER FUNCTION regtest_func_1(text) RENAME TO regtest_func;

SET search_path = regtest_schema, regtest_schema_2, public;

--
-- misc ALTER commands
--
ALTER DATABASE regtest_sepgsql_test_database CONNECTION LIMIT 999;
ALTER DATABASE regtest_sepgsql_test_database SET search_path TO regtest_schema, public; -- not supported yet

ALTER TABLE regtest_table ADD COLUMN d float;
ALTER TABLE regtest_table DROP COLUMN d;
ALTER TABLE regtest_table ALTER b SET DEFAULT 'abcd';   -- not supported yet
ALTER TABLE regtest_table ALTER b SET DEFAULT 'XYZ';    -- not supported yet
ALTER TABLE regtest_table ALTER b DROP DEFAULT;         -- not supported yet
ALTER TABLE regtest_table ALTER b SET NOT NULL;
ALTER TABLE regtest_table ALTER b DROP NOT NULL;
ALTER TABLE regtest_table ALTER b SET STATISTICS -1;
ALTER TABLE regtest_table ALTER b SET (n_distinct = 999);
ALTER TABLE regtest_table ALTER b SET STORAGE PLAIN;
ALTER TABLE regtest_table ADD CONSTRAINT test_fk FOREIGN KEY (a) REFERENCES regtest_table_3(x); -- not supported
ALTER TABLE regtest_table ADD CONSTRAINT test_ck CHECK (b like '%abc%') NOT VALID;      -- not supported
ALTER TABLE regtest_table VALIDATE CONSTRAINT test_ck;  -- not supported
ALTER TABLE regtest_table DROP CONSTRAINT test_ck;      -- not supported

CREATE TRIGGER regtest_test_trig BEFORE UPDATE ON regtest_table
    FOR EACH ROW EXECUTE PROCEDURE suppress_redundant_updates_trigger();

ALTER TABLE regtest_table DISABLE TRIGGER regtest_test_trig;    -- not supported
ALTER TABLE regtest_table ENABLE  TRIGGER regtest_test_trig;    -- not supported

CREATE RULE regtest_test_rule AS ON INSERT TO regtest_table_3 DO ALSO NOTHING;
ALTER TABLE regtest_table_3 DISABLE RULE regtest_test_rule;     -- not supported
ALTER TABLE regtest_table_3 ENABLE RULE regtest_test_rule;      -- not supported

ALTER TABLE regtest_table SET WITH OIDS;
ALTER TABLE regtest_table SET WITHOUT OIDS;
ALTER TABLE regtest_table SET (fillfactor = 75);
ALTER TABLE regtest_table RESET (fillfactor);
ALTER TABLE regtest_table_2 NO INHERIT regtest_table;   -- not supported
ALTER TABLE regtest_table_2 INHERIT regtest_table;      -- not supported
ALTER TABLE regtest_table SET TABLESPACE pg_default;

ALTER VIEW regtest_view SET (security_barrier);

ALTER SEQUENCE regtest_seq INCREMENT BY 10 START WITH 1000;

--
-- clean-up objects
--
RESET sepgsql.debug_audit;
RESET client_min_messages;
DROP DATABASE regtest_sepgsql_test_database;
DROP SCHEMA regtest_schema CASCADE;
DROP SCHEMA regtest_schema_2 CASCADE;
DROP USER regtest_sepgsql_test_user;
