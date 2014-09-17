-- predictability
SET synchronous_commit = on;

SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
-- fail because of an already existing slot
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
-- fail because of an invalid name
SELECT 'init' FROM pg_create_logical_replication_slot('Invalid Name', 'test_decoding');

-- fail twice because of an invalid parameter values
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', 'frakbar');
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'nonexistant-option', 'frakbar');
SELECT 'init' FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', 'frakbar');

-- succeed once
SELECT pg_drop_replication_slot('regression_slot');
-- fail
SELECT pg_drop_replication_slot('regression_slot');

-- check that we're detecting a streaming rep slot used for logical decoding
SELECT 'init' FROM pg_create_physical_replication_slot('repl');
SELECT data FROM pg_logical_slot_get_changes('repl', NULL, NULL, 'include-xids', '0');
SELECT pg_drop_replication_slot('repl');


SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');

/* check whether status function reports us, only reproduceable columns */
SELECT slot_name, plugin, slot_type, active,
    NOT catalog_xmin IS NULL AS catalog_xmin_set,
    xmin IS NULl  AS data_xmin_not_set,
    pg_xlog_location_diff(restart_lsn, '0/01000000') > 0 AS some_wal
FROM pg_replication_slots;

/*
 * Check that changes are handled correctly when interleaved with ddl
 */
CREATE TABLE replication_example(id SERIAL PRIMARY KEY, somedata int, text varchar(120));
BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (1, 1);
INSERT INTO replication_example(somedata, text) VALUES (1, 2);
COMMIT;

ALTER TABLE replication_example ADD COLUMN bar int;

INSERT INTO replication_example(somedata, text, bar) VALUES (2, 1, 4);

BEGIN;
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 2, 4);
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 3, 4);
INSERT INTO replication_example(somedata, text, bar) VALUES (2, 4, NULL);
COMMIT;

ALTER TABLE replication_example DROP COLUMN bar;
INSERT INTO replication_example(somedata, text) VALUES (3, 1);

BEGIN;
INSERT INTO replication_example(somedata, text) VALUES (3, 2);
INSERT INTO replication_example(somedata, text) VALUES (3, 3);
COMMIT;

ALTER TABLE replication_example RENAME COLUMN text TO somenum;

INSERT INTO replication_example(somedata, somenum) VALUES (4, 1);

-- collect all changes
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

ALTER TABLE replication_example ALTER COLUMN somenum TYPE int4 USING (somenum::int4);
-- throw away changes, they contain oids
SELECT count(data) FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

INSERT INTO replication_example(somedata, somenum) VALUES (5, 1);

BEGIN;
INSERT INTO replication_example(somedata, somenum) VALUES (6, 1);
ALTER TABLE replication_example ADD COLUMN zaphod1 int;
INSERT INTO replication_example(somedata, somenum, zaphod1) VALUES (6, 2, 1);
ALTER TABLE replication_example ADD COLUMN zaphod2 int;
INSERT INTO replication_example(somedata, somenum, zaphod2) VALUES (6, 3, 1);
INSERT INTO replication_example(somedata, somenum, zaphod1) VALUES (6, 4, 2);
COMMIT;

-- show changes
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

-- hide changes bc of oid visible in full table rewrites
CREATE TABLE tr_unique(id2 serial unique NOT NULL, data int);
INSERT INTO tr_unique(data) VALUES(10);
ALTER TABLE tr_unique RENAME TO tr_pkey;
ALTER TABLE tr_pkey ADD COLUMN id serial primary key;
SELECT count(data) FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

INSERT INTO tr_pkey(data) VALUES(1);
--show deletion with primary key
DELETE FROM tr_pkey;

/* display results */
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

/*
 * check that disk spooling works
 */
BEGIN;
CREATE TABLE tr_etoomuch (id serial primary key, data int);
INSERT INTO tr_etoomuch(data) SELECT g.i FROM generate_series(1, 10234) g(i);
DELETE FROM tr_etoomuch WHERE id < 5000;
UPDATE tr_etoomuch SET data = - data WHERE id > 5000;
COMMIT;

/* display results, but hide most of the output */
SELECT count(*), min(data), max(data)
FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0')
GROUP BY substring(data, 1, 24)
ORDER BY 1,2;

/*
 * check whether we decode subtransactions correctly in relation with each
 * other
 */
CREATE TABLE tr_sub (id serial primary key, path text);

-- toplevel, subtxn, toplevel, subtxn, subtxn
BEGIN;
INSERT INTO tr_sub(path) VALUES ('1-top-#1');

SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('1-top-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-1-#2');
RELEASE SAVEPOINT a;

SAVEPOINT b;
SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#1');
INSERT INTO tr_sub(path) VALUES ('1-top-2-1-#2');
RELEASE SAVEPOINT c;
INSERT INTO tr_sub(path) VALUES ('1-top-2-#1');
RELEASE SAVEPOINT b;
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

-- check that we handle xlog assignments correctly
BEGIN;
-- nest 80 subtxns
SAVEPOINT subtop;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;SAVEPOINT a;
-- assign xid by inserting
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#1');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#2');
INSERT INTO tr_sub(path) VALUES ('2-top-1...--#3');
RELEASE SAVEPOINT subtop;
INSERT INTO tr_sub(path) VALUES ('2-top-#1');
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

-- make sure rollbacked subtransactions aren't decoded
BEGIN;
INSERT INTO tr_sub(path) VALUES ('3-top-2-#1');
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('3-top-2-1-#1');
SAVEPOINT b;
INSERT INTO tr_sub(path) VALUES ('3-top-2-2-#1');
ROLLBACK TO SAVEPOINT b;
INSERT INTO tr_sub(path) VALUES ('3-top-2-#2');
COMMIT;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

-- test whether a known, but not yet logged toplevel xact, followed by a
-- subxact commit is handled correctly
BEGIN;
SELECT txid_current() != 0; -- so no fixed xid apears in the outfile
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('4-top-1-#1');
RELEASE SAVEPOINT a;
COMMIT;

-- test whether a change in a subtransaction, in an unknown toplevel
-- xact is handled correctly.
BEGIN;
SAVEPOINT a;
INSERT INTO tr_sub(path) VALUES ('5-top-1-#1');
COMMIT;


SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');


/*
 * Check whether treating a table as a catalog table works somewhat
 */
CREATE TABLE replication_metadata (
    id serial primary key,
    relation name NOT NULL,
    options text[]
)
WITH (user_catalog_table = true)
;
\d+ replication_metadata

INSERT INTO replication_metadata(relation, options)
VALUES ('foo', ARRAY['a', 'b']);

ALTER TABLE replication_metadata RESET (user_catalog_table);
\d+ replication_metadata

INSERT INTO replication_metadata(relation, options)
VALUES ('bar', ARRAY['a', 'b']);

ALTER TABLE replication_metadata SET (user_catalog_table = true);
\d+ replication_metadata

INSERT INTO replication_metadata(relation, options)
VALUES ('blub', NULL);

-- make sure rewrites don't work
ALTER TABLE replication_metadata ADD COLUMN rewritemeornot int;
ALTER TABLE replication_metadata ALTER COLUMN rewritemeornot TYPE text;

ALTER TABLE replication_metadata SET (user_catalog_table = false);
\d+ replication_metadata

INSERT INTO replication_metadata(relation, options)
VALUES ('zaphod', NULL);

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

/*
 * check whether we handle updates/deletes correct with & without a pkey
 */

/* we should handle the case without a key at all more gracefully */
CREATE TABLE table_without_key(id serial, data int);
INSERT INTO table_without_key(data) VALUES(1),(2);
DELETE FROM table_without_key WHERE data = 1;
-- won't log old keys
UPDATE table_without_key SET data = 3 WHERE data = 2;
UPDATE table_without_key SET id = -id;
UPDATE table_without_key SET id = -id;
-- should log the full old row now
ALTER TABLE table_without_key REPLICA IDENTITY FULL;
UPDATE table_without_key SET data = 3 WHERE data = 2;
UPDATE table_without_key SET id = -id;
UPDATE table_without_key SET id = -id;
DELETE FROM table_without_key WHERE data = 3;

CREATE TABLE table_with_pkey(id serial primary key, data int);
INSERT INTO table_with_pkey(data) VALUES(1), (2);
DELETE FROM table_with_pkey WHERE data = 1;
-- should log the old pkey
UPDATE table_with_pkey SET data = 3 WHERE data = 2;
UPDATE table_with_pkey SET id = -id;
UPDATE table_with_pkey SET id = -id;
-- check that we log nothing despite having a pkey
ALTER TABLE table_without_key REPLICA IDENTITY NOTHING;
UPDATE table_with_pkey SET id = -id;
-- check that we log everything despite having a pkey
ALTER TABLE table_without_key REPLICA IDENTITY FULL;
UPDATE table_with_pkey SET id = -id;
DELETE FROM table_with_pkey WHERE data = 3;

CREATE TABLE table_with_unique_not_null(id serial unique, data int);
ALTER TABLE table_with_unique_not_null ALTER COLUMN id SET NOT NULL; --already set
-- won't log anything, replica identity not setup
INSERT INTO table_with_unique_not_null(data) VALUES(1), (2);
DELETE FROM table_with_unique_not_null WHERE data = 1;
UPDATE table_with_unique_not_null SET data = 3 WHERE data = 2;
UPDATE table_with_unique_not_null SET id = -id;
UPDATE table_with_unique_not_null SET id = -id;
DELETE FROM table_with_unique_not_null WHERE data = 3;
-- should log old key
ALTER TABLE table_with_unique_not_null REPLICA IDENTITY USING INDEX table_with_unique_not_null_id_key;
INSERT INTO table_with_unique_not_null(data) VALUES(1), (2);
DELETE FROM table_with_unique_not_null WHERE data = 1;
UPDATE table_with_unique_not_null SET data = 3 WHERE data = 2;
UPDATE table_with_unique_not_null SET id = -id;
UPDATE table_with_unique_not_null SET id = -id;
DELETE FROM table_with_unique_not_null WHERE data = 3;

-- check toast support
BEGIN;
CREATE SEQUENCE toasttable_rand_seq START 79 INCREMENT 1499; -- portable "random"
CREATE TABLE toasttable(
       id serial primary key,
       toasted_col1 text,
       rand1 float8 DEFAULT nextval('toasttable_rand_seq'),
       toasted_col2 text,
       rand2 float8 DEFAULT nextval('toasttable_rand_seq')
       );
COMMIT;
-- uncompressed external toast data
INSERT INTO toasttable(toasted_col1) SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i);

-- compressed external toast data
INSERT INTO toasttable(toasted_col2) SELECT repeat(string_agg(to_char(g.i, 'FM0000'), ''), 50) FROM generate_series(1, 500) g(i);

-- update of existing column
UPDATE toasttable
    SET toasted_col1 = (SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i))
WHERE id = 1;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

INSERT INTO toasttable(toasted_col1) SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i);

-- update of second column, first column unchanged
UPDATE toasttable
    SET toasted_col2 = (SELECT string_agg(g.i::text, '') FROM generate_series(1, 2000) g(i))
WHERE id = 1;

-- make sure we decode correctly even if the toast table is gone
DROP TABLE toasttable;

SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

-- done, free logical replication slot
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

SELECT pg_drop_replication_slot('regression_slot');

/* check that the slot is gone */
SELECT * FROM pg_replication_slots;
