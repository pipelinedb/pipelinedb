-- predictability
SET synchronous_commit = on;

-- fail because we're creating a slot while in an xact with xid
BEGIN;
SELECT txid_current() = 0;
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
ROLLBACK;

-- fail because we're creating a slot while in an subxact whose topxact has a xid
BEGIN;
SELECT txid_current() = 0;
SAVEPOINT barf;
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
ROLLBACK TO SAVEPOINT barf;
ROLLBACK;

-- succeed, outside tx.
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');

-- succeed, in tx without xid.
BEGIN;
SELECT 'init' FROM pg_create_logical_replication_slot('regression_slot', 'test_decoding');
COMMIT;

CREATE TABLE nobarf(id serial primary key, data text);
INSERT INTO nobarf(data) VALUES('1');

-- decoding works in transaction with xid
BEGIN;
SELECT txid_current() = 0;
-- don't show yet, haven't committed
INSERT INTO nobarf(data) VALUES('2');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');
COMMIT;

INSERT INTO nobarf(data) VALUES('3');
SELECT data FROM pg_logical_slot_get_changes('regression_slot', NULL, NULL, 'include-xids', '0');

SELECT 'stop' FROM pg_drop_replication_slot('regression_slot');
