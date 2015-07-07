
SELECT EnableLongTransactions();

CREATE TABLE test_locks (id numeric, state varchar);
INSERT INTO test_locks(id) VALUES (1);
INSERT INTO test_locks(id) VALUES (2);
INSERT INTO test_locks(id) VALUES (3);

-- Enable locks checking on the table
SELECT CheckAuth('test_locks', 'id');

-- this has no lock
UPDATE test_locks SET state = 'nolocks';

-- place the lock
SELECT LockRow('test_locks', '1', 'auth1', now()::timestamp+'00:01');
SELECT LockRow('test_locks', '2', 'auth2', now()::timestamp+'00:01');

-- this should fail due to missing auth
UPDATE test_locks SET state = 'unauthorized' where id = 1;

BEGIN;

	-- Add authorization for row 1
	SELECT AddAuth('auth1');

	-- we're authorized for row 1
	UPDATE test_locks SET state = 'authorized' where id = 1;
END;

-- Out of transaction we're no more authorized for row 1
UPDATE test_locks SET state = 'unauthorized' where id = 1;

BEGIN;

	-- Add authorization for row 2
	SELECT AddAuth('auth2');

	-- we're authorized for row 2
	UPDATE test_locks SET state = 'authorized' where id = 2;
END;


BEGIN;

	-- Add authorization for row 2
	SELECT AddAuth('auth2');

	-- we're *not* authorized for row 1
	UPDATE test_locks SET state = 'unauthorized' where id = 1;
END;

UPDATE test_locks SET state = 'unauthorized' where id = 2;
UPDATE test_locks SET state = 'unauthorized' where id = 1;

SELECT * from test_locks;

DROP TABLE test_locks;

SELECT DisableLongTransactions();

