CREATE CONTINUOUS VIEW test_cont_alter AS SELECT x::int FROM stream;

ALTER VIEW test_cont_alter RENAME TO lolcat;
ALTER TABLE test_cont_alter_mrel RENAME TO lolcat;
ALTER VIEW test_cont_alter ALTER COLUMN x SET DEFAULT 10;
ALTER TABLE test_cont_alter_mrel DROP COLUMN x;

DROP CONTINUOUS VIEW test_cont_alter;
