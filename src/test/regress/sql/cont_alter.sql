CREATE FOREIGN TABLE cont_alter_stream (x int) SERVER pipelinedb;

CREATE VIEW test_cont_alter AS SELECT x::int FROM cont_alter_stream;

ALTER VIEW test_cont_alter RENAME TO lolcat;
ALTER TABLE test_cont_alter_mrel RENAME TO lolcat;
ALTER VIEW test_cont_alter ALTER COLUMN x SET DEFAULT 10;

DROP FOREIGN TABLE cont_alter_stream CASCADE;
