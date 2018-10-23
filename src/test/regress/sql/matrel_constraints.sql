CREATE FOREIGN TABLE mc_s0 (x integer, y integer) SERVER pipelinedb;

CREATE VIEW mc_v0 AS SELECT x, sum(y), count(*) FROM mc_s0 GROUP BY x;

ALTER TABLE mc_v0_mrel ADD CONSTRAINT chk0 CHECK (x > 5);
ALTER TABLE mc_v0_mrel ADD CONSTRAINT chk1 CHECK (count < 3);
ALTER TABLE mc_v0_mrel ADD CONSTRAINT chk2 CHECK (sum < 3);

INSERT INTO mc_s0 (x, y) SELECT x, 1 AS y FROM generate_series(1, 10) AS x;

SELECT * FROM mc_v0 ORDER BY x;

INSERT INTO mc_s0 (x, y) SELECT x, 1 AS y FROM generate_series(1, 10) AS x;
INSERT INTO mc_s0 (x, y) SELECT x, 1 AS y FROM generate_series(1, 10) AS x;
INSERT INTO mc_s0 (x, y) SELECT x, 1 AS y FROM generate_series(1, 10) AS x;

SELECT * FROM mc_v0 ORDER BY x;

DROP FOREIGN TABLE mc_s0 CASCADE; 