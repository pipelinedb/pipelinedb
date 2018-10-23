CREATE FOREIGN TABLE ct_stream0 (x int) SERVER pipelinedb;
CREATE FOREIGN TABLE ct_stream1 (x int) SERVER pipelinedb;

CREATE VIEW ct0 AS SELECT x::int, count(*) FROM ct_stream0 GROUP BY x;
CREATE VIEW ct1 WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_stream0')) AS SELECT x::int % 4 AS x FROM ct_stream1 WHERE x > 10 AND x < 50;

CREATE TABLE ct2 (x int);
CREATE OR REPLACE FUNCTION ct_tg()
RETURNS trigger AS
$$
BEGIN
 INSERT INTO ct2 (x) VALUES (NEW.x);
 RETURN NEW;
END;
$$
LANGUAGE plpgsql;
CREATE VIEW ct3 WITH (action=transform, outputfunc=ct_tg) AS SELECT x::int FROM ct_stream1 WHERE x % 2 = 0;

INSERT INTO ct_stream1 (x) SELECT generate_series(0, 100) AS x;

SELECT * FROM ct0 ORDER BY x;
SELECT * FROM ct2 ORDER BY x;

DROP FUNCTION ct_tg() CASCADE;
DROP TABLE ct2;
DROP VIEW ct1;
DROP VIEW ct0;
DROP FOREIGN TABLE ct_stream0;
DROP FOREIGN TABLE ct_stream1;

-- Stream-table JOIN
CREATE TABLE ct_t (x integer, s text);
INSERT INTO ct_t (x, s) VALUES (0, 'zero');
INSERT INTO ct_t (x, s) VALUES (1, 'one');

CREATE FOREIGN TABLE ct_s0 (x text) SERVER pipelinedb;
CREATE FOREIGN TABLE ct_s1 (x int) SERVER pipelinedb;
CREATE VIEW ct_v AS SELECT x::text FROM ct_s0;
CREATE VIEW ct WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_s0')) AS
  SELECT t.s AS x FROM ct_s1 s JOIN ct_t t on t.x = s.x::integer;

INSERT INTO ct_s1 (x) VALUES (0), (1);
INSERT INTO ct_s1 (x) VALUES (0), (2);

SELECT * FROM ct_v ORDER BY x;

DROP VIEW ct;
DROP VIEW ct_v;
DROP TABLE ct_t;
DROP FOREIGN TABLE ct_s1;
DROP FOREIGN TABLE ct_s0;

CREATE FOREIGN TABLE ct_s (x int, y text) SERVER pipelinedb;

CREATE VIEW ct_invalid WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_s')) AS SELECT y, x FROM ct_s;
CREATE VIEW ct_invalid WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_s')) AS SELECT x, x AS y FROM ct_s;
CREATE VIEW ct_valid WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_s')) AS SELECT x, 'a'::text FROM ct_s;

DROP FOREIGN TABLE ct_s CASCADE;

CREATE FOREIGN TABLE ct_s0 (x int) SERVER pipelinedb;
CREATE FOREIGN TABLE ct_s1 (x int) SERVER pipelinedb;

CREATE VIEW ct_t WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_s1')) AS SELECT x % 4 AS x FROM ct_s0;
CREATE VIEW ct_v0 AS SELECT x FROM ct_s1;
CREATE VIEW ct_v1 AS SELECT x FROM output_of('ct_t');

INSERT INTO ct_s0 SELECT generate_series(1, 10) x;

SELECT * FROM ct_v0;
SELECT * FROM ct_v1;

CREATE VIEW ct_ostream WITH (action=transform) AS SELECT 1 AS a, 2 AS b, 3 AS c, x + 42 AS d FROM ct_s1;

DROP FOREIGN TABLE ct_s0 CASCADE;
DROP FOREIGN TABLE ct_s1 CASCADE;

CREATE FOREIGN TABLE fanout (x integer) SERVER pipelinedb;

CREATE VIEW fanout0 WITH (action=transform) AS SELECT generate_series(1, 2) FROM fanout;
CREATE VIEW fanout1 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout0');
CREATE VIEW fanout2 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout1');
CREATE VIEW fanout3 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout2');
CREATE VIEW fanout4 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout3');
CREATE VIEW fanout5 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout4');
CREATE VIEW fanout6 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout5');
CREATE VIEW fanout7 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout6');
CREATE VIEW fanout8 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout7');
CREATE VIEW fanout9 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout8');
CREATE VIEW fanout10 WITH (action=transform) AS SELECT generate_series(1, 2) FROM output_of('fanout9');

CREATE VIEW fanout11 AS SELECT count(*) FROM output_of('fanout10');

INSERT INTO fanout (x) VALUES (0);

SELECT * FROM fanout11;

INSERT INTO fanout (x) VALUES (0);

SELECT * FROM fanout11;

INSERT INTO fanout (x) VALUES (0);

SELECT * FROM fanout11;

DROP FOREIGN TABLE fanout CASCADE;

CREATE FOREIGN TABLE ct_a (n int) SERVER pipelinedb;
CREATE FOREIGN TABLE ct_b (n int) SERVER pipelinedb;

CREATE VIEW ct_stream_insert0 AS SELECT n FROM ct_b;

CREATE SCHEMA test_cont_transform;

CREATE FUNCTION test_cont_transform.insert_into_b () RETURNS TRIGGER AS
$$
BEGIN
  INSERT INTO ct_b VALUES (NEW.n);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE VIEW ct_stream_insert1 WITH (action=transform, outputfunc=test_cont_transform.insert_into_b) AS
SELECT n FROM ct_a;

INSERT INTO ct_a SELECT generate_series(1, 100);
INSERT INTO ct_a SELECT generate_series(1, 100);

SELECT pg_sleep(1);
SELECT count(*) FROM ct_stream_insert0;

DROP FUNCTION test_cont_transform.insert_into_b() CASCADE;
DROP SCHEMA test_cont_transform;
DROP FOREIGN TABLE ct_b CASCADE;
DROP FOREIGN TABLE ct_a;
