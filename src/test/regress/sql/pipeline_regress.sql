CREATE FOREIGN TABLE test_stream (
  ts TIMESTAMP (0) WITH TIME ZONE,
  foobar TEXT
) SERVER pipelinedb;

CREATE VIEW test_view AS
  SELECT
    SECOND(ts) AS secondstamp,
    foobar,
    COUNT(*) AS count
  FROM test_stream
  WHERE (arrival_timestamp > clock_timestamp() - interval '7 day')
  GROUP BY
    secondstamp,
    foobar;

INSERT INTO test_stream (foobar, ts) VALUES ('foo', clock_timestamp()), ('foo', clock_timestamp());
SELECT pg_sleep(1);
INSERT INTO test_stream (foobar, ts) VALUES ('foo', clock_timestamp()), ('bar', clock_timestamp());

SELECT foobar, count FROM test_view ORDER BY secondstamp, foobar;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream;

CREATE FOREIGN TABLE test_stream (user_id int, page_id int) SERVER pipelinedb;
CREATE VIEW test_view AS
  SELECT
    user_id::int,
    page_id::int,
    arrival_timestamp as ts
  FROM test_stream
  WHERE arrival_timestamp >= ( clock_timestamp() - interval '90 minutes' );

INSERT INTO test_stream (user_id, page_id) VALUES (1, 1);
SELECT pg_sleep(1);
INSERT INTO test_stream (user_id, page_id) VALUES (2, 2);

SELECT user_id, page_id FROM test_view ORDER BY ts;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream CASCADE;

CREATE OR REPLACE FUNCTION arrival_timestamp()
RETURNS timestamptz AS
$$
BEGIN
  RETURN timestamp '2000-01-01 00:00:00';
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FOREIGN TABLE test_stream (x int) SERVER pipelinedb;
CREATE VIEW test_view AS
  SELECT arrival_timestamp FROM test_stream
  WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';

INSERT INTO test_stream (x) VALUES (NULL);
INSERT INTO test_stream (x) VALUES (NULL);

SELECT COUNT(*) FROM test_view;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream CASCADE;
DROP FUNCTION arrival_timestamp();

CREATE FOREIGN TABLE test_stream (id int, x float8) SERVER pipelinedb;
CREATE VIEW test_view AS
  SELECT ((id::integer)::numeric + avg(id::integer)) AS x
  FROM test_stream
  GROUP BY id::integer;

INSERT INTO test_stream (id) VALUES (1), (2), (3);

SELECT * FROM test_view ORDER BY x;

DROP VIEW test_view;

CREATE VIEW test_view AS
  SELECT id::integer + avg(id) AS x, SUM(x::float8)
  FROM test_stream
  GROUP BY id;

\d+ test_view_mrel;

INSERT INTO test_stream (id, x) VALUES (1, 1.5), (2, 3.0), (3, 4.5);

SELECT * FROM test_view ORDER BY x;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream CASCADE;

CREATE FOREIGN TABLE test_stream (x int) SERVER pipelinedb;
CREATE VIEW test_view AS
SELECT
  second(arrival_timestamp),
  COUNT(*)
FROM
  test_stream
WHERE
  (arrival_timestamp > clock_timestamp() - interval '10 minute')
GROUP BY second;

INSERT INTO test_stream (x) VALUES (1), (1);
SELECT pg_sleep(1);
INSERT INTO test_stream (x) VALUES (1), (1);

SELECT count FROM test_view_mrel;
SELECT count FROM test_view;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream CASCADE;

CREATE FOREIGN TABLE test_stream (uid bigint) SERVER pipelinedb;
CREATE VIEW test_view AS
  SELECT uid::bigint, COUNT(*)
FROM
  test_stream
GROUP BY uid;

-- Ensure that hashes colide.
SELECT pipelinedb.hash_group(13362), pipelinedb.hash_group(41950);

INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (41950);
INSERT INTO test_stream (uid) VALUES (41950);
INSERT INTO test_stream (uid) VALUES (41950);
INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (13362);
INSERT INTO test_stream (uid) VALUES (41950);
INSERT INTO test_stream (uid) VALUES (41950);
INSERT INTO test_stream (uid) VALUES (41950);

SELECT * FROM test_view ORDER BY uid;

DROP VIEW test_view;
DROP FOREIGN TABLE test_stream CASCADE;

CREATE FOREIGN TABLE test_stream (x int, y int) SERVER pipelinedb;
CREATE VIEW v AS SELECT x::int, count(*) FROM test_stream;
CREATE VIEW v AS SELECT x::int, y::int, count(*) FROM test_stream GROUP BY x;
DROP FOREIGN TABLE test_stream CASCADE;

-- #1357
CREATE FUNCTION jsonb_element_bigint_agg_state(acc jsonb, elem jsonb)
RETURNS jsonb LANGUAGE SQL AS $$
SELECT json_object_agg(key,value)::jsonb
FROM (SELECT key, sum(value) as value
        FROM (SELECT key, value::bigint AS value
                FROM jsonb_each_text(acc)
                UNION ALL
                SELECT key, value::bigint AS value
                FROM jsonb_each_text(elem)) x
        GROUP BY 1) y;
$$ PARALLEL SAFE;

CREATE AGGREGATE jsonb_element_bigint_agg(jsonb)
(
    stype = jsonb,
    initcond = '{}',
    sfunc = jsonb_element_bigint_agg_state,
    combinefunc = jsonb_element_bigint_agg_state,
    parallel = safe
);

CREATE FOREIGN TABLE jsonb_test_stream (key integer, values jsonb) SERVER pipelinedb;
CREATE VIEW test_json_udf_agg AS SELECT key, jsonb_element_bigint_agg(values) FROM jsonb_test_stream GROUP BY key;

INSERT INTO jsonb_test_stream (key, values) VALUES (1, '{"a": 1}'::jsonb);
INSERT INTO jsonb_test_stream (key, values) VALUES (1, '{"a": 1}'::jsonb);
SELECT * from test_json_udf_agg;

DROP VIEW test_json_udf_agg;
DROP AGGREGATE jsonb_element_bigint_agg(jsonb);
DROP FUNCTION jsonb_element_bigint_agg_state(acc jsonb, elem jsonb);
DROP FOREIGN TABLE jsonb_test_stream;

CREATE FOREIGN TABLE sw_ts_expr_s (x int) SERVER pipelinedb;

CREATE VIEW sw_ts_expr1 AS
  SELECT count(*) FROM sw_ts_expr_s
  WHERE minute(arrival_timestamp) + interval '1 second' > clock_timestamp() - interval '5 minute';

CREATE VIEW sw_ts_expr2 AS
  SELECT minute(arrival_timestamp), count(*) FROM sw_ts_expr_s
  WHERE minute(arrival_timestamp) > clock_timestamp() - interval '5 minute'
  GROUP BY minute(arrival_timestamp);

SELECT pg_get_viewdef('sw_ts_expr1');
SELECT pg_get_viewdef('sw_ts_expr2');

INSERT INTO sw_ts_expr_s (x) VALUES (1), (1);
INSERT INTO sw_ts_expr_s (x) VALUES (1), (1);

SELECT * FROM sw_ts_expr1;
SELECT count FROM sw_ts_expr2;

CREATE VIEW unknown_type_cv AS SELECT x, 'a' FROM sw_ts_expr_s;

SELECT pg_get_viewdef('unknown_type_cv');

DROP VIEW unknown_type_cv;

CREATE FOREIGN TABLE ct_out_s (x integer, a text) SERVER pipelinedb;
CREATE VIEW unknown_type_ct1 WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_out_s')) AS
  SELECT x, 'a' FROM sw_ts_expr_s;

CREATE VIEW unknown_type_ct2 WITH (action=transform, outputfunc=pipelinedb.insert_into_stream('ct_out_s')) AS
  SELECT x, 'a'::text FROM sw_ts_expr_s;

SELECT pg_get_viewdef('unknown_type_ct1');
SELECT pg_get_viewdef('unknown_type_ct2');

DROP VIEW unknown_type_ct1;
DROP VIEW unknown_type_ct2;
DROP FOREIGN TABLE ct_out_s;

DROP FOREIGN TABLE sw_ts_expr_s CASCADE;

-- WINDOW aggregates are not currently supported
CREATE FOREIGN TABLE window_s (x integer) SERVER pipelinedb;

CREATE VIEW not_happenin AS
 SELECT first_value(x) OVER w
FROM window_s WINDOW w AS (ORDER BY x);

DROP FOREIGN TABLE window_s;

-- Set operations can be used within VIEWs created over CVs, but not in CV definitions themselves
CREATE FOREIGN TABLE set_s (x integer) SERVER pipelinedb;

CREATE VIEW set0 AS
 SELECT x FROM set_s UNION ALL SELECT x FROM set_s;

CREATE VIEW set0 AS
 SELECT x, count(*) FROM set_s GROUP BY x;

CREATE VIEW set1 AS
 SELECT x, count(*) FROM set_s GROUP BY x;

CREATE VIEW set2 AS
 SELECT x, count FROM set0 UNION ALL SELECT x, count FROM set1;

INSERT INTO set_s (x) SELECT generate_series(1, 5) x;

SELECT x, count FROM set2 ORDER BY x, count;

DROP FOREIGN TABLE set_s CASCADE;
