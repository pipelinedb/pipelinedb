CREATE STREAM test_stream (
  ts TIMESTAMP (0) WITH TIME ZONE,
  foobar TEXT
);

CREATE CONTINUOUS VIEW test_view AS
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

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream;

CREATE STREAM test_stream (user_id int, page_id int);
CREATE CONTINUOUS VIEW test_view AS
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

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream CASCADE;

CREATE OR REPLACE FUNCTION arrival_timestamp()
RETURNS timestamptz AS
$$
BEGIN
  RETURN timestamp '2000-01-01 00:00:00';
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE STREAM test_stream (x int);
CREATE CONTINUOUS VIEW test_view AS
  SELECT arrival_timestamp FROM test_stream
  WHERE arrival_timestamp > clock_timestamp() - interval '1 hour';

INSERT INTO test_stream (x) VALUES (NULL);
INSERT INTO test_stream (x) VALUES (NULL);

SELECT COUNT(*) FROM test_view;

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream CASCADE;
DROP FUNCTION arrival_timestamp();

CREATE STREAM test_stream (id int, x float8);
CREATE CONTINUOUS VIEW test_view AS
  SELECT ((id::integer)::numeric + avg(id::integer)) AS x
  FROM test_stream
  GROUP BY id::integer;

INSERT INTO test_stream (id) VALUES (1), (2), (3);

SELECT * FROM test_view ORDER BY x;

DROP CONTINUOUS VIEW test_view;

CREATE CONTINUOUS VIEW test_view AS
  SELECT id::integer + avg(id) AS x, SUM(x::float8)
  FROM test_stream
  GROUP BY id;

\d+ test_view_mrel;

INSERT INTO test_stream (id, x) VALUES (1, 1.5), (2, 3.0), (3, 4.5);

SELECT * FROM test_view ORDER BY x;

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream CASCADE;

CREATE STREAM test_stream (x int);
CREATE CONTINUOUS VIEW test_view AS
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

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream CASCADE;

CREATE STREAM test_stream (uid bigint);
CREATE CONTINUOUS VIEW test_view AS
  SELECT uid::bigint, COUNT(*)
FROM
  test_stream
GROUP BY uid;

-- Ensure that hashes colide.
SELECT hash_group(13362), hash_group(41950);

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

DROP CONTINUOUS VIEW test_view;
DROP STREAM test_stream CASCADE;

CREATE STREAM test_stream (x int, y int);
CREATE CONTINUOUS VIEW v AS SELECT x::int, count(*) FROM test_stream;
CREATE CONTINUOUS VIEW v AS SELECT x::int, y::int, count(*) FROM test_stream GROUP BY x;
DROP STREAM test_stream CASCADE;

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
$$;

CREATE AGGREGATE jsonb_element_bigint_agg(jsonb)
(
    stype = jsonb,
    initcond = '{}',
    sfunc = jsonb_element_bigint_agg_state
);

CREATE STREAM jsonb_test_stream (key integer, values jsonb);
CREATE CONTINUOUS VIEW test_json_udf_agg AS SELECT key, jsonb_element_bigint_agg(values) FROM jsonb_test_stream GROUP BY key;

INSERT INTO jsonb_test_stream (key, values) VALUES (1, '{"a": 1}'::jsonb);
INSERT INTO jsonb_test_stream (key, values) VALUES (1, '{"a": 1}'::jsonb);
SELECT * from test_json_udf_agg;

DROP CONTINUOUS VIEW test_json_udf_agg;
DROP AGGREGATE jsonb_element_bigint_agg(jsonb);
DROP FUNCTION jsonb_element_bigint_agg_state(acc jsonb, elem jsonb);
DROP STREAM jsonb_test_stream;

CREATE STREAM sw_ts_expr_s (x int);

CREATE CONTINUOUS VIEW sw_ts_expr1 AS
  SELECT count(*) FROM sw_ts_expr_s
  WHERE minute(arrival_timestamp) + interval '1 second' > clock_timestamp() - interval '5 minute';

CREATE CONTINUOUS VIEW sw_ts_expr2 AS
  SELECT minute(arrival_timestamp), count(*) FROM sw_ts_expr_s
  WHERE minute(arrival_timestamp) > clock_timestamp() - interval '5 minute'
  GROUP BY minute(arrival_timestamp);

\d+ sw_ts_expr1
\d+ sw_ts_expr2

INSERT INTO sw_ts_expr_s (x) VALUES (1), (1);
INSERT INTO sw_ts_expr_s (x) VALUES (1), (1);

SELECT * FROM sw_ts_expr1;
SELECT count FROM sw_ts_expr2;

CREATE CONTINUOUS VIEW unknown_type_cv AS SELECT x, 'a' FROM sw_ts_expr_s;
CREATE CONTINUOUS VIEW unknown_type_cv AS SELECT x, 'a'::text FROM sw_ts_expr_s;

\d+ unknown_type_cv

DROP CONTINUOUS VIEW unknown_type_cv;

CREATE STREAM ct_out_s (x integer, a text);
CREATE VIEW unknown_type_ct WITH (action=transform, outputfunc=pipeline_stream_insert('ct_out_s')) AS
  SELECT x, 'a' FROM sw_ts_expr_s;

CREATE VIEW unknown_type_ct WITH (action=transform, outputfunc=pipeline_stream_insert('ct_out_s')) AS
  SELECT x, 'a'::text FROM sw_ts_expr_s;

SELECT pg_get_viewdef('unknown_type_ct');

DROP VIEW unknown_type_ct;
DROP STREAM ct_out_s;


DROP STREAM sw_ts_expr_s CASCADE;

