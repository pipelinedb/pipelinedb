-- array_sort function
DROP FUNCTION IF EXISTS array_sort(anyarray);
CREATE FUNCTION
  array_sort(
      array_vals_to_sort anyarray
  )
  RETURNS TABLE (
    sorted_array anyarray
  )
  AS $BODY$
    BEGIN
      RETURN QUERY SELECT
        ARRAY_AGG(val) AS sorted_array
      FROM
        (
          SELECT
            UNNEST(array_vals_to_sort) AS val
          ORDER BY
            val
        ) AS sorted_vals
      ;
    END;
  $BODY$
LANGUAGE plpgsql;

-- json_to_array function
DROP FUNCTION IF EXISTS json_to_array(json);
CREATE FUNCTION
  json_to_array(
      json_array json
  )
  RETURNS TABLE (
      pg_array text[]
  )
  AS $BODY$
    BEGIN
      RETURN QUERY SELECT
        ARRAY_AGG(val) AS pg_array
      FROM
        (
          SELECT
            json_array_elements(json_array)::text AS val
          ORDER BY
            val
        ) AS json_elements
      ;
    END;
  $BODY$
LANGUAGE plpgsql;

-- json_keys_array function
DROP FUNCTION IF EXISTS json_keys_array(json);
CREATE FUNCTION
  json_keys_array(
      json_object json
  )
  RETURNS TABLE (
      pg_array text[]
  )
  AS $BODY$
    BEGIN
      RETURN QUERY SELECT
        ARRAY_AGG(val) AS pg_array
      FROM
        (
          SELECT
            json_object_keys(json_object)::text AS val
          ORDER BY
            val
        ) AS json_elements
      ;
    END;
  $BODY$
LANGUAGE plpgsql;

CREATE STREAM cqobjectagg_stream (key text, tval text, fval float8, ival integer, n text, v integer, t text);

-- json_agg
CREATE CONTINUOUS VIEW test_json_agg AS SELECT key::text, json_agg(tval::text) AS j0, json_agg(fval::float8) AS j1, json_agg(ival::integer) AS j2 FROM cqobjectagg_stream GROUP BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('x', 'more text', 0.01, 42), ('x', 'blaahhhh', 0.01, 42);
INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('y', '4.2', 1.01, 42), ('z', '\"quoted\"', 2.01, 42), ('x', '', 0.01, 42), ('z', '2', '3', '4');

SELECT key, array_sort(json_to_array(j0)) FROM test_json_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1)) FROM test_json_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2)) FROM test_json_agg ORDER BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('y', 'more text', 0.01, 42), ('z', 'blaahhhh', 0.01, 42);

SELECT key, array_sort(json_to_array(j0)) FROM test_json_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1)) FROM test_json_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2)) FROM test_json_agg ORDER BY key;

-- json_object_agg
CREATE CONTINUOUS VIEW test_object_agg0 AS SELECT n, json_object_agg(n::text, v::integer) FROM cqobjectagg_stream GROUP BY n;
CREATE CONTINUOUS VIEW test_object_agg1 AS SELECT n, json_object_agg(n::text, t::text) FROM cqobjectagg_stream GROUP BY n;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k2', 4, '4');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k3', 5, '5'), ('k3', 6, '6');

SELECT n, array_sort(json_keys_array(json_object_agg)) FROM test_object_agg0 ORDER BY n;
SELECT n, array_sort(json_keys_array(json_object_agg)) FROM test_object_agg1 ORDER BY n;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');

SELECT n, array_sort(json_keys_array(json_object_agg)) FROM test_object_agg0 ORDER BY n;
SELECT n, array_sort(json_keys_array(json_object_agg)) FROM test_object_agg1 ORDER BY n;

DROP STREAM cqobjectagg_stream CASCADE;

-- bytea_string_agg, string_agg
CREATE STREAM cqobjectagg_stream (k text, v bytea);
CREATE STREAM cqobjectagg_text_stream (k text, v text);

CREATE CONTINUOUS VIEW test_bstring_agg AS SELECT k::text, string_agg(v::bytea, '@') FROM cqobjectagg_stream GROUP by k;
CREATE CONTINUOUS VIEW test_string_agg AS SELECT k::text, string_agg(v::text, '@') FROM cqobjectagg_text_stream GROUP by k;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 'val0'), ('x', 'val1');
INSERT INTO cqobjectagg_stream (k, v) VALUES ('y', 'val0'), ('y', 'val1');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('x', 'val0'), ('x', 'val1');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('y', 'val0'), ('y', 'val1');

SELECT k, array_sort(regexp_split_to_array(encode(string_agg, 'escape'), '@')) FROM test_bstring_agg ORDER BY k;
SELECT k, array_sort(regexp_split_to_array(string_agg, '@')) FROM test_string_agg ORDER BY k;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 'val3');
INSERT INTO cqobjectagg_stream (k, v) VALUES ('z', 'val4');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('x', 'val3');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('z', 'val4');

SELECT k, array_sort(regexp_split_to_array(encode(string_agg, 'escape'), '@')) FROM test_bstring_agg ORDER BY k;
SELECT k, array_sort(regexp_split_to_array(string_agg, '@')) FROM test_string_agg ORDER BY k;

DROP STREAM cqobjectagg_stream CASCADE;
DROP STREAM cqobjectagg_text_stream CASCADE;

-- array_agg
CREATE STREAM cqobjectagg_stream (k text, v int);

CREATE CONTINUOUS VIEW test_array_agg AS SELECT k::text, array_agg(v::integer) FROM cqobjectagg_stream GROUP BY k;
\d+ test_array_agg_mrel

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 0), ('x', 1), ('x', 2), ('x', 3);
INSERT INTO cqobjectagg_stream (k, v) VALUES ('y', 0), ('y', 1);

SELECT k, array_sort(array_agg) FROM test_array_agg ORDER BY k;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 4), ('y', 2), ('z', 10), ('z', 20);

SELECT k, array_sort(array_agg) FROM test_array_agg ORDER BY k;

DROP CONTINUOUS VIEW test_array_agg;

CREATE CONTINUOUS VIEW test_array_agg AS SELECT array_agg(k::text) FROM cqobjectagg_stream;

INSERT INTO cqobjectagg_stream (k) VALUES ('hello'), ('world');
SELECT pg_sleep(0.1);
INSERT INTO cqobjectagg_stream (k) VALUES ('lol'), ('cat');

SELECT array_sort(array_agg) FROM test_array_agg;

DROP STREAM cqobjectagg_stream cASCADE;

-- json_object_int_sum
CREATE STREAM cqobjectagg_stream (x int, payload text);

CREATE CONTINUOUS VIEW jois AS SELECT x::integer, json_object_int_sum(payload::text), COUNT(*) FROM cqobjectagg_stream GROUP BY x;

INSERT INTO cqobjectagg_stream (x, payload) SELECT x % 10 AS x, '{ "k' || x::text || '": ' || x::integer || ' }' AS payload FROM generate_series(1, 100) AS x;
INSERT INTO cqobjectagg_stream (x, payload) SELECT x % 10 AS x, '{ "k' || x::text || '": ' || x::integer || ' }' AS payload FROM generate_series(1, 100) AS x;
INSERT INTO cqobjectagg_stream (x, payload) SELECT x % 10 AS x, '{ "k' || x::text || '": ' || x::integer || ' }' AS payload FROM generate_series(1, 100) AS x;
INSERT INTO cqobjectagg_stream (x, payload) SELECT x % 10 AS x, '{ "k' || x::text || '": ' || x::integer || ' }' AS payload FROM generate_series(1, 100) AS x;
INSERT INTO cqobjectagg_stream (x, payload) SELECT x % 10 AS x, '{ "k' || x::text || '": ' || x::integer || ' }' AS payload FROM generate_series(1, 100) AS x;

SELECT * FROM jois ORDER BY x;

DROP STREAM cqobjectagg_stream cASCADE;

-- array_agg_array
CREATE STREAM cqobjectagg_stream (x int, y int);

CREATE CONTINUOUS VIEW test_array_agg_array AS SELECT array_agg(ARRAY[x::int, y::int]) FROM cqobjectagg_stream;

INSERT INTO cqobjectagg_stream (x, y) VALUES (1, 11);
INSERT INTO cqobjectagg_stream (x, y) VALUES (2, 12);
SELECT pg_sleep(0.1);
INSERT INTO cqobjectagg_stream (x, y) VALUES (3, 13);

SELECT * FROM test_array_agg_array;

DROP STREAM cqobjectagg_stream CASCADE;

-- jsonb_agg
CREATE STREAM cqobjectagg_stream (key text, tval text, fval float8, ival integer, n text, v integer, t text);

CREATE CONTINUOUS VIEW test_jsonb_agg AS SELECT key::text, jsonb_agg(tval::text) AS j0, jsonb_agg(fval::float8) AS j1, jsonb_agg(ival::integer) AS j2 FROM cqobjectagg_stream GROUP BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('x', 'more text', 0.01, 42), ('x', 'blaahhhh', 0.01, 42);
INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('y', '4.2', 1.01, 42), ('z', '\"quoted\"', 2.01, 42), ('x', '', 0.01, 42), ('z', '2', '3', '4');

SELECT key, array_sort(json_to_array(j0::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2::json)) FROM test_jsonb_agg ORDER BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('y', 'more text', 0.01, 42), ('z', 'blaahhhh', 0.01, 42);

SELECT key, array_sort(json_to_array(j0::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2::json)) FROM test_jsonb_agg ORDER BY key;

DROP CONTINUOUS VIEW test_jsonb_agg;

-- jsonb_object_agg
CREATE CONTINUOUS VIEW test_jsonb_object_agg0 AS SELECT n, jsonb_object_agg(n::text, v::integer) FROM cqobjectagg_stream GROUP BY n;
CREATE CONTINUOUS VIEW test_jsonb_object_agg1 AS SELECT n, jsonb_object_agg(n::text, t::text) FROM cqobjectagg_stream GROUP BY n;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k2', 4, '4');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k3', 5, '5'), ('k3', 6, '6');

SELECT n, array_sort(json_keys_array(jsonb_object_agg::json)) FROM test_jsonb_object_agg0 ORDER BY n;
SELECT n, array_sort(json_keys_array(jsonb_object_agg::json)) FROM test_jsonb_object_agg1 ORDER BY n;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');

SELECT n, array_sort(json_keys_array(jsonb_object_agg::json)) FROM test_jsonb_object_agg0 ORDER BY n;
SELECT n, array_sort(json_keys_array(jsonb_object_agg::json)) FROM test_jsonb_object_agg1 ORDER BY n;

DROP STREAM cqobjectagg_stream CASCADE;

DROP FUNCTION array_sort(anyarray);
DROP FUNCTION json_to_array(json);
DROP FUNCTION json_keys_array(json);

-- #1875 regression test
CREATE STREAM elapsed_json_stream_ts (ts TIMESTAMP, app_id varchar, country varchar, elapsed_time bigint);

CREATE CONTINUOUS TRANSFORM elapsed_json_transform_ts AS
SELECT '{ "' || ((elapsed_time / 10) * 10)::text || '": ' || 1 || ' }' elapsed_time_bucket, country, app_id, ts  FROM  elapsed_json_stream_ts;

CREATE CONTINUOUS VIEW elapsed_json_cv_ts_hour WITH (ttl = '2 hour', ttl_column = 'hour') AS
  SELECT date_round(ts, '1 hour') as hour,
    json_object_int_sum(elapsed_time_bucket) as elapsed_time_bucket,
    app_id,
    country
  FROM output_of('elapsed_json_transform_ts')
GROUP BY hour, app_id, country;

INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:00:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:00:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:01:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:01:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:01:00', 'a', 'us', 100);

SELECT * from elapsed_json_cv_ts_hour ORDER BY hour;
SELECT combine(elapsed_time_bucket) FROM elapsed_json_cv_ts_hour;

CREATE CONTINUOUS VIEW elapsed_json_cv_ts_day WITH (ttl = '10 days', ttl_column = 'day') AS
  SELECT date_round((new).hour, '24 hours') as day,
    combine((delta).elapsed_time_bucket) AS elapsed_time_bucket,
    (new).app_id,
    (new).country
  FROM output_of('elapsed_json_cv_ts_hour')
GROUP BY day, app_id, country;

INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:02:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:02:00', 'a', 'us', 100);
INSERT INTO elapsed_json_stream_ts VALUES ('2017-10-27 00:03:00', 'a', 'us', 100);

SELECT * FROM elapsed_json_cv_ts_day ORDER BY day;
SELECT combine(elapsed_time_bucket) FROM elapsed_json_cv_ts_day;

DROP STREAM elapsed_json_stream_ts CASCADE;
