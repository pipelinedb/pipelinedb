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

DROP CONTINUOUS VIEW test_json_agg;

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

DROP CONTINUOUS VIEW test_object_agg0;
DROP CONTINUOUS VIEW test_object_agg1;

-- bytea_string_agg, string_agg
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

DROP CONTINUOUS VIEW test_bstring_agg;
DROP CONTINUOUS VIEW test_string_agg;

-- array_agg
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

DROP CONTINUOUS VIEW test_array_agg;

DROP FUNCTION array_sort(anyarray);
DROP FUNCTION json_to_array(json);
DROP FUNCTION json_keys_array(json);
