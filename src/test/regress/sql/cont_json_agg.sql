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

CREATE FOREIGN TABLE cqobjectagg_stream (key text, tval text, fval float8, ival integer, n text, v integer, t text) SERVER pipelinedb;

-- json_agg
CREATE VIEW test_json_agg AS SELECT key::text, json_agg(tval::text) AS j0, json_agg(fval::float8) AS j1, json_agg(ival::integer) AS j2 FROM cqobjectagg_stream GROUP BY key;

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
CREATE VIEW test_object_agg0 AS SELECT n, json_object_agg(n::text, v::integer) FROM cqobjectagg_stream GROUP BY n;
CREATE VIEW test_object_agg1 AS SELECT n, json_object_agg(n::text, t::text) FROM cqobjectagg_stream GROUP BY n;

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

DROP FOREIGN TABLE cqobjectagg_stream CASCADE;

-- jsonb_agg
CREATE FOREIGN TABLE cqobjectagg_stream (key text, tval text, fval float8, ival integer, n text, v integer, t text) SERVER pipelinedb;

CREATE VIEW test_jsonb_agg AS SELECT key::text, jsonb_agg(tval::text) AS j0, jsonb_agg(fval::float8) AS j1, jsonb_agg(ival::integer) AS j2 FROM cqobjectagg_stream GROUP BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('x', 'more text', 0.01, 42), ('x', 'blaahhhh', 0.01, 42);
INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('y', '4.2', 1.01, 42), ('z', '\"quoted\"', 2.01, 42), ('x', '', 0.01, 42), ('z', '2', '3', '4');

SELECT key, array_sort(json_to_array(j0::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2::json)) FROM test_jsonb_agg ORDER BY key;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('y', 'more text', 0.01, 42), ('z', 'blaahhhh', 0.01, 42);

SELECT key, array_sort(json_to_array(j0::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j1::json)) FROM test_jsonb_agg ORDER BY key;
SELECT key, array_sort(json_to_array(j2::json)) FROM test_jsonb_agg ORDER BY key;

DROP VIEW test_jsonb_agg;

-- jsonb_object_agg
CREATE VIEW test_jsonb_object_agg0 AS SELECT n, jsonb_object_agg(n::text, v::integer) FROM cqobjectagg_stream GROUP BY n;
CREATE VIEW test_jsonb_object_agg1 AS SELECT n, jsonb_object_agg(n::text, t::text) FROM cqobjectagg_stream GROUP BY n;

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

DROP FOREIGN TABLE cqobjectagg_stream CASCADE;

DROP FUNCTION array_sort(anyarray);
DROP FUNCTION json_to_array(json);
DROP FUNCTION json_keys_array(json);

