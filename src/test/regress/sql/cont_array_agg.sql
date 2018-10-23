-- array_sort function
CREATE FUNCTION
  ca_array_sort(
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

-- array_agg
CREATE FOREIGN TABLE cqobjectagg_stream (k text, v int) SERVER pipelinedb;

CREATE VIEW test_array_agg AS SELECT k::text, array_agg(v::integer) FROM cqobjectagg_stream GROUP BY k;
\d+ test_array_agg_mrel

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 0), ('x', 1), ('x', 2), ('x', 3);
INSERT INTO cqobjectagg_stream (k, v) VALUES ('y', 0), ('y', 1);

SELECT k, ca_array_sort(array_agg) FROM test_array_agg ORDER BY k;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 4), ('y', 2), ('z', 10), ('z', 20);

SELECT k, ca_array_sort(array_agg) FROM test_array_agg ORDER BY k;

DROP VIEW test_array_agg;

CREATE VIEW test_array_agg AS SELECT array_agg(k::text) FROM cqobjectagg_stream;

INSERT INTO cqobjectagg_stream (k) VALUES ('hello'), ('world');
SELECT pg_sleep(0.1);
INSERT INTO cqobjectagg_stream (k) VALUES ('lol'), ('cat');

SELECT ca_array_sort(array_agg) FROM test_array_agg;
DROP FOREIGN TABLE cqobjectagg_stream CASCADE;

-- array_agg_array
CREATE FOREIGN TABLE cqobjectagg_stream (x int, y int) SERVER pipelinedb;

CREATE VIEW test_array_agg_array AS SELECT array_agg(ARRAY[x::int, y::int]) FROM cqobjectagg_stream;

INSERT INTO cqobjectagg_stream (x, y) VALUES (1, 11);
INSERT INTO cqobjectagg_stream (x, y) VALUES (2, 12);
SELECT pg_sleep(0.1);
INSERT INTO cqobjectagg_stream (x, y) VALUES (3, 13);

SELECT * FROM test_array_agg_array;

DROP FOREIGN TABLE cqobjectagg_stream CASCADE;