-- Implement a simple average aggregate that stores its state as JSON
-- in order to test combinein/transout functions

-- Transition function
CREATE FUNCTION combinable_avg_trans(state integer[], incoming integer)RETURNS integer[] AS $$
BEGIN
	IF state IS NULL THEN
		RETURN ARRAY[incoming, 1];
	ELSE
		RETURN ARRAY[state[1] + incoming, state[2] + 1];
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Final function
CREATE FUNCTION combinable_avg_final(state integer[]) RETURNS float8 AS $$
BEGIN
	RETURN state[1]::float8 / state[2];
END;
$$
LANGUAGE plpgsql;

-- Combine function
CREATE FUNCTION combinable_avg_combine(state integer[], incoming integer[]) RETURNS integer[] AS $$
BEGIN
	IF state IS NULL THEN
		RETURN ARRAY[incoming[1], incoming[2]];
	ELSE
		RETURN ARRAY[state[1] + incoming[1], state[2] + incoming[2]];
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Trans out function
CREATE FUNCTION combinable_avg_transout(state integer[]) RETURNS json AS $$
BEGIN
	RETURN format('{ "sum": %s, "count": %s }', state[1], state[2])::json;
END;
$$
LANGUAGE plpgsql;

-- Combine in function
CREATE FUNCTION combinable_avg_combinein(state json) RETURNS integer[] AS $$
BEGIN
	IF state IS NULL OR state::text = '' THEN
		RETURN ARRAY[0, 0];
	ELSE
		RETURN ARRAY[(state->>'sum')::integer, (state->>'count')::integer];
	END IF;
END;
$$
LANGUAGE plpgsql;

CREATE AGGREGATE combinable_avg(integer)
(
	sfunc=combinable_avg_trans,
	finalfunc=combinable_avg_final,
	stype=integer[],
	combinefunc=combinable_avg_combine,
	transoutfunc=combinable_avg_transout,
	combineinfunc=combinable_avg_combinein
);

-- Verify that pipeline_combine entry created
SELECT count(*) FROM pipeline_combine WHERE combinefn IN
  (SELECT oid FROM pg_proc WHERE proname='combinable_avg_combine');

-- Verify that the matrel looks ok
CREATE CONTINUOUS VIEW test_combinable_aggs_v0 AS
	SELECT x::integer, combinable_avg(y::integer) FROM cca_stream GROUP BY x;

\d+ test_combinable_aggs_v0_mrel;

INSERT INTO cca_stream (x, y) VALUES (0, 0);
INSERT INTO cca_stream (x, y) VALUES (0, 1);
INSERT INTO cca_stream (x, y) VALUES (1, 2);
INSERT INTO cca_stream (x, y) VALUES (1, 3);

-- Force multiple batch executions so we actually combine something
SELECT pg_sleep(0.2);

INSERT INTO cca_stream (x, y) VALUES (2, 4);
INSERT INTO cca_stream (x, y) VALUES (2, 5);
INSERT INTO cca_stream (x, y) VALUES (3, 6);
INSERT INTO cca_stream (x, y) VALUES (3, 7);

SELECT round(combinable_avg) FROM test_combinable_aggs_v0 ORDER BY x;
SELECT x, combinable_avg FROM test_combinable_aggs_v0_mrel ORDER BY x;

-- We should also be able to run user combines on it
SELECT combine(combinable_avg) FROM test_combinable_aggs_v0;

-- Can't drop combine, transout, or combinein functions that an aggregate depends on
DROP FUNCTION combinable_avg_combine(integer[], integer[]);
DROP FUNCTION combinable_avg_transout(integer[]);
DROP FUNCTION combinable_avg_combinein(json);

DROP CONTINUOUS VIEW test_combinable_aggs_v0;

DROP AGGREGATE combinable_avg (integer);

-- pipeline_combine entry should be removed
SELECT count(*) FROM pipeline_combine WHERE combinefn IN
  (SELECT oid FROM pg_proc WHERE proname='combinable_avg_combine');

-- Dropping the aggregate shouldn't affect any of the functions in pipeline_combine
DROP FUNCTION combinable_avg_trans(integer[], integer) CASCADE;
DROP FUNCTION combinable_avg_final(integer[]) CASCADE;
DROP FUNCTION combinable_avg_combine(integer[], integer[]) CASCADE;
DROP FUNCTION combinable_avg_transout(integer[]) CASCADE;
DROP FUNCTION combinable_avg_combinein(json) CASCADE;

-- Test polymorphic types
CREATE FUNCTION set_add (
  anyarray,
  anyelement
) RETURNS anyarray AS
$$
BEGIN
  IF $1 IS NULL THEN
    RETURN ARRAY[$2];
  END IF;

  IF ARRAY[$2] && $1 THEN
    RETURN $1;
  END IF;

  RETURN array_append($1, $2);
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION set_merge (
  anyarray,
  anyarray
) RETURNS anyarray AS
$$
DECLARE
  i integer;
BEGIN
  IF $1 IS NULL THEN
    IF $2 IS NULL THEN
      RETURN NULL;
    END IF;
    RETURN $2;
  END IF;

  IF $2 IS NULL THEN
    RETURN $1;
  END IF;

  FOR i IN 1 .. array_length($2, 1) LOOP
    IF NOT ARRAY[$2[i]] && $1 THEN
      $1 := array_append($1, $2[i]);
    END IF;
  END LOOP;

  RETURN $1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE AGGREGATE test_set_agg (anyelement) (
  stype = anyarray,
  sfunc = set_add,
  combinefunc = set_merge
);

CREATE TABLE cont_plpgsql_t (x text);

INSERT INTO cont_plpgsql_t VALUES ('a'), ('b'), ('c');
INSERT INTO cont_plpgsql_t VALUES ('a'), ('b'), ('c');
INSERT INTO cont_plpgsql_t VALUES ('d'), ('e'), ('f');

SELECT test_set_agg(x) FROM cont_plpgsql_t;

SELECT set_merge(test_set_agg(x), set_add(NULL::text[], 'z')) FROM cont_plpgsql_t;

CREATE CONTINUOUS VIEW cont_plpgsql_cv AS SELECT test_set_agg(x::text) FROM cont_plpgsql_s;

INSERT INTO cont_plpgsql_s (x) VALUES ('a'), ('b'), ('c');
SELECT pg_sleep(0.2);
INSERT INTO cont_plpgsql_s (x) VALUES ('a'), ('b'), ('c');
SELECT pg_sleep(0.2);
INSERT INTO cont_plpgsql_s (x) VALUES ('d'), ('e'), ('f');

SELECT unnest(test_set_agg) FROM cont_plpgsql_cv ORDER BY unnest;

DROP TABLE cont_plpgsql_t;
DROP CONTINUOUS VIEW cont_plpgsql_cv;

DROP AGGREGATE test_set_agg (anyelement);
DROP FUNCTION set_add (anyarray, anyelement);
DROP FUNCTION set_merge (anyarray, anyarray);
