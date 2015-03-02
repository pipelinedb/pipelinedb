SET debug_sync_stream_insert = 'on';

-- json_agg
CREATE CONTINUOUS VIEW test_json_agg AS SELECT key::text, json_agg(tval::text) AS j0, json_agg(fval::float8) AS j1, json_agg(ival::integer) AS j2 FROM cqobjectagg_stream GROUP BY key;

ACTIVATE test_json_agg;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('x', 'more text', 0.01, 42), ('x', 'blaahhhh', 0.01, 42);
INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('y', '4.2', 1.01, 42), ('z', '\"quoted\"', 2.01, 42), ('x', '', 0.01, 42), ('z', '2', '3', '4');

DEACTIVATE test_json_agg;

SELECT key, j0 FROM test_json_agg ORDER BY key;
SELECT key, j1 FROM test_json_agg ORDER BY key;
SELECT key, j2 FROM test_json_agg ORDER BY key;

ACTIVATE test_json_agg;

INSERT INTO cqobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 42), ('y', 'more text', 0.01, 42), ('z', 'blaahhhh', 0.01, 42);

DEACTIVATE test_json_agg;

SELECT j0 FROM test_json_agg ORDER BY key;
SELECT j1 FROM test_json_agg ORDER BY key;
SELECT j2 FROM test_json_agg ORDER BY key;

-- json_object_agg
CREATE CONTINUOUS VIEW test_object_agg0 AS SELECT n, json_object_agg(n::text, v::integer) FROM cqobjectagg_stream GROUP BY n;
CREATE CONTINUOUS VIEW test_object_agg1 AS SELECT n, json_object_agg(n::text, t::text) FROM cqobjectagg_stream GROUP BY n;

ACTIVATE test_object_agg0;
ACTIVATE test_object_agg1;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k2', 4, '4');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k3', 5, '5'), ('k3', 6, '6');

DEACTIVATE test_object_agg0, test_object_agg1;

SELECT * FROM test_object_agg0 ORDER BY n;
SELECT * FROM test_object_agg1 ORDER BY n;

ACTIVATE test_object_agg0;
ACTIVATE test_object_agg1;

INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');

DEACTIVATE test_object_agg0, test_object_agg1;

SELECT * FROM test_object_agg0 ORDER BY n;
SELECT * FROM test_object_agg1 ORDER BY n;

-- bytea_string_agg, string_agg
CREATE CONTINUOUS VIEW test_bstring_agg AS SELECT k::text, string_agg(v::bytea, '00000') FROM cqobjectagg_stream GROUP by k;
CREATE CONTINUOUS VIEW test_string_agg AS SELECT k::text, string_agg(v::text, k::text) FROM cqobjectagg_text_stream GROUP by k;

ACTIVATE test_bstring_agg;
ACTIVATE test_string_agg;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 'val0'), ('x', 'val1');
INSERT INTO cqobjectagg_stream (k, v) VALUES ('y', 'val0'), ('y', 'val1');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('x', 'val0'), ('x', 'val1');
INSERT INTO cqobjectagg_text_stream (k, v) VALUES ('y', 'val0'), ('y', 'val1');

DEACTIVATE test_bstring_agg, test_string_agg;

SELECT * FROM test_bstring_agg ORDER BY k;
SELECT * FROM test_string_agg ORDER BY k;

ACTIVATE test_bstring_agg;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 'val3');
INSERT INTO cqobjectagg_stream (k, v) VALUES ('z', 'val4');

DEACTIVATE test_bstring_agg;

SELECT * FROM test_bstring_agg ORDER BY k;
SELECT * FROM test_string_agg ORDER BY k;

-- array_agg
CREATE CONTINUOUS VIEW test_array_agg AS SELECT k::text, array_agg(v::integer) FROM cqobjectagg_stream GROUP BY k;

ACTIVATE test_array_agg;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 0), ('x', 1), ('x', 2), ('x', 3);
INSERT INTO cqobjectagg_stream (k, v) VALUES ('y', 0), ('y', 1);

DEACTIVATE test_array_agg;

SELECT * FROM test_array_agg ORDER BY k;

ACTIVATE test_array_agg;

INSERT INTO cqobjectagg_stream (k, v) VALUES ('x', 4), ('y', 2), ('z', 10), ('z', 20);

DEACTIVATE test_array_agg;

SELECT * FROM test_array_agg ORDER BY k;

DROP CONTINUOUS VIEW test_json_agg;
DROP CONTINUOUS VIEW test_object_agg0;
DROP CONTINUOUS VIEW test_object_agg1;
DROP CONTINUOUS VIEW test_bstring_agg;
DROP CONTINUOUS VIEW test_string_agg;
DROP CONTINUOUS VIEW test_array_agg;
