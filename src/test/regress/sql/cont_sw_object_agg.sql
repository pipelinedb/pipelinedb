-- json_agg
CREATE CONTINUOUS VIEW test_sw_json_agg AS SELECT key::text, json_agg(tval::text) AS j0, json_agg(fval::float8) AS j1, json_agg(ival::integer) AS j2 FROM cqswobjectagg_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' GROUP BY key;

ACTIVATE test_sw_json_agg;

INSERT INTO cqswobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.01, 41), ('x', 'more text', 0.02, 42), ('x', 'blaahhhh', 0.03, 43);
INSERT INTO cqswobjectagg_stream (key, tval, fval, ival) VALUES ('y', '4.2', 1.01, 42), ('z', '\"quoted\"', 2.01, 42), ('x', '', 0.04, 44), ('z', '2', '3', '4');

DEACTIVATE test_sw_json_agg;

SELECT key, j0 FROM test_sw_json_agg ORDER BY key;
SELECT key, j1 FROM test_sw_json_agg ORDER BY key;
SELECT key, j2 FROM test_sw_json_agg ORDER BY key;

ACTIVATE test_sw_json_agg;

INSERT INTO cqswobjectagg_stream (key, tval, fval, ival) VALUES ('x', 'text', 0.05, 45), ('y', 'more text', 0.02, 43), ('z', 'blaahhhh', 0.03, 44);

DEACTIVATE test_sw_json_agg;

SELECT j0 FROM test_sw_json_agg ORDER BY key;
SELECT j1 FROM test_sw_json_agg ORDER BY key;
SELECT j2 FROM test_sw_json_agg ORDER BY key;

-- json_object_agg
CREATE CONTINUOUS VIEW test_sw_object_agg0 AS SELECT n, json_object_agg(n::text, v::integer) FROM cqswobjectagg_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' GROUP BY n;
CREATE CONTINUOUS VIEW test_sw_object_agg1 AS SELECT n, json_object_agg(n::text, t::text) FROM cqswobjectagg_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' GROUP BY n;

ACTIVATE test_sw_object_agg0;
ACTIVATE test_sw_object_agg1;

INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k2', 4, '4');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k3', 5, '5'), ('k3', 6, '6');

DEACTIVATE test_sw_object_agg0, test_sw_object_agg1;

SELECT * FROM test_sw_object_agg0 ORDER BY n;
SELECT * FROM test_sw_object_agg1 ORDER BY n;

ACTIVATE test_sw_object_agg0;
ACTIVATE test_sw_object_agg1;

INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k0', 1, '1');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k0', 2, '2');
INSERT INTO cqswobjectagg_stream (n, v, t) VALUES ('k1', 3, '3');

DEACTIVATE test_sw_object_agg0, test_sw_object_agg1;

SELECT * FROM test_sw_object_agg0 ORDER BY n;
SELECT * FROM test_sw_object_agg1 ORDER BY n;

-- array_agg
CREATE CONTINUOUS VIEW test_sw_array_agg AS SELECT k::text, array_agg(v::integer) FROM cqswobjectagg_stream WHERE arrival_timestamp > clock_timestamp() - interval '1 hour' GROUP BY k;

ACTIVATE test_sw_array_agg;

INSERT INTO cqswobjectagg_stream (k, v) VALUES ('x', 0), ('x', 1), ('x', 2), ('x', 3);
INSERT INTO cqswobjectagg_stream (k, v) VALUES ('y', 0), ('y', 1);

DEACTIVATE test_sw_array_agg;

SELECT * FROM test_sw_array_agg ORDER BY k;

ACTIVATE test_sw_array_agg;

INSERT INTO cqswobjectagg_stream (k, v) VALUES ('x', 4), ('y', 2), ('z', 10), ('z', 20);

DEACTIVATE test_sw_array_agg;

SELECT * FROM test_sw_array_agg ORDER BY k;

DROP CONTINUOUS VIEW test_sw_json_agg;
DROP CONTINUOUS VIEW test_sw_object_agg0;
DROP CONTINUOUS VIEW test_sw_object_agg1;
DROP CONTINUOUS VIEW test_sw_array_agg;
