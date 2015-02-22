SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW infer_v0 AS SELECT x::int8, y::bigint FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v1 AS SELECT x::int4, y::real FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v2 AS SELECT x::int2, y::integer FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v3 AS SELECT x::numeric, y::float8 FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v4 AS SELECT x::float4, y::numeric FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v5 AS SELECT x::money, y::money, s::json FROM infer_stream;
CREATE CONTINUOUS VIEW infer_v6 AS SELECT x::money, y::money, s::jsonb FROM infer_stream;

ACTIVATE infer_v0, infer_v1, infer_v2, infer_v3, infer_v4, infer_v5, infer_v6;

INSERT INTO infer_stream (x, y) VALUES (1.1, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1.999, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1.2, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1.2, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1.2, 3.2);
INSERT INTO infer_stream (x, y) VALUES (1.2, 3.2);
INSERT INTO infer_stream (s) VALUES ('4');
INSERT INTO infer_stream (s) VALUES ('[0, 1]');
INSERT INTO infer_stream (s) VALUES ('{"key": "1"}');

DEACTIVATE infer_v0, infer_v1, infer_v2, infer_v3, infer_v4, infer_v5, infer_v6;

SELECT * FROM infer_v0;
SELECT * FROM infer_v1;
SELECT * FROM infer_v2;
SELECT * FROM infer_v3;
SELECT * FROM infer_v4;
SELECT * FROM infer_v5;
SELECT * FROM infer_v6;

DROP CONTINUOUS VIEW infer_v0;
DROP CONTINUOUS VIEW infer_v1;
DROP CONTINUOUS VIEW infer_v2;
DROP CONTINUOUS VIEW infer_v3;
DROP CONTINUOUS VIEW infer_v4;
DROP CONTINUOUS VIEW infer_v5;
DROP CONTINUOUS VIEW infer_v6;
