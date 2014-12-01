SET debug_sync_stream_insert = 'on';

CREATE CONTINUOUS VIEW test_stream_casts0 AS SELECT k::text, SUM(v::float8) AS fsum, SUM(i::int8) AS isum, COUNT(*) FROM stream_casts_stream GROUP BY k;

CREATE CONTINUOUS VIEW test_stream_casts1 AS SELECT k::integer, SUM(v::float4) AS fsum, SUM(i::int4) AS isum, COUNT(*) FROM stream_casts_stream GROUP BY k;

ACTIVATE test_stream_casts0, test_stream_casts1;

INSERT INTO stream_casts_stream (k, v, i) VALUES ('001', 1.224, 1002);
INSERT INTO stream_casts_stream (k, v, i) VALUES ('102', 1e15, -100);
INSERT INTO stream_casts_stream (k, v, i) VALUES ('144', -1e-3, 1);

DEACTIVATE test_stream_casts0, test_stream_casts1;

SELECT * FROM test_stream_casts0 ORDER BY k;
SELECT * FROM test_stream_casts1 ORDER BY k;

DROP CONTINUOUS VIEW test_stream_casts0;
DROP CONTINUOUS VIEW test_stream_casts1;

