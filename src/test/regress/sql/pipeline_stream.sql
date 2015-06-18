CREATE CONTINUOUS VIEW ps0 AS SELECT id::integer FROM stream0;

SELECT * FROM pipeline_streams() ORDER BY name;

CREATE CONTINUOUS VIEW ps1 AS SELECT id::integer, val::text FROM stream0;

SELECT * FROM pipeline_streams() ORDER BY name;

CREATE CONTINUOUS VIEW ps2 AS SELECT id::float FROM stream0;
CREATE CONTINUOUS VIEW ps3 AS SELECT x::integer, y::timestamp FROM stream1;

SELECT * FROM pipeline_streams() ORDER BY name;

CREATE CONTINUOUS VIEW ps4 AS SELECT id::text FROM stream0;

SELECT * FROM pipeline_streams() ORDER BY name;

CREATE STREAM stream2 (x INT);

SELECT * FROM pipeline_streams() ORDER BY name;

CREATE CONTINUOUS VIEW ps5 AS SELECT x FROM stream2;

SELECT * FROM pipeline_streams() ORDER BY name;

DROP CONTINUOUS VIEW ps0;

SELECT * FROM pipeline_streams() ORDER BY name;

DROP CONTINUOUS VIEW ps1;

SELECT * FROM pipeline_streams() ORDER BY name;

DROP CONTINUOUS VIEW ps2;
DROP CONTINUOUS VIEW ps3;

SELECT * FROM pipeline_streams() ORDER BY name;

DROP CONTINUOUS VIEW ps5;

SELECT * FROM pipeline_streams() ORDER BY name;

DROP STREAM stream2;

SELECT * FROM pipeline_streams() ORDER BY name;
