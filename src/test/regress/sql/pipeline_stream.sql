CREATE CONTINUOUS VIEW ps0 AS SELECT id::integer FROM stream0;
CREATE CONTINUOUS VIEW ps1 AS SELECT stream0.id::integer FROM stream0, stream1;
CREATE CONTINUOUS VIEW ps2 AS SELECT id::integer FROM stream2;
CREATE CONTINUOUS VIEW ps3 AS SELECT id::integer FROM stream3;
CREATE CONTINUOUS VIEW ps4 AS SELECT stream4.id::integer FROM stream4, stream5;

ACTIVATE ps0, ps1, ps2, ps3, ps4;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps1;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps2;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps3;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps4;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps0;

SELECT * FROM pipeline_stream ORDER BY name;

ACTIVATE ps0;

SELECT * FROM pipeline_stream ORDER BY name;

ACTIVATE ps4;

SELECT * FROM pipeline_stream ORDER BY name;

DEACTIVATE ps0, ps4;

DROP CONTINUOUS VIEW ps0;
DROP CONTINUOUS VIEW ps1;
DROP CONTINUOUS VIEW ps2;
DROP CONTINUOUS VIEW ps3;
DROP CONTINUOUS VIEW ps4;
