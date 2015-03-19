CREATE CONTINUOUS VIEW ps0 AS SELECT id::integer FROM stream0;
CREATE CONTINUOUS VIEW ps1 AS SELECT stream0.id::integer FROM stream0, stream1;

SELECT * FROM pipeline_stream ORDER BY name;

CREATE CONTINUOUS VIEW ps2 AS SELECT id::integer FROM stream2;
CREATE CONTINUOUS VIEW ps3 AS SELECT id::integer FROM stream3;
CREATE CONTINUOUS VIEW ps4 AS SELECT stream4.id::integer FROM stream4, stream5;

SELECT * FROM pipeline_stream ORDER BY name;

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

SELECT * FROM pipeline_stream ORDER BY name;

DROP CONTINUOUS VIEW ps3;
DROP CONTINUOUS VIEW ps4;

SELECT * FROM pipeline_stream ORDER BY name;

INSERT INTO stream0 (x) VALUES (1);

CREATE CONTINUOUS VIEW ps5 AS SELECT x::int FROM stream0;

INSERT INTO stream0 (x) VALUES (1);

ACTIVATE ps5;

INSERT INTO stream0 (x) VALUES (1);

DEACTIVATE ps5;

INSERT INTO stream0 (x) VALUES (1);

DROP CONTINUOUS VIEW ps5;

INSERT INTO stream0 (x) VALUES (1);
