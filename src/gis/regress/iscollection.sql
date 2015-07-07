-- Ensure there are no false-positives
SELECT 'point', ST_IsCollection('POINT(42 42)');
SELECT 'poly', ST_IsCollection('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))');
SELECT 'line', ST_IsCollection('LINESTRING(0 0, 10 10)');

-- PostGIS doesn't support typed empties...
--SELECT 'empty point', ST_IsCollection('POINT EMPTY');
--SELECT 'empty poly', ST_IsCollection('POLYGON EMPTY');
--SELECT 'empty line', ST_IsCollection('LINESTRING EMPTY');

--Ensure that all collections return true (even if they contain a single geometry).
SELECT 'empty multipoint', ST_IsCollection('MULTIPOINT EMPTY');
SELECT 'multipoint', ST_IsCollection('MULTIPOINT((0 0))');
SELECT 'multipoint+', ST_IsCollection('MULTIPOINT((0 0), (42 42))');

SELECT 'empty multiline', ST_IsCollection('MULTILINESTRING EMPTY');
SELECT 'multiline', ST_IsCollection('MULTILINESTRING((0 0, 10 10))');
SELECT 'multiline+', ST_IsCollection('MULTILINESTRING((0 0, 10 10), (100 100, 142 142))');

SELECT 'empty multipoly', ST_IsCollection('MULTIPOLYGON EMPTY');
SELECT 'multipoly', ST_IsCollection('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)))');
SELECT 'multipoly+', ST_IsCollection('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((100 100, 110 100, 110 110, 100 110, 100 100)))');

SELECT 'empty collection', ST_IsCollection('GEOMETRYCOLLECTION EMPTY');
SELECT 'collection', ST_IsCollection('GEOMETRYCOLLECTION(POINT(0 0))');
SELECT 'collection+', ST_IsCollection('GEOMETRYCOLLECTION(POINT(0 0), POINT(42 42))');


