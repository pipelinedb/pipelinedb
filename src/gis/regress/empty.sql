-- ST_SnapToGrid
SELECT 'T1.1', ST_AsEWKT(ST_SnapToGrid('POINT EMPTY', 1));
SELECT 'T1.2', ST_AsEWKT(ST_SnapToGrid('LINESTRING EMPTY', 1));
SELECT 'T1.3', ST_AsEWKT(ST_SnapToGrid('SRID=4326;POLYGON EMPTY', 1));

-- ST_Buffer
SELECT 'T2.1', ST_AsEWKT(ST_Buffer('SRID=4326;POINT EMPTY', 0));
SELECT 'T2.2', ST_AsEWKT(ST_Buffer('SRID=4326;LINESTRING EMPTY', 0));
SELECT 'T2.3', ST_AsEWKT(ST_Buffer('SRID=4326;MULTIPOLYGON EMPTY', 0));
WITH b as ( SELECT ST_Buffer('SRID=4326;MULTIPOINT EMPTY', 1) as g )
SELECT 'T2.4', ST_Srid(g), GeometryType(g) from b;

-- ST_AsGML (output may need some tweaking)
SELECT 'T3.1', ST_AsGML('POINT EMPTY');
SELECT 'T3.2', ST_AsGML('LINESTRING EMPTY');
SELECT 'T3.3', ST_AsGML('POLYGON EMPTY');
SELECT 'T3.4', ST_AsGML('MULTIPOLYGON EMPTY');
SELECT 'T3.5', ST_AsGML('MULTILINESTRING EMPTY');
SELECT 'T3.6', ST_AsGML('GEOMETRYCOLLECTION EMPTY');
SELECT 'T3.7', ST_AsGML(3,'POINT EMPTY'::geometry);
SELECT 'T3.8', ST_AsGML(3,'LINESTRING EMPTY'::geometry);
SELECT 'T3.9', ST_AsGML(3,'POLYGON EMPTY'::geometry);
SELECT 'T3.10', ST_AsGML(3,'MULTIPOLYGON EMPTY'::geometry);
SELECT 'T3.11', ST_AsGML(3,'MULTILINESTRING EMPTY'::geometry);
SELECT 'T3.12', ST_AsGML(3,'GEOMETRYCOLLECTION EMPTY'::geometry);
SELECT 'T3.13', ST_AsGML(3,'POINT EMPTY'::geometry);
SELECT 'T3.14', ST_AsGML(3,'LINESTRING EMPTY'::geometry);
SELECT 'T3.15', ST_AsGML(3,'POLYGON EMPTY'::geometry);
SELECT 'T3.16', ST_AsGML(3,'MULTIPOLYGON EMPTY'::geometry);
SELECT 'T3.17', ST_AsGML(3,'MULTILINESTRING EMPTY'::geometry);
SELECT 'T3.18', ST_AsGML(3,'GEOMETRYCOLLECTION EMPTY'::geometry);

-- See http://trac.osgeo.org/postgis/wiki/DevWikiEmptyGeometry

WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry,
 120 as tolerance
 ) SELECT 'ST_Buffer(empty, tolerance) == empty', encode(ST_AsBinary(ST_Buffer(empty, tolerance),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Union(geometry, empty) == geometry', encode(ST_AsBinary(ST_Union(geometry, empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_Union(empty, empty) == empty', encode(ST_AsBinary(ST_Union(empty, empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Intersection(geometry, empty) == geometry', encode(ST_AsBinary(ST_Intersection(geometry, empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_Intersection(empty, empty) == empty', encode(ST_AsBinary(ST_Intersection(empty, empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Difference(geometry, empty) == geometry', encode(ST_AsBinary(ST_Difference(geometry, empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Difference(empty, geometry) == empty', encode(ST_AsBinary(ST_Difference(empty, geometry),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Distance(geometry, empty) == NULL', ST_Distance(geometry, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry,
 120 as tolerance
 ) SELECT 'ST_DWithin(geometry, empty, tolerance) == FALSE', ST_DWithin(geometry, empty, tolerance) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Within(geometry, empty) == FALSE', ST_Within(geometry, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Contains(empty, geometry) == FALSE', ST_Contains(empty, geometry) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Within(empty, geometry) == FALSE', ST_Within(empty, geometry) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_Contains(empty, empty) == FALSE', ST_Contains(empty, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Intersects(geometry, empty) == FALSE', ST_Intersects(geometry, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_Intersects(empty, empty) == FALSE', ST_Intersects(empty, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Disjoint(empty, empty) == TRUE', ST_Disjoint(empty, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 'POLYGON((0 0, 10 0, 5 5, 0 0))'::geometry as geometry
 ) SELECT 'ST_Disjoint(geometry, empty) == TRUE', ST_Disjoint(geometry, empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty1,
 'POINT Z EMPTY'::geometry as empty2
 ) SELECT 'ST_Equals(empty1, empty2) == TRUE', ST_Equals(empty1, empty2) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_IsSimple(empty) == TRUE', ST_IsSimple(empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_IsValid(empty) == TRUE', ST_IsValid(empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_NumGeometries(empty) == 0', ST_NumGeometries(empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_NRings(empty) == 0', ST_NRings(empty) FROM inp;
WITH inp AS (SELECT
 'LINESTRING EMPTY'::geometry as empty
 ) SELECT 'ST_NumPoints(empty) == 0', ST_NumPoints(empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_NPoints(empty) == 0', ST_NPoints(empty) FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 1 as n
 ) SELECT 'ST_GeometryN(empty, n) == empty', encode(ST_AsEWKB(ST_GeometryN(empty, n),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_ExteriorRing(empty) == empty', encode(ST_AsEWKB(ST_ExteriorRing(empty),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty,
 1 as n
 ) SELECT 'ST_InteriorRingN(empty, n) == NULL', encode(ST_AsEWKB(ST_InteriorRingN(empty, n),'ndr'),'hex') FROM inp;
WITH inp AS (SELECT
 'POLYGON EMPTY'::geometry as empty
 ) SELECT 'ST_Area(empty) == 0', ST_Area(empty) FROM inp;
WITH inp AS (SELECT
 'LINESTRING EMPTY'::geometry as empty
 ) SELECT 'ST_Length(empty) == 0', ST_Length(empty) FROM inp;


-- Operators

-- same box, see http://trac.osgeo.org/postgis/ticket/1453
SELECT '~=', 'POINT EMPTY'::geometry ~= 'POINT EMPTY'::geometry;
