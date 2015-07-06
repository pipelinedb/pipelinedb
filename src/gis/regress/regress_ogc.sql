---
--- Tests for GEOS/JTS implemented functions
---
---
-- Repeat all tests with new function names.
SET client_min_messages TO NOTICE;
SELECT 'buffer', ST_astext(ST_SnapToGrid(ST_buffer('POINT(0 0)'::geometry, 1, 2), 1.0e-6));

SELECT 'geomunion', ST_astext(ST_union('POINT(0 0)'::geometry, 'POINT(1 1)'::geometry));
SELECT 'convexhull', ST_asewkt(ST_convexhull('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'::geometry));
SELECT 'relate', ST_relate('POINT(0 0)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'relate', ST_relate('POINT(0 0)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry, 'F0FFFF*02');
SELECT 'relate', ST_relate('POINT(0 0)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry, 'F0FFF0*02');
SELECT 'disjoint', ST_disjoint('POINT(0 0)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'touches', ST_touches('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'intersects', ST_intersects('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry);
SELECT 'crosses', ST_crosses('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(-4 0, 1 1)'::geometry);
-- PIP - point within polygon
SELECT 'within100', ST_within('POINT(5 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on vertex of polygon
SELECT 'within101', ST_within('POINT(0 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point outside polygon
SELECT 'within102', ST_within('POINT(-1 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on edge of polygon
SELECT 'within103', ST_within('POINT(0 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point in line with polygon edge
SELECT 'within104', ST_within('POINT(0 12)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'within105', ST_within(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex 
SELECT 'within106', ST_within(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - point within polygon
SELECT 'disjoint100', ST_disjoint('POINT(5 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon vertex
SELECT 'disjoint101', ST_disjoint('POINT(0 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point outside polygon
SELECT 'disjoint102', ST_disjoint('POINT(-1 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon edge
SELECT 'disjoint103', ST_disjoint('POINT(0 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point in line with polygon edge
SELECT 'disjoint104', ST_disjoint('POINT(0 12)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'disjoint105', ST_disjoint(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex 
SELECT 'disjoint106', ST_disjoint(ST_GeomFromText('POINT(521543 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - point within polygon
SELECT 'disjoint150', ST_disjoint('POINT(5 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon vertex
SELECT 'disjoint151', ST_disjoint('POINT(0 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point outside polygon
SELECT 'disjoint152', ST_disjoint('POINT(-1 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon edge
SELECT 'disjoint153', ST_disjoint('POINT(0 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point in line with polygon edge
SELECT 'disjoint154', ST_disjoint('POINT(0 12)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'disjoint155', ST_disjoint(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex
SELECT 'disjoint156', ST_disjoint(ST_GeomFromText('POINT(521543 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - point within polygon
SELECT 'intersects100', ST_intersects('POINT(5 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon vertex
SELECT 'intersects101', ST_intersects('POINT(0 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point outside polygon
SELECT 'intersects102', ST_intersects('POINT(-1 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon edge
SELECT 'intersects103', ST_intersects('POINT(0 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point in line with polygon edge
SELECT 'intersects104', ST_intersects('POINT(0 12)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'intersects105', ST_intersects(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex
SELECT 'intersects106', ST_intersects(ST_GeomFromText('POINT(521543 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - point within polygon
SELECT 'intersects150', ST_intersects('POINT(5 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon vertex
SELECT 'intersects151', ST_intersects('POINT(0 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point outside polygon
SELECT 'intersects152', ST_intersects('POINT(-1 0)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point on polygon edge
SELECT 'intersects153', ST_intersects('POINT(0 5)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point in line with polygon edge
SELECT 'intersects154', ST_intersects('POINT(0 12)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'intersects155', ST_intersects(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex
SELECT 'intersects156', ST_intersects(ST_GeomFromText('POINT(521543 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - point within polygon
SELECT 'contains100', ST_contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'POINT(5 5)'::geometry);
-- PIP - point on vertex of polygon
SELECT 'contains101', ST_contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'POINT(0 0)'::geometry);
-- PIP - point outside polygon
SELECT 'contains102', ST_contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'POINT(-1 0)'::geometry);
-- PIP - point on edge of polygon
SELECT 'contains103', ST_contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'POINT(0 5)'::geometry);
-- PIP - point in line with polygon edge
SELECT 'contains104', ST_contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'POINT(0 12)'::geometry);
-- PIP - point vertically aligned with polygon vertex 
SELECT 'contains105', ST_contains(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631), ST_GeomFromText('POINT(521513 5377804)', 32631));
-- PIP - repeated vertex 
SELECT 'contains106', ST_contains(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631), ST_GeomFromText('POINT(521513 5377804)', 32631));
-- moved here from regress.sql
select 'within119', ST_within('LINESTRING(-1 -1, -1 101, 101 101, 101 -1)'::GEOMETRY,'BOX3D(0 0, 100 100)'::BOX3D);
select 'within120', ST_within('LINESTRING(-1 -1, -1 100, 101 100, 101 -1)'::GEOMETRY,'BOX3D(0 0, 100 100)'::BOX3D);
SELECT 'contains110', ST_Contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'LINESTRING(1 10, 9 10, 9 8)'::geometry);
SELECT 'contains111', ST_Contains('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry, 'LINESTRING(1 10, 10 10, 10 8)'::geometry);
SELECT 'within130', ST_Within('LINESTRING(1 10, 9 10, 9 8)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
SELECT 'within131', ST_Within('LINESTRING(1 10, 10 10, 10 8)'::geometry, 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
SELECT 'overlaps', ST_overlaps('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry,'POINT(5 5)'::geometry);
SELECT 'isvalid', ST_isvalid('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'::geometry);
SELECT 'isvalid', ST_isvalid('POLYGON((0 0, 0 10, 10 10, -5 10, 10 0, 0 0))'::geometry);
SELECT 'isvalid', ST_isvalid('GEOMETRYCOLLECTION EMPTY'::geometry);
SELECT 'intersection', ST_astext(ST_intersection('LINESTRING(0 10, 0 -10)'::geometry, 'LINESTRING(0 0, 1 1)'::geometry));
SELECT 'difference', ST_astext(ST_difference('LINESTRING(0 10, 0 -10)'::GEOMETRY, 'LINESTRING(0 2, 0 -2)'::GEOMETRY));
SELECT 'boundary', ST_astext(ST_boundary('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'::geometry));
SELECT 'symdifference', ST_astext(ST_symdifference('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'::geometry, 'LINESTRING(0 0, 20 20)'::geometry));
SELECT 'issimple', ST_issimple('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'::geometry);
SELECT 'equals', ST_equals('LINESTRING(0 0, 1 1)'::geometry, 'LINESTRING(1 1, 0 0)'::geometry);
WITH inp AS ( SELECT
 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'
::geometry as g )
SELECT 'pointonsurface', ST_Contains(g, ST_pointonsurface(g)) from inp;
SELECT 'centroid', ST_astext(ST_centroid('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0),(2 2, 2 4, 4 4, 4 2, 2 2))'::geometry));
SELECT 'exteriorring', ST_astext(ST_exteriorring(ST_PolygonFromText('POLYGON((52 18,66 23,73 9,48 6,52 18),(59 18,67 18,67 13,59 13,59 18))')));
SELECT 'polygonize_garray', ST_astext(ST_polygonize('{0102000000020000000000000000000000000000000000000000000000000024400000000000000000:0102000000020000000000000000002440000000000000000000000000000000000000000000000000:0102000000020000000000000000002440000000000000244000000000000000000000000000000000:0102000000020000000000000000002440000000000000244000000000000024400000000000000000:0102000000020000000000000000002440000000000000244000000000000000000000000000002440:0102000000020000000000000000000000000000000000244000000000000000000000000000002440:0102000000020000000000000000000000000000000000244000000000000024400000000000002440:0102000000020000000000000000000000000000000000244000000000000000000000000000000000:0102000000020000000000000000000000000000000000244000000000000024400000000000000000}'::geometry[]));

SELECT 'polygonize_garray', ST_astext(ST_geometryn(ST_polygonize('{LINESTRING(0 0, 10 0):LINESTRING(10 0, 10 10):LINESTRING(10 10, 0 10):LINESTRING(0 10, 0 0)}'::geometry[]), 1));

select 'linemerge149', ST_asewkt(ST_linemerge('GEOMETRYCOLLECTION(LINESTRING(0 0, 1 1), LINESTRING(4 4, 1 1), LINESTRING(-5 -5, 0 0))'::geometry));

--- postgis-devel/2005-December/001784.html
select 'intersects', ST_intersects(
   ST_polygonfromtext('POLYGON((0.0 0.0,1.0 0.0,1.0 1.0,1.0 0.0,0.0 0.0))'),
      ST_polygonfromtext('POLYGON((0.0 2.0,1.0 2.0,1.0 3.0,0.0 3.0,0.0 2.0))')
      );

select 'ST_GeometryN', ST_asewkt(ST_GeometryN('LINESTRING(0 0, 1 1)'::geometry, 1));
select 'ST_NumGeometries', ST_NumGeometries('LINESTRING(0 0, 1 1)'::geometry);
select 'ST_Union1', ST_AsText(ST_Union(ARRAY['POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'::geometry, 'POLYGON((0.5 0.5, 1.5 0.5, 1.5 1.5, 0.5 1.5, 0.5 0.5))'::geometry]));
select 'ST_StartPoint1',ST_AsText(ST_StartPoint('LINESTRING(0 0, 1 1, 2 2)'::geometry));
select 'ST_EndPoint1', ST_AsText(ST_Endpoint('LINESTRING(0 0, 1 1, 2 2)'::geometry));
select 'ST_PointN1', ST_AsText(ST_PointN('LINESTRING(0 0, 1 1, 2 2)'::geometry,2));
select 'ST_PointN2', ST_AsText(ST_PointN('LINESTRING(0 0, 1 1, 2 2)'::geometry,3));
select 'ST_PointN3',  ST_AsText(ST_PointN('LINESTRING(0 0, 1 1, 2 2)'::geometry,4));
select 'ST_PointN4', ST_AsText(ST_PointN('LINESTRING(0 0, 1 1, 2 2)'::geometry,0));
select 'ST_PointN5', ST_AsText(ST_PointN('LINESTRING(0 0, 1 1, 2 2)'::geometry,1));
select 'ST_PointN6', ST_AsText(ST_PointN('POLYGON((0 0, 1 1, 0 1, 0 0))'::geometry,1));

-- issues with EMPTY --
select 'ST_Buffer(empty)', ST_AsText(ST_Buffer('POLYGON EMPTY'::geometry, 0.5));
