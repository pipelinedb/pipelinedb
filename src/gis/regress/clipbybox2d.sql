-- Overlap
SELECT ST_ClipByBox2d(ST_MakeEnvelope(0,0,10,10),ST_MakeEnvelope(5,5,20,20))::box2d;
-- Geom covers rect
SELECT ST_ClipByBox2d(ST_MakeEnvelope(0,0,10,10),ST_MakeEnvelope(5,5,8,8))::box2d;
-- Geom within rect
SELECT ST_ClipByBox2d(ST_MakeEnvelope(2,2,8,8),ST_MakeEnvelope(0,0,10,10))::box2d;
-- Multipoint with point inside, point outside and point on boundary
SELECT ST_AsText(ST_ClipByBox2d('MULTIPOINT(-1 -1, 0 0, 2 2)'::geometry,ST_MakeEnvelope(0,0,10,10)));
-- Invalid polygon (bow-tie) -- ST_Intersection throws an exception here
SELECT ST_AsText(ST_ClipByBox2d('POLYGON((0 0, 10 10, 0 10, 10 0, 0 0))', ST_MakeEnvelope(2,2,8,8)));
-- Invalid polygon (lineal self-intersection) -- ST_Intersection returns a collection
SELECT ST_AsText(ST_ClipByBox2d('POLYGON((0 0,5 4,5 6,0 10,10 10,5 6,5 4,10 0,0 0))', ST_MakeEnvelope(2,2,10,5)));
-- Invalid polygon (non-closed ring) -- ST_Intersection raises an exception
-- The HEXWKB represents 'POLYGON((0 0, 10 0, 10 10, 0 10))'
SELECT ST_AsText(ST_ClipByBox2d(
  '0103000000010000000400000000000000000000000000000000000000000000000000244000000000000000000000000000002440000000000000244000000000000000000000000000002440'
  , ST_MakeEnvelope(2,2,5,5)));
-- Geometry disjoint from box, from a table
-- See http://trac.osgeo.org/postgis/ticket/2950
CREATE TEMPORARY TABLE t AS SELECT
'SRID=3857;POLYGON((41 20,41 0,21 0,1 20,1 40,21 40,41 20))'
::geometry g;
SELECT ST_AsEWKT(ST_ClipByBox2d(g, ST_MakeEnvelope(-20,-20,-10,-10))) FROM t;
-- See http://trac.osgeo.org/postgis/ticket/2954
SELECT ST_AsEWKT(ST_ClipByBox2D('SRID=4326;POINT(0 0)','BOX3D(-1 -1,1 1)'::box3d::box2d));
