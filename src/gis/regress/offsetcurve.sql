\set VERBOSITY terse
set client_min_messages to NOTICE;
SELECT 't0', ST_OffsetCurve('POINT(0 0)', 10);
SELECT 't0', ST_AsEWKT(ST_OffsetCurve('SRID=42;LINESTRING(0 0, 10 0)', 0));
SELECT 't1', ST_AsEWKT(ST_OffsetCurve('SRID=42;LINESTRING(0 0, 10 0)', 10));
SELECT 't2', ST_AsEWKT(ST_OffsetCurve('SRID=42;LINESTRING(0 0, 10 0)', -10));
SELECT 't3', ST_AsEWKT(ST_OffsetCurve('SRID=42;LINESTRING(10 0, 0 0)', 10));
SELECT 't4', ST_AsEWKT(ST_OffsetCurve('SRID=42;LINESTRING(10 0, 0 0)', -10));
SELECT 't5', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 10 10)', -10),
1));
SELECT 't5b', ST_AsEWKT(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 10 10)', 10));
SELECT 't6', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 10 10)', -10,
 'quad_segs=2'),
1));
SELECT 't7', ST_AsEWKT(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 10 10)', -10,
 'join=bevel')
);
SELECT 't8', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 10 10)', -10,
 'quad_segs=2 join=mitre'),
1));
SELECT 't9', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 5 10)', -10,
 'quad_segs=2 join=mitre mitre_limit=1'),
1));
SELECT 't10', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 5 10)', 2,
 'quad_segs=2 join=mitre mitre_limit=1'),
1));
SELECT 't10b', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'SRID=42;LINESTRING(0 0, 10 0, 5 10)', 2,
 'quad_segs=2 join=miter miter_limit=1'),
1));
SELECT 't11', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'LINESTRING(36 38,38 35,41 34,42 33,45 32,47 28,50 28,52 32,57 33)', 2,
 'join=mitre'),
0.2));
SELECT 't12', ST_AsEWKT(ST_SnapToGrid(ST_OffsetCurve(
 'LINESTRING(36 38,38 35,41 34,42 33,45 32,47 28,50 28,52 32,57 33)', -2,
 'join=mitre'),
0.2));
SELECT 't13', ST_AsEWKT(ST_OffsetCurve(
 'LINESTRING(0 0,0 20, 10 20, 10 10, 0 10)', 2,
 'join=mitre'
));
SELECT 't14', ST_AsEWKT(ST_OffsetCurve(
 'LINESTRING(0 0,0 20, 10 20, 10 10, 0 10)', -2,
 ''
));
