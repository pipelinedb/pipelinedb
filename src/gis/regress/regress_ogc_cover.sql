---
--- Tests for GEOS/JTS implemented functions
--- supported by GEOS 3.0 and upwards
---

SELECT 'covers100', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'LINESTRING(1 10, 9 10, 9 8)');
SELECT 'covers101', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'LINESTRING(1 10, 10 10, 10 8)');
-- PIP - point within polygon
SELECT 'covers102', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(5 5)');
-- PIP - point on vertex of polygon
SELECT 'covers103', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 0)');
-- PIP - point outside polygon
SELECT 'covers104', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(-1 0)');
-- PIP - point on edge of polygon
SELECT 'covers105', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 5)');
-- PIP - point in line with polygon edge
SELECT 'covers106', ST_Covers('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', 'POINT(0 12)');
-- PIP - point vertically aligned with polygon vertex 
SELECT 'covers107', ST_Covers(ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631), ST_GeomFromText('POINT(521513 5377804)', 32631));
-- PIP - repeated vertex 
SELECT 'covers108', ST_Covers(ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631), ST_GeomFromText('POINT(521513 5377804)', 32631));
-- CoveredBy cases
SELECT 'coveredby100', ST_CoveredBy('LINESTRING(1 10, 9 10, 9 8)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
SELECT 'coveredby101', ST_CoveredBy('LINESTRING(1 10, 10 10, 10 8)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point within polygon
SELECT 'coveredby102', ST_CoveredBy('POINT(5 5)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point on vertex of polygon
SELECT 'coveredby103', ST_CoveredBy('POINT(0 0)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point outside polygon
SELECT 'coveredby104', ST_CoveredBy('POINT(-1 0)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point on edge of polygon
SELECT 'coveredby105', ST_CoveredBy('POINT(0 5)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point in line with polygon edge
SELECT 'coveredby106', ST_CoveredBy('POINT(0 12)', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
-- PIP - point vertically aligned with polygon vertex 
SELECT 'coveredby107', ST_CoveredBy(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));
-- PIP - repeated vertex 
SELECT 'coveredby108', ST_CoveredBy(ST_GeomFromText('POINT(521513 5377804)', 32631), ST_GeomFromText('POLYGON((521526 5377783, 521482 5377811, 521481 5377811, 521494 5377832, 521539 5377804, 521526 5377783))', 32631));

