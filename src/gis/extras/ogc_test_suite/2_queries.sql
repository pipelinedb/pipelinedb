-- FILE: sqltque.sql 10/01/98
--
--       1         2         3         4         5         6         7         8
--345678901234567890123456789012345678901234567890123456789012345678901234567890
--//////////////////////////////////////////////////////////////////////////////
--
-- Copyright 1998, Open GIS Consortium, Inc.
--
-- The material in this document details an Open GIS Consortium Test Suite in
-- accordance with a license that your organization has signed. Please refer
-- to http://www.opengis.org/testing/ to obtain a copy of the general license
-- (it is part of the Conformance Testing Agreement). 
--
--//////////////////////////////////////////////////////////////////////////////
--
-- OpenGIS Simple Features for SQL (Types and Functions) Test Suite Software
-- 
-- This file "sqltque.sql" is part 2 of a two part standardized test
-- suite in SQL script form. The other file that is required for this test
-- suite, "sqltsch.sql", one additional script is provided ("sqltcle.sql") that
-- performs cleanup operations between test runs, and other documents that 
-- describe the OGC Conformance Test Program are available via the WWW at 
-- http://www.opengis.org/testing/index.htm
--
-- NOTE CONCERNING INFORMATION ON CONFORMANCE TESTING AND THIS TEST SUITE
-- ----------------------------------------------------------------------
--
-- Organizations wishing to submit product for conformance testing should
-- access the above WWW site to discover the proper procedure for obtaining
-- a license to use the OpenGIS(R) certification mark associated with this
-- test suite.
--
--
-- NOTE CONCERNING TEST SUITE ADAPTATION
-- -------------------------------------
--
-- OGC recognizes that many products will have to adapt this test suite to
-- make it work properly. OGC has documented the allowable adaptations within
-- this test suite where possible. Other information about adaptations may be
-- discovered in the Test Suite Guidelines document for this test suite.
-- 
-- PLEASE NOTE THE OGC REQUIRES THAT ADAPTATIONS ARE FULLY DOCUMENTED USING
-- LIBERAL COMMENT BLOCKS CONFORMING TO THE FOLLOWING FORMAT:
--
-- -- !#@ ADAPTATION BEGIN 
-- explanatory text goes here
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- original sql goes here
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
-- adated sql goes here
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END 
--
--//////////////////////////////////////////////////////////////////////////////
--
-- BEGIN TEST SUITE CODE
--
--//////////////////////////////////////////////////////////////////////////////
--
-- Please refer to the Test Suite Guidelines for this test suite for further
-- information concerning the test data. The actual data is created by 
-- executing "sqltsch.sql"
--
--//////////////////////////////////////////////////////////////////////////////
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING THE TABLE/VIEW STRUCTURE
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T1	
-- GEOMETRY_COLUMNS table/view is created/updated properly	
-- For this test we will check to see that all of the feature tables are 
-- represented by entries in the GEOMETRY_COLUMNS table/view
--
-- ANSWER: lakes, road_segments, divided_routes, buildings, forests, bridges, 
--         named_places, streams, ponds, map_neatlines
-- *** ADAPTATION ALERT ***	
-- Since there are no quotes around the table names in the CREATE TABLEs,
-- they will be converted to upper case in many DBMSs, and therefore, the 
-- answer to this query may be:
-- ANSWER: LAKES, ROAD_SEGMENTS, DIVIDED_ROUTES, BUILDINGS, FORESTS, BRIDGES, 
--         NAMED_PLACES, STREAMS, PONDS, MAP_NEATLINES
-- *** ADAPTATION ALERT ***	
-- If the implementer made the adaptation concerning the buildings table
-- in sqltsch.sql, then the answer here may differ slightly.
--	
--
--================================
--
SELECT f_table_name
FROM geometry_columns;
--
--
--================================
-- Conformance Item T2	
-- GEOMETRY_COLUMNS table/view is created/updated properly	
-- For this test we will check to see that the correct geometry columns for the 
-- streams table is represented in the GEOMETRY_COLUMNS table/view
--
-- ANSWER: centerline
-- *** ADAPTATION ALERT ***	
-- Since there are no quotes around the table name, streams, in it's CREATE TABLE,
-- it will be converted to upper case in many DBMSs, and therefore, the WHERE 
-- clause may have to be f_table_name = 'STREAMS'.
--
--================================
--
SELECT f_geometry_column
FROM geometry_columns
WHERE f_table_name = 'streams';
--
--
--================================
-- Conformance Item T3	
-- GEOMETRY_COLUMNS table/view is created/updated properly	
-- For this test we will check to see that the correct coordinate dimension 
-- for the streams table is represented in the GEOMETRY_COLUMNS table/view
--
-- ANSWER: 2	
-- *** ADAPTATION ALERT ***	
-- Since there are no quotes around the table name, streams, in it's CREATE TABLE,
-- it will be converted to upper case in many DBMSs, and therefore, the WHERE 
-- clause may have to be f_table_name = 'STREAMS'.
--
--================================
--
SELECT coord_dimension
FROM geometry_columns
WHERE f_table_name = 'streams';
--
--
--================================
-- Conformance Item T4	
-- GEOMETRY_COLUMNS table/view is created/updated properly	
-- For this test we will check to see that the correct value of srid for 
-- the streams table is represented in the GEOMETRY_COLUMNS table/view
--
-- ANSWER: 101	
-- *** ADAPTATION ALERT ***	
-- Since there are no quotes around the table name, streams, in it's CREATE TABLE,
-- it will be converted to upper case in many DBMSs, and therefore, the WHERE 
-- clause may have to be f_table_name = 'STREAMS'.
--
--================================
--
SELECT srid
FROM geometry_columns
WHERE f_table_name = 'streams';
--
--
--================================
-- Conformance Item T5
-- SPATIAL_REF_SYS table/view is created/updated properly
-- For this test we will check to see that the correct value of srtext is 
-- represented in the SPATIAL_REF_SYS table/view
--
-- ANSWER: 'PROJCS["UTM_ZONE_14N", GEOGCS["World Geodetic System 72", 
--           DATUM["WGS_72",  SPHEROID["NWL_10D", 6378135, 298.26]], 
--           PRIMEM["Greenwich", 0], UNIT["Meter", 1.0]], 
--           PROJECTION["Traverse_Mercator"], PARAMETER["False_Easting", 500000.0], 
--           PARAMETER["False_Northing", 0.0], PARAMETER["Central_Meridian", -99.0], 
--           PARAMETER["Scale_Factor", 0.9996], PARAMETER["Latitude_of_origin", 0.0], 
--           UNIT["Meter", 1.0]]'	
--
--================================
--
SELECT srtext
FROM SPATIAL_REF_SYS
WHERE SRID = 101;
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.10.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T6	
-- Dimension(g Geometry) : Integer
-- For this test we will determine the dimension of Blue Lake.
--
-- ANSWER: 2
--
--================================
--
SELECT Dimension(shore) 
FROM lakes 
WHERE name = 'Blue Lake';
--
--
--================================
-- Conformance Item T7	
-- GeometryType(g Geometry) : String
-- For this test we will determine the type of Route 75.
--
-- ANSWER: 9 (which corresponds to 'MULTILINESTRING')
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script incorrectly references the 'lakes' table
-- instead of the 'divided_routes' table where 'Route 75' 
-- appears.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT GeometryType(centerlines) 
-- FROM lakes 
-- WHERE name = 'Route 75';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT GeometryType(centerlines) 
FROM divided_routes 
WHERE name = 'Route 75';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T8	
-- AsText(g Geometry) : String
-- For this test we will determine the WKT representation of Goose Island.
--
-- ANSWER: 'POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )'
--
--================================
--
SELECT AsText(boundary) 
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T9	
-- AsBinary(g Geometry) : Blob
-- For this test we will determine the WKB representation of Goose Island.
-- We will test by applying AsText to the result of PolygonFromText to the 
-- result of AsBinary.
--
-- ANSWER: 'POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )'
--
--================================
--
SELECT AsText(PolygonFromWKB(AsBinary(boundary))) 
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T10	
-- SRID(g Geometry) : Integer
-- For this test we will determine the SRID of Goose Island.
--
-- ANSWER: 101
--
--================================
--
SELECT SRID(boundary) 
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T11	
-- IsEmpty(g Geometry) : Integer
-- For this test we will determine whether the geometry of a 
-- segment of Route 5 is empty.
--
-- ANSWER: 0
-- *** Adaptation Alert ***
-- If the implementer provides IsEmpty as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: FALSE or 'f'
--
--================================
--
SELECT IsEmpty(centerline) 
FROM road_segments 
WHERE name = 'Route 5' AND aliases = 'Main Street';
--
--================================
-- Conformance Item T12	
-- IsSimple(g Geometry) : Integer
-- For this test we will determine whether the geometry of a 
-- segment of Blue Lake is simple.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides IsSimple as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT IsSimple(shore) 
FROM lakes 
WHERE name = 'Blue Lake';
--
--================================
-- Conformance Item T13	
-- Boundary(g Geometry) : Geometry
-- For this test we will determine the boundary of Goose Island.
-- NOTE: The boundary result is as defined in 3.12.3.2 of 96-015R1.
-- 
-- ANSWER: 'LINESTRING( 67 13, 67 18, 59 18, 59 13, 67 13 )'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script includes extraneous parenthesis around the 
-- 'boundary' column.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT AsText(Boundary((boundary)) 
-- FROM named_places 
-- WHERE name = 'Goose Island';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(Boundary(boundary)) 
FROM named_places 
WHERE name = 'Goose Island';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T14	
-- Envelope(g Geometry) : Geometry
-- For this test we will determine the envelope of Goose Island.
-- 
-- ANSWER: 'POLYGON( ( 59 13, 59 18, 67 18, 67 13, 59 13) )'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script includes extraneous parenthesis around the
-- 'boundary' column.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT AsText(Envelope((boundary)) 
-- FROM named_places 
-- WHERE name = 'Goose Island';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(Envelope(boundary)) 
FROM named_places 
WHERE name = 'Goose Island';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.11.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T15	
-- X(p Point) : Double Precision
-- For this test we will determine the X coordinate of Cam Bridge.
-- 
-- ANSWER: 44.00
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script reference to 'Bridges' is not correct, the 
-- attribute value is 'Cam Bridge'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT X(position) 
-- FROM bridges 
-- WHERE name = 'Bridges';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT X(position) 
FROM bridges 
WHERE name = 'Cam Bridge';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T16	
-- Y(p Point) : Double Precision
-- For this test we will determine the Y coordinate of Cam Bridge.
-- 
-- ANSWER: 31.00
--
--================================
--
SELECT Y(position) 
FROM bridges 
WHERE name = 'Cam Bridge';
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.12.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T17	
-- StartPoint(c Curve) : Point
-- For this test we will determine the start point of road segment 102.
--
-- ANSWER: 'POINT( 0 18 )'
--
--================================
--
SELECT AsText(StartPoint(centerline))
FROM road_segments 
WHERE fid = 102;
--
--================================
-- Conformance Item T18	
-- EndPoint(c Curve) : Point
-- For this test we will determine the end point of road segment 102.
--
-- ANSWER: 'POINT( 44 31 )'
--
--================================
--
SELECT AsText(EndPoint(centerline))
FROM road_segments 
WHERE fid = 102;
--
--================================
-- Conformance Item T19	
-- IsClosed(c Curve) : Integer
-- For this test we will determine the boundary of Goose Island.
-- 
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides IsClosed as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT IsClosed(Boundary(boundary)) 
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T20	
-- IsRing(c Curve) : Integer
-- For this test we will determine the boundary of Goose Island.
-- 
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides IsRing as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT IsRing(Boundary(boundary)) 
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T21	
-- Length(c Curve) : Double Precision
-- For this test we will determine the length of road segment 106.
--
-- ANSWER: 26.00 (meters)
--
--================================
--
SELECT Length(centerline)
FROM road_segments 
WHERE fid = 106;
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.13.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T22	
-- NumPoints(l LineString) : Integer
-- For this test we will determine the number of points in road segment 102.
--
-- ANSWER: 5
--
--================================
--
SELECT NumPoints(centerline)
FROM road_segments 
WHERE fid = 102;
--
--================================
-- Conformance Item T23	
-- PointN(l LineString, n Integer) : Point
-- For this test we will determine the 1st point in road segment 102.
--
-- ANSWER: 'POINT( 0 18 )'
--
--================================
--
SELECT AsText(PointN(centerline, 1))
FROM road_segments 
WHERE fid = 102;
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.14.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T24	
-- Centroid(s Surface) : Point
-- For this test we will determine the centroid of Goose Island.
--
-- ANSWER: 'POINT( 63 15.5 )'
--
--================================
--
SELECT AsText(Centroid(boundary))
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T25	
-- PointOnSurface(s Surface) : Point
-- For this test we will determine a point on Goose Island.
-- NOTE: For this test we will have to uses the Contains function
--       (which we don't test until later).
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Contains as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT Contains(boundary, PointOnSurface(boundary))
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T26	
-- Area(s Surface) : Double Precision
-- For this test we will determine the area of Goose Island.
--
-- ANSWER: 40.00 (square meters)
--
--================================
--
SELECT Area(boundary)
FROM named_places 
WHERE name = 'Goose Island';
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.15.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T27	
-- ExteriorRing(p Polygon) : LineString
-- For this test we will determine the exteroir ring of Blue Lake.
--
-- ANSWER: 'LINESTRING(52 18, 66 23, 73  9, 48  6, 52 18)'
--
--================================
--
SELECT AsText(ExteriorRing(shore))
FROM lakes 
WHERE name = 'Blue Lake';
--
--================================
-- Conformance Item T28	
-- NumInteriorRings(p Polygon) : Integer
-- For this test we will determine the number of interior rings of Blue Lake.
--
-- ANSWER: 1
--
--================================
--
SELECT NumInteriorRings(shore)
FROM lakes 
WHERE name = 'Blue Lake';
--
--================================
-- Conformance Item T29	
-- InteriorRingN(p Polygon, n Integer) : LineString
-- For this test we will determine the first interior ring of Blue Lake.
--
-- ANSWER: 'LINESTRING(59 18, 67 18, 67 13, 59 13, 59 18)'
--
--================================
--
SELECT AsText(InteriorRingN(shore, 1))
FROM lakes 
WHERE name = 'Blue Lake';
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.16.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T30	
-- NumGeometries(g GeometryCollection) : Integer
-- For this test we will determine the number of geometries in Route 75.
--
-- ANSWER: 2
--
--================================
--
SELECT NumGeometries(centerlines)
FROM divided_routes 
WHERE name = 'Route 75';
--
--================================
-- Conformance Item T31	
-- GeometryN(g GeometryCollection, n Integer) : Geometry
-- For this test we will determine the second geometry in Route 75.
--
-- ANSWER: 'LINESTRING( 16 0, 16 23, 16 48 )'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT GeometryN(centerlines, 2)
-- FROM divided_routes 
-- WHERE name = 'Route 75';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(GeometryN(centerlines, 2))
FROM divided_routes 
WHERE name = 'Route 75';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.17.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T32	
-- IsClosed(mc MultiCurve) : Integer
-- For this test we will determine if the geometry of Route 75 is closed.
--
-- ANSWER: 0
-- *** Adaptation Alert ***
-- If the implementer provides IsClosed as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: FALSE or 'f'
--
--================================
--
SELECT IsClosed(centerlines)
FROM divided_routes 
WHERE name = 'Route 75';
--
--================================
-- Conformance Item T33	
-- Length(mc MultiCurve) : Double Precision
-- For this test we will determine the length of Route 75.
-- NOTE: This makes no semantic sense in our example...
--
-- ANSWER: 96.00 (meters)
--
--================================
--
SELECT Length(centerlines)
FROM divided_routes 
WHERE name = 'Route 75';
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.18.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T34	
-- Centroid(ms MultiSurface) : Point
-- For this test we will determine the centroid of the ponds.
--
-- ANSWER: 'POINT( 25 42 )'
--
--================================
--
SELECT AsText(Centroid(shores))
FROM ponds 
WHERE fid = 120;
--
--================================
-- Conformance Item T35	
-- PointOnSurface(ms MultiSurface) : Point
-- For this test we will determine a point on the ponds.
-- NOTE: For this test we will have to uses the Contains function
--       (which we don't test until later).
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Contains as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT Contains(shores, PointOnSurface(shores))
FROM ponds 
WHERE fid = 120;
--
--================================
-- Conformance Item T36	
-- Area(ms MultiSurface) : Double Precision
-- For this test we will determine the area of the ponds.
--
-- ANSWER: 8.00 (square meters)
--
--================================
--
SELECT Area(shores)
FROM ponds 
WHERE fid = 120;
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.19.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T37	
-- Equals(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of Goose Island is equal
-- to the same geometry as consructed from it's WKT representation.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Equals as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT Equals(boundary, PolygonFromText('POLYGON( ( 67 13, 67 18, 59 18, 59 13, 67 13) )',1))
FROM named_places 
WHERE name = 'Goose Island';
--
--================================
-- Conformance Item T38	
-- Disjoint(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of Route 75 is disjoint
-- from the geometry of Ashton.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Disjoint as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
SELECT Disjoint(centerlines, boundary)
FROM divided_routes, named_places 
WHERE divided_routes.name = 'Route 75' AND named_places.name = 'Ashton';
--
--================================
-- Conformance Item T39	
-- Touch(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of Cam Stream touches
-- the geometry of Blue Lake.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Touch as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- The test script attempts to test the 'Touch' function, but the 
-- specification document uses 'Touches' as the function name.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Touch(centerline, shore)
-- FROM streams, lakes 
-- WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Touches(centerline, shore)
FROM streams, lakes 
WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T40	
-- Within(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of the house at 215 Main Street
-- is within Ashton.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Within as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script reverses the correct order of arguments to 'Within()'. 
-- Specification says 'Within(g1,g2) is 'TRUE if g1 is completely 
-- contained in g2' and test explanation says we are checking if 
-- the house (g1, footprint) is within Ashton (g2, boundary).
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Within(boundary, footprint)
-- FROM named_places, buildings 
-- WHERE named_places.name = 'Ashton' AND buildings.address = '215 Main Street';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Within(footprint, boundary)
FROM named_places, buildings 
WHERE named_places.name = 'Ashton' AND buildings.address = '215 Main Street';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T41	
-- Overlap(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of Green Forest overlaps 
-- the geometry of Ashton.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Overlap as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script uses 'Overlap()' as the function name and specification
-- gives 'Overlaps()' as the function name.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Overlap(forest.boundary, named_places.boundary)
-- FROM forests, named_places 
-- WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Overlaps(forests.boundary, named_places.boundary)
FROM forests, named_places 
WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T42	
-- Cross(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of road segment 102 crosses 
-- the geometry of Route 75.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Cross as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script uses 'Cross()' as the function name and specification
-- gives 'Crosses()' as the function name.
-- Test script references 'road_segment' table and the correct table
-- name is 'road_segments'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Cross(road_segment.centerline, divided_routes.centerlines)
-- FROM road_segment, divided_routes 
-- WHERE road_segment.fid = 102 AND divided_routes.name = 'Route 75';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Crosses(road_segments.centerline, divided_routes.centerlines)
FROM road_segments, divided_routes 
WHERE road_segments.fid = 102 AND divided_routes.name = 'Route 75';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T43	
-- Intersects(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of road segment 102 intersects 
-- the geometry of Route 75.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Intersects as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script references 'road_segment' table and the correct table
-- name is 'road_segments'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Intersects(road_segment.centerline, divided_routes.centerlines)
-- FROM road_segment, divided_routes 
-- WHERE road_segment.fid = 102 AND divided_routes.name = 'Route 75';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Intersects(road_segments.centerline, divided_routes.centerlines)
FROM road_segments, divided_routes 
WHERE road_segments.fid = 102 AND divided_routes.name = 'Route 75';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T44	
-- Contains(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine if the geometry of Green Forest contains 
-- the geometry of Ashton.
--
-- ANSWER: 0
-- *** Adaptation Alert ***
-- If the implementer provides Contains as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: FALSE or 'f'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script references 'forest' table and the correct table
-- name is 'forests'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Contains(forest.boundary, named_places.boundary)
-- FROM forests, named_places 
-- WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Contains(forests.boundary, named_places.boundary)
FROM forests, named_places 
WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T45	
-- Relate(g1 Geometry, g2 Geometry, PatternMatrix String) : Integer
-- For this test we will determine if the geometry of Green Forest relates to 
-- the geometry of Ashton using the pattern "TTTTTTTTT".
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Relate as a boolean function, instead of as
-- an INTEGER function, then:
-- ANSWER: TRUE or 't'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script references 'forest' table and the correct table
-- name is 'forests'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Relate(forest.boundary, named_places.boundary, 'TTTTTTTTT')
-- FROM forests, named_places 
-- WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT Relate(forests.boundary, named_places.boundary, 'TTTTTTTTT')
FROM forests, named_places 
WHERE forests.name = 'Green Forest' AND named_places.name = 'Ashton';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.20.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T46	
-- Distance(g1 Geometry, g2 Geometry) : Double Precision
-- For this test we will determine the distance between Cam Bridge and Ashton.
--
-- ANSWER: 12 (meters)
--
--================================
--
SELECT Distance(position, boundary)
FROM bridges, named_places 
WHERE bridges.name = 'Cam Bridge' AND named_places.name = 'Ashton';
--
--
--
--//////////////////////////////////////////////////////////////////////////////
--
-- QUERIES TESTING FUNCTIONS IN SECTION 3.2.21.2
--
--//////////////////////////////////////////////////////////////////////////////
--
--================================
-- Conformance Item T47	
-- Intersection(g1 Geometry, g2 Geometry) : Geometry
-- For this test we will determine the intersection between Cam Stream and 
-- Blue Lake.
--
-- ANSWER: 'POINT( 52 18 )'
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Intersection(centerline, shore)
-- FROM streams, lakes 
-- WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(Intersection(centerline, shore))
FROM streams, lakes 
WHERE streams.name = 'Cam Stream' AND lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T48	
-- Difference(g1 Geometry, g2 Geometry) : Geometry
-- For this test we will determine the difference between Ashton and 
-- Green Forest.
--
-- ANSWER: 'POLYGON( ( 56 34, 62 48, 84 48, 84 42, 56 34) )'
-- NOTE: The order of the vertices here is arbitrary.
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- Note that the return geometry is the same as the official
-- answer but with a different start point.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Difference(named_places.boundary, forests.boundary)
-- FROM named_places, forests 
-- WHERE named_places.name = 'Ashton' AND forests.name = 'Green Forest';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(Difference(named_places.boundary, forests.boundary))
FROM named_places, forests 
WHERE named_places.name = 'Ashton' AND forests.name = 'Green Forest';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T49	
-- Union(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine the union of Blue Lake and Goose Island 
--
-- ANSWER: 'POLYGON((52 18,66 23,73 9,48 6,52 18))'
-- NOTE: The outer ring of Blue Lake is the answer.
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- Test script uses 'Ashton' as the place name where it means
-- to use 'Goose Island'.
-- Specification uses 'Union()' as a function name, but UNION
-- is a SQL reserved work.  Function name adapted to 'GeomUnion()'
-- for out implementation.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT Union(shore, boundary)
-- FROM lakes, named_places 
-- WHERE lakes.name = 'Blue Lake' AND named_places.name = Ashton';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(GeomUnion(shore, boundary))
FROM lakes, named_places 
WHERE lakes.name = 'Blue Lake' AND named_places.name = 'Goose Island';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T50	
-- SymmetricDifference(g1 Geometry, g2 Geometry) : Integer
-- For this test we will determine the symmetric difference of Blue Lake 
-- and Goose Island 
--
-- ANSWER: 'POLYGON((52 18,66 23,73 9,48 6,52 18))'
-- NOTE: The outer ring of Blue Lake is the answer.
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- Test script uses 'Ashton' as the place name where it means
-- to use 'Goose Island'.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT SymmetricDifference(shore, boundary)
-- FROM lakes, named_places 
-- WHERE lakes.name = 'Blue Lake' OR named_places.name = 'Ashton';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(SymmetricDifference(shore, boundary))
FROM lakes, named_places 
WHERE lakes.name = 'Blue Lake' AND named_places.name = 'Goose Island';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T51	
-- Buffer(g Geometry, d Double Precision) : Geometry
-- For this test we will make a 15 meter buffer about Cam Bridge.
-- NOTE: This test we count the number of buildings contained in
--       the buffer that is generated. This test only works because
--       we have a single bridge record, two building records, and
--       we selected the buffer size such that only one of the buildings
--       is contained in the buffer.
--
-- ANSWER: 1
-- *** Adaptation Alert ***
-- If the implementer provides Contains as a boolean function, instead of as
-- an INTEGER function, then the WHERE clause should be:
-- WHERE Contains(Buffer(bridges.position, 15.0), buildings.footprint) = 'TRUE';
--   - or -
-- WHERE Contains(Buffer(bridges.position, 15.0), buildings.footprint) = 't';
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Our boolean function implementations return actual boolean values,
-- so no further logical comparison (to 1 or 't') is required.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT count(*)
-- FROM buildings, bridges
-- WHERE Contains(Buffer(bridges.position, 15.0), buildings.footprint) = 1;
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT count(*)
FROM buildings, bridges
WHERE Contains(Buffer(bridges.position, 15.0), buildings.footprint);
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END
--
--================================
-- Conformance Item T52	
-- ConvexHull(g Geometry) : Geometry
-- For this test we will determine the convex hull of Blue Lake 
--
-- ANSWER: 'POLYGON((52 18,66 23,73 9,48 6,52 18))'
-- NOTE: The outer ring of Blue Lake is the answer.
--
--================================
--
-- !#@ ADAPTATION BEGIN
-- Test script does not wrap a geometry-returning function in
-- AsText(), so there is no guarantee that the return string
-- will match the official answer.
-- Note that the return geometry is the same as the official
-- answer but with a different start point.
-- ---------------------
-- -- BEGIN ORIGINAL SQL
-- ---------------------
-- SELECT ConvexHull(shore)
-- FROM lakes
-- WHERE lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ORIGINAL SQL
-- ---------------------
-- -- BEGIN ADAPTED  SQL
-- ---------------------
SELECT AsText(ConvexHull(shore))
FROM lakes
WHERE lakes.name = 'Blue Lake';
-- ---------------------
-- -- END   ADAPTED  SQL
-- ---------------------
-- -- !#@ ADAPTATION END


--
--
--
-- end sqltque.sql
