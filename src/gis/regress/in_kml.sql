--
-- GeomFromKML regression test
-- Written by Olivier Courtin - Oslandia
--


--
-- spatial_ref_sys datas
--

-- EPSG 4326 : WGS 84
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (4326,'EPSG',4326,'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]','+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ');


-- Empty Geometry
SELECT 'empty_geom', ST_AsEWKT(ST_GeomFromKML(NULL));

--
-- XML
--

-- ERROR: Empty String
SELECT 'xml_1', ST_AsEWKT(ST_GeomFromKML(''));

-- ERROR: Not well formed XML
SELECT 'xml_2', ST_AsEWKT(ST_GeomFromKML('<foo>'));

-- ERROR: Not a KML Geometry
SELECT 'xml_3', ST_AsEWKT(ST_GeomFromKML('<foo/>'));



--
-- Point
--

-- 1 Point
SELECT 'point_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- See http://trac.osgeo.org/postgis/ticket/2372
SELECT 'point_1a', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1 ,2</kml:coordinates></kml:Point>'));
SELECT 'point_1b', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1, 2</kml:coordinates></kml:Point>'));
SELECT 'point_1c', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates> 1,2</kml:coordinates></kml:Point>'));
SELECT 'point_1d', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates> 1,2 </kml:coordinates></kml:Point>'));

-- ERROR: 2 points
SELECT 'point_error_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2 3,4</kml:coordinates></kml:Point>'));

-- ERROR: empty point
SELECT 'point_error_2', ST_AsEWKT(ST_GeomFromKML('<kml:Point></kml:Point>'));



--
-- LineString
--

-- 2 Points
SELECT 'linestring_1', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>1,2 3,4</kml:coordinates></kml:LineString>'));

-- ERROR 1 Point
SELECT 'linestring_2', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>1,2</kml:coordinates></kml:LineString>'));

-- ERROR: empty coordinates 
SELECT 'linestring_3', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates></kml:coordinates></kml:LineString>'));
SELECT 'linestring_4', ST_AsEWKT(ST_GeomFromKML('<kml:LineString></kml:LineString>'));

-- XML not elements handle
SELECT 'linestring_5', ST_AsEWKT(ST_GeomFromKML(' <!-- --> <kml:LineString> <!-- --> <kml:coordinates>1,2 3,4</kml:coordinates></kml:LineString>'));




--
-- Polygon
--

-- 1 ring
SELECT 'polygon_1', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));

-- ERROR: In exterior ring: Last point is not the same as the first one 
SELECT 'polygon_2', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,3</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));

-- ERROR: In exterior 3D ring: Last point is not the same as the first one in Z
SELECT 'polygon_3', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2,3 4,5,6 7,8,9 1,2,0</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));

-- ERROR: Only 3 points in exterior ring
SELECT 'polygon_4', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'polygon_5', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates></kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));
SELECT 'polygon_6', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon>'));
SELECT 'polygon_7', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs></kml:outerBoundaryIs></kml:Polygon>'));
SELECT 'polygon_8', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon></kml:Polygon>'));

-- 2 rings
SELECT 'polygon_9', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));

-- XML not elements handle
SELECT 'polygon_10', ST_AsEWKT(ST_GeomFromKML(' <!-- --> <kml:Polygon> <!-- --> <kml:outerBoundaryIs> <!-- --> <kml:LinearRing> <!-- --> <kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs> <!-- --> <kml:innerBoundaryIs> <!-- --> <kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));

-- Empty interior ring coordinates  (even if defined)
SELECT 'polygon_11', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs></kml:innerBoundaryIs></kml:Polygon>'));

-- ERROR: Only 3 points in interior ring
SELECT 'polygon_12', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 7,8</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));

-- ERROR: In interior ring: Last point is not the same as the first one 
SELECT 'polygon_13', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,9</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));

-- ERROR: In interior 3D ring: Last point is not the same as the first one in Z
SELECT 'polygon_14', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2,3 4,5,6 7,8,9 1,2,3</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>10,11,12 13,14,15 16,17,18 10,11,0</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));

-- 3 rings
SELECT 'polygon_15', ST_AsEWKT(ST_GeomFromKML('<kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs><kml:innerBoundaryIs><kml:LinearRing><kml:coordinates>13,14 15,16 17,18 13,14</kml:coordinates></kml:LinearRing></kml:innerBoundaryIs></kml:Polygon>'));




--
-- MultiGeometry
--

-- 1 point 
--SELECT 'multi_1', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point></kml:MultiGeometry>'));

-- 2 points
--SELECT 'multi_2', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point><kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point></kml:MultiGeometry>'));

-- 1 line 
--SELECT 'multi_3', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:LineString><kml:coordinates>1,2 3,4</kml:coordinates></kml:LineString></kml:MultiGeometry>'));

-- 2 lines
--SELECT 'multi_4', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:LineString><kml:coordinates>1,2 3,4</kml:coordinates></kml:LineString><kml:LineString><kml:coordinates>5,6 7,8</kml:coordinates></kml:LineString></kml:MultiGeometry>'));

-- 1 polygon 
--SELECT 'multi_5', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry>'));

-- 2 polygons
--SELECT 'multi_6', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>1,2 3,4 5,6 1,2</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry>'));

-- Point, LineString and Polygon
SELECT 'multi_7', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point><kml:LineString><kml:coordinates>3,4 5,6</kml:coordinates></kml:LineString><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry>'));

-- Empty collection
SELECT 'multi_8', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry></kml:MultiGeometry>'));

-- Collection of collection
SELECT 'multi_9', ST_AsEWKT(ST_GeomFromKML('<kml:MultiGeometry><kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point><kml:MultiGeometry><kml:LineString><kml:coordinates>3,4 5,6</kml:coordinates></kml:LineString><kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry></kml:MultiGeometry>'));

-- XML not elements handle
SELECT 'multi_10', ST_AsEWKT(ST_GeomFromKML(' <!-- --> <kml:MultiGeometry> <!-- --> <kml:Point> <!-- --> <kml:coordinates>1,2</kml:coordinates></kml:Point> <!-- --> <kml:LineString><kml:coordinates>3,4 5,6</kml:coordinates></kml:LineString> <!-- --> <kml:Polygon><kml:outerBoundaryIs><kml:LinearRing><kml:coordinates>7,8 9,10 11,12 7,8</kml:coordinates></kml:LinearRing></kml:outerBoundaryIs></kml:Polygon></kml:MultiGeometry>'));




--
-- KML Namespace
-- 

-- KML namespace
SELECT 'ns_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point xmlns:kml="http://www.opengis.net/kml/2.2"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- KML namespace without explicit prefix 
SELECT 'ns_2', ST_AsEWKT(ST_GeomFromKML('<kml:Point xmlns="http://www.opengis.net/kml/2.2"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- ERROR wrong namespace
SELECT 'ns_3', ST_AsEWKT(ST_GeomFromKML('<kml:Point xmlns:kml="http://foo.net"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- Several namespaces
SELECT 'ns_4', ST_AsEWKT(ST_GeomFromKML('<kml:Point xmlns:foo="http://bar.net" xmlns:kml="http://www.opengis.net/kml/2.2"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- Ignore other namespace element
SELECT 'ns_5', ST_AsEWKT(ST_GeomFromKML('<kml:Point xmlns:foo="http://foo.net" xmlns:kml="http://www.opengis.net/kml/2.2"><kml:coordinates>1,2</kml:coordinates><foo:coordinates>3,4</foo:coordinates></kml:Point>'));

-- Attribute without explicit namespace
-- TODO SELECT 'ns_6', ST_AsEWKT(ST_GeomFromKML('<kml:Point altitudeMode="relative" xmlns:gml="http://www.opengis.net/gml"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- Attribute with explicit KML namespace
-- TODO SELECT 'ns_8', ST_AsEWKT(ST_GeomFromKML('<kml:Point kml:srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- Attribute with explicit prefix but unqualified namespace
-- TODO SELECT 'ns_10', ST_AsEWKT(ST_GeomFromKML('<kml:Point foo:srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml"><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- Ignore other namespace attribute
-- TODO SELECT 'ns_11', ST_AsEWKT(ST_GeomFromKML('<kml:Point foo:srsName="EPSG:4326" xmlns:foo="http://foo.net" xmlns:gml="http://www.opengis.net/gml"><kml:coordinates>1,2</kml:coordinates><foo:coordinates>3,4</foo:coordinates></kml:Point>'));




--
-- Coordinates
--

-- X,Y
SELECT 'coordinates_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2</kml:coordinates></kml:Point>'));

-- ERROR: single X
SELECT 'coordinates_2', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1</kml:coordinates></kml:Point>'));

-- X,Y,Z
SELECT 'coordinates_3', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2,3</kml:coordinates></kml:Point>'));

-- ERROR: 4 dimension
SELECT 'coordinates_4', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2,3,4</kml:coordinates></kml:Point>'));

-- ERROR: Only commas
SELECT 'coordinates_5', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>,</kml:coordinates></kml:Point>'));
SELECT 'coordinates_6', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates> , </kml:coordinates></kml:Point>'));

-- ERROR: empty or spaces
SELECT 'coordinates_7', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates></kml:coordinates></kml:Point>'));
SELECT 'coordinates_8', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>  </kml:coordinates></kml:Point>'));

-- ERROR: End on comma
SELECT 'coordinates_9', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2,3,</kml:coordinates></kml:Point>'));
SELECT 'coordinates_10', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2,</kml:coordinates></kml:Point>'));

-- ERROR: Begin on comma
SELECT 'coordinates_11', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>,1 2,3</kml:coordinates></kml:LineString>'));

-- Whitespaces before and after 
SELECT 'coordinates_12', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates> 1,2 3,4 </kml:coordinates></kml:LineString>'));
SELECT 'coordinates_13', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates> 
								1,2 3,4  
						   </kml:coordinates></kml:LineString>'));

-- ERROR: Spaces insides 
SELECT 'coordinates_14', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>1, 2 3, 4</kml:coordinates></kml:LineString>'));

-- Several spaces as tuples separator
SELECT 'coordinates_15', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>1,2   3,4</kml:coordinates></kml:LineString>'));
SELECT 'coordinates_16', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>
									1,2
									3,4
						   </kml:coordinates></kml:LineString>'));

-- ERROR: Junk
SELECT 'coordinates_17', ST_AsEWKT(ST_GeomFromKML('<kml:LineString><kml:coordinates>!@#$%^*()"</kml:coordinates></kml:LineString>'));








--
-- Bijective PostGIS KML test
--

-- Point
SELECT 'kml_1', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;POINT(1 2)'))));

-- Point - 3D
SELECT 'kml_2', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;POINT(1 2 3)'))));

-- Linestring  
SELECT 'kml_3', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;LINESTRING(1 2,3 4)'))));

-- Linestring - 3D
SELECT 'kml_4', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;LINESTRING(1 2 3,4 5 6)'))));

-- Polygon KML
SELECT 'kml_5', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;POLYGON((1 2,3 4,5 6,1 2))'))));

-- Polygon KML - 3D
SELECT 'kml_6', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;POLYGON((1 2 3,4 5 6,7 8 9,1 2 3))'))));

-- Multipoint
SELECT 'kml_7', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTIPOINT(1 2,3 4)'))));

-- Multipoint - 3D
SELECT 'kml_8', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTIPOINT(1 2 3,4 5 6)'))));

-- Multilinestring
SELECT 'kml_9', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTILINESTRING((1 2,3 4),(5 6,7 8))'))));

-- Multilinestring - 3D
SELECT 'kml_10', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTILINESTRING((1 2 3,4 5 6),(7 8 9,10 11 12))'))));

-- Multipolygon 
SELECT 'kml_11', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTIPOLYGON(((1 2,3 4,5 6,1 2)),((7 8,9 10,11 12,7 8)))'))));

-- Multipolygon - 3D
SELECT 'kml_12', ST_AsEWKT(ST_GeomFromKML(ST_AsKML(ST_AsEWKT('SRID=4326;MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)),((10 11 12,13 14 15,16 17 18,10 11 12)))'))));


--
-- Double
--

-- Several digits
SELECT 'double_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1234567890</kml:coordinates></kml:Point>'));

-- Sign +/- 
SELECT 'double_2', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1</kml:coordinates></kml:Point>'));
SELECT 'double_3', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,+1</kml:coordinates></kml:Point>'));

-- ERROR: double sign
SELECT 'double_4', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,--1</kml:coordinates></kml:Point>'));

-- ERROR: sign inside digit
SELECT 'double_5', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1-1</kml:coordinates></kml:Point>'));

-- Decimal part
SELECT 'double_6', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.2</kml:coordinates></kml:Point>'));
SELECT 'double_7', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.23</kml:coordinates></kml:Point>'));

-- no digit after dot
SELECT 'double_8', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.</kml:coordinates></kml:Point>'));

-- ERROR: several dots
SELECT 'double_9', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.2.3</kml:coordinates></kml:Point>'));

-- ERROR: no digit before dot
SELECT 'double_10', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,.1</kml:coordinates></kml:Point>'));
SELECT 'double_11', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-.1</kml:coordinates></kml:Point>'));

-- ERROR: not a digit
SELECT 'double_12', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,a</kml:coordinates></kml:Point>'));
SELECT 'double_13', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1a</kml:coordinates></kml:Point>'));
SELECT 'double_14', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1a2</kml:coordinates></kml:Point>'));

-- Exp 
SELECT 'double_15', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1e2</kml:coordinates></kml:Point>'));
SELECT 'double_16', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1E+2</kml:coordinates></kml:Point>'));
SELECT 'double_17', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1e-2</kml:coordinates></kml:Point>'));
SELECT 'double_18', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1E-2</kml:coordinates></kml:Point>'));

-- Exp with decimal parts
SELECT 'double_19', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.23E2</kml:coordinates></kml:Point>'));
SELECT 'double_20', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1.23e2</kml:coordinates></kml:Point>'));
SELECT 'double_21', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1.23E2</kml:coordinates></kml:Point>'));

-- ERROR: no exp digit
SELECT 'double_22', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1E</kml:coordinates></kml:Point>'));
SELECT 'double_23', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1e</kml:coordinates></kml:Point>'));

-- ERROR: dot inside exp digits
SELECT 'double_24', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1e2.3</kml:coordinates></kml:Point>'));
SELECT 'double_25', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,1E2.3</kml:coordinates></kml:Point>'));

-- ERROR: spaces inside 
SELECT 'double_26', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,- 1.23</kml:coordinates></kml:Point>'));
SELECT 'double_27', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1 .23</kml:coordinates></kml:Point>'));
SELECT 'double_28', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1. 23</kml:coordinates></kml:Point>'));
SELECT 'double_29', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1.23 E2</kml:coordinates></kml:Point>'));
SELECT 'double_30', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,-1.23E 2</kml:coordinates></kml:Point>'));

-- ERROR: Junk 
SELECT 'double_31', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,$0%@#$^%#</kml:coordinates></kml:Point>'));

-- ERROR: mixed coordinate dimension
SELECT 'mixed_dims_1', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2 1,2,3</kml:coordinates></kml:Point>'));
SELECT 'mixed_dims_2', ST_AsEWKT(ST_GeomFromKML('<kml:Point><kml:coordinates>1,2,3 1,2</kml:coordinates></kml:Point>'));





--
-- Delete inserted spatial data
--
DELETE FROM spatial_ref_sys WHERE srid = 4326;
