--
-- GeomFromGML regression test
-- Written by Olivier Courtin - Oslandia
--


--
-- spatial_ref_sys datas
--

-- EPSG 4326 : WGS 84
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (4326,'EPSG',4326,'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]','+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ');

--- EPSG 27562 : NTF (Paris) / Lambert Centre France
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (27562,'EPSG',27562,'PROJCS["NTF (Paris) / Lambert Centre France",GEOGCS["NTF (Paris)",DATUM["Nouvelle_Triangulation_Francaise_Paris",SPHEROID["Clarke 1880 (IGN)",6378249.2,293.4660212936265,AUTHORITY["EPSG","7011"]],TOWGS84[-168,-60,320,0,0,0,0],AUTHORITY["EPSG","6807"]],PRIMEM["Paris",2.33722917,AUTHORITY["EPSG","8903"]],UNIT["grad",0.01570796326794897,AUTHORITY["EPSG","9105"]],AUTHORITY["EPSG","4807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Lambert_Conformal_Conic_1SP"],PARAMETER["latitude_of_origin",52],PARAMETER["central_meridian",0],PARAMETER["scale_factor",0.99987742],PARAMETER["false_easting",600000],PARAMETER["false_northing",200000],AUTHORITY["EPSG","27562"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs ');

--- EPSG 27582 : NTF (Paris) / France II (deprecated)
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (27582,'EPSG',27582,'PROJCS["NTF (Paris) / France II (deprecated)",GEOGCS["NTF (Paris)",DATUM["Nouvelle_Triangulation_Francaise_Paris",SPHEROID["Clarke 1880 (IGN)",6378249.2,293.4660212936265,AUTHORITY["EPSG","7011"]],TOWGS84[-168,-60,320,0,0,0,0],AUTHORITY["EPSG","6807"]],PRIMEM["Paris",2.33722917,AUTHORITY["EPSG","8903"]],UNIT["grad",0.01570796326794897,AUTHORITY["EPSG","9105"]],AUTHORITY["EPSG","4807"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Lambert_Conformal_Conic_1SP"],PARAMETER["latitude_of_origin",52],PARAMETER["central_meridian",0],PARAMETER["scale_factor",0.99987742],PARAMETER["false_easting",600000],PARAMETER["false_northing",2200000],AUTHORITY["EPSG","27582"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=lcc +lat_1=46.8 +lat_0=46.8 +lon_0=0 +k_0=0.99987742 +x_0=600000 +y_0=2200000 +a=6378249.2 +b=6356515 +towgs84=-168,-60,320,0,0,0,0 +pm=paris +units=m +no_defs ');




-- Empty Geometry
SELECT 'empty_geom', ST_AsEWKT(ST_GeomFromGML(NULL));

--
-- XML
--

-- ERROR: Empty String
SELECT 'xml_1', ST_AsEWKT(ST_GeomFromGML(''));

-- ERROR: Not well formed XML
SELECT 'xml_2', ST_AsEWKT(ST_GeomFromGML('<foo>'));

-- ERROR: Not a GML Geometry
SELECT 'xml_3', ST_AsEWKT(ST_GeomFromGML('<foo/>'));



--
-- Point
--

-- 1 Point
SELECT 'point_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- ERROR: 2 points
SELECT 'point_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2 3,4</gml:coordinates></gml:Point>'));

-- ERROR: empty point
SELECT 'point_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point></gml:Point>'));

-- srsName handle
SELECT 'point_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:4326"><gml:pos>1 2</gml:pos></gml:Point>'));



--
-- LineString
--

-- 2 Points
SELECT 'linestring_1', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString>'));

-- ERROR 1 Point
SELECT 'linestring_2', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>1,2</gml:coordinates></gml:LineString>'));

-- srsName handle
SELECT 'linestring_3', ST_AsEWKT(ST_GeomFromGML('<gml:LineString srsName="EPSG:4326"><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString>'));

-- ERROR: empty coordinates 
SELECT 'linestring_4', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates></gml:coordinates></gml:LineString>'));
SELECT 'linestring_5', ST_AsEWKT(ST_GeomFromGML('<gml:LineString></gml:LineString>'));

-- XML not elements handle
SELECT 'linestring_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:LineString> <!-- --> <gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString>'));




--
-- Curve
--

-- 2 Points
SELECT 'curve_1', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- ERROR 1 Point
SELECT 'curve_2', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- srsName handle
SELECT 'curve_3', ST_AsEWKT(ST_GeomFromGML('<gml:Curve srsName="EPSG:4326"><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- ERROR: empty coordinates 
SELECT 'curve_4', ST_AsEWKT(ST_GeomFromGML('<gml:Curve srsName="EPSG:4326"><gml:segments><gml:LineStringSegment><gml:posList></gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));
SELECT 'curve_5', ST_AsEWKT(ST_GeomFromGML('<gml:Curve srsName="EPSG:4326"><gml:segments><gml:LineStringSegment></gml:LineStringSegment></gml:segments></gml:Curve>'));
SELECT 'curve_6', ST_AsEWKT(ST_GeomFromGML('<gml:Curve srsName="EPSG:4326"><gml:segments></gml:segments></gml:Curve>'));
SELECT 'curve_7', ST_AsEWKT(ST_GeomFromGML('<gml:Curve srsName="EPSG:4326"></gml:Curve>'));

-- linear interpolation 
SELECT 'curve_8', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment interpolation="linear"><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- ERROR: wrong interpolation 
SELECT 'curve_9', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment interpolation="spline"><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- XML not elements handle
SELECT 'curve_10', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:Curve> <!-- --> <gml:segments> <!-- --> <gml:LineStringSegment> <!-- --> <gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- 2 Segments
SELECT 'curve_11', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList>3 4 5 6</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));
SELECT 'curve_12', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList srsDimension="3">4 5 6 7 8 9</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- ERROR 2 Segments disjoint
SELECT 'curve_13', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList>5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- ERROR 2 3D Segments disjoint in Z
SELECT 'curve_14', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList srsDimension="3">4 5 0 7 8 9</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));

-- 2 segments, mixed dimension
SELECT 'curve_15', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList srsDimension="2">4 5 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));
SELECT 'curve_16', ST_AsEWKT(ST_GeomFromGML('<gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">1 2 3 4</gml:posList></gml:LineStringSegment><gml:LineStringSegment><gml:posList srsDimension="3">3 4 5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve>'));




--
-- Polygon
--

-- GML 2 - 1 ring
SELECT 'polygon_1', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));

-- srsName handle
SELECT 'polygon_2', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));

-- ERROR: In exterior ring: Last point is not the same as the first one 
SELECT 'polygon_3', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,3</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));

-- ERROR: In exterior 3D ring: Last point is not the same as the first one in Z
SELECT 'polygon_4', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2,3 4,5,6 7,8,9 1,2,0</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));

-- ERROR: Only 3 points in exterior ring
SELECT 'polygon_5', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'polygon_6', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates></gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));
SELECT 'polygon_7', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>'));
SELECT 'polygon_8', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs></gml:outerBoundaryIs></gml:Polygon>'));
SELECT 'polygon_9', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon></gml:Polygon>'));

-- GML 2 - 2 rings
SELECT 'polygon_10', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- XML not elements handle
SELECT 'polygon_11', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:Polygon> <!-- --> <gml:outerBoundaryIs> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs> <!-- --> <gml:innerBoundaryIs> <!-- --> <gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- Empty interior ring coordinates  (even if defined)
SELECT 'polygon_12', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs></gml:innerBoundaryIs></gml:Polygon>'));

-- ERROR: Only 3 points in interior ring
SELECT 'polygon_13', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>7,8 9,10 7,8</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- ERROR: In interior ring: Last point is not the same as the first one 
SELECT 'polygon_14', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,9</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- ERROR: In interior 3D ring: Last point is not the same as the first one in Z
SELECT 'polygon_15', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2,3 4,5,6 7,8,9 1,2,3</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>10,11,12 13,14,15 16,17,18 10,11,0</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- GML 2 - 3 rings
SELECT 'polygon_16', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>13,14 15,16 17,18 13,14</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon>'));

-- GML 3 
SELECT 'polygon_17', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:interior></gml:Polygon>'));

-- Mixed dimension in rings
SELECT 'polygon_18', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:interior></gml:Polygon>'));
SELECT 'polygon_19', ST_AsEWKT(ST_GeomFromGML('<gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:interior></gml:Polygon>'));



-- 
-- LinearRing
-- 

-- 2D LinearRing
SELECT 'linearring_1', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing>'));

-- srsName handle
SELECT 'linearring_2', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing srsName="EPSG:4326"><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing>'));

-- ERROR: Last point is not the same as the first one 
SELECT 'linearring_3', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing><gml:posList>1 2 3 4 5 6 1 3</gml:posList></gml:LinearRing>'));

-- ERROR: Last point is not the same as the first one in Z
SELECT 'linearring_4', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing><gml:posList>1 2 3 4 5 6 1 3</gml:posList></gml:LinearRing>'));

-- ERROR: Only 3 points
SELECT 'linearring_5', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing><gml:posList>1 2 3 4 1 3</gml:posList></gml:LinearRing>'));

-- ERROR: Empty coordinates 
SELECT 'linearring_6', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing><gml:posList></gml:posList></gml:LinearRing>'));
SELECT 'linearring_7', ST_AsEWKT(ST_GeomFromGML('<gml:LinearRing></gml:LinearRing>'));

-- XML not elements handle
SELECT 'linearring_8', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:LinearRing> <!-- --> <gml:posList>1 2 3 4 5 6 1 2</gml:posList> <!-- --> </gml:LinearRing>'));


--
-- Triangle
--

-- a triangle
SELECT 'triangle_1', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- srsName handle
SELECT 'triangle_2', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle srsName="EPSG:4326"><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- ERROR: Last point is not the same as the first one 
SELECT 'triangle_3', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,0</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surfacne>'));

-- ERROR: Last point is not the same as the first one in Z
SELECT 'triangle_4', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior><gml:LinearRing><gml:coordinates>1,2,3 4,5,6 7,8,9 1,2,0</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- ERROR: Only 3 points
SELECT 'triangle_5', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'triangle_6', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior><gml:LinearRing></gml:LinearRing></gml:exterior></gml:Triangle>'));
SELECT 'triangle_7', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle><gml:exterior></gml:exterior></gml:Triangle>'));
SELECT 'triangle_8', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle></gml:Triangle>'));

-- XML not elements handle
SELECT 'triangle_9', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:Triangle> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- planar interpolation
SELECT 'triangle_10', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle interpolation="planar"><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));

-- ERROR: interpolation not planar
SELECT 'triangle_11', ST_AsEWKT(ST_GeomFromGML('<gml:Triangle interpolation="not_planar"><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Triangle>'));



--
-- Surface
--

-- 1 ring
SELECT 'surface_1', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- srsName handle
SELECT 'surface_2', ST_AsEWKT(ST_GeomFromGML('<gml:Surface srsName="EPSG:4326"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: In exterior ring: Last point is not the same as the first one 
SELECT 'surface_3', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,0</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: In exterior 3D ring: Last point is not the same as the first one in Z
SELECT 'surface_4', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2,3 4,5,6 7,8,9 1,2,0</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: Only 3 points in exterior ring
SELECT 'surface_5', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'surface_6', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));
SELECT 'surface_7', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));
SELECT 'surface_8', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch></gml:PolygonPatch></gml:patches></gml:Surface>'));
SELECT 'surface_9', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches></gml:patches></gml:Surface>'));
SELECT 'surface_10', ST_AsEWKT(ST_GeomFromGML('<gml:Surface></gml:Surface>'));

-- 2 rings
SELECT 'surface_11', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- XML not elements handle
SELECT 'surface_12', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:Surface> <!-- --> <gml:patches> <!-- --> <gml:PolygonPatch> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior> <!-- --> <gml:interior> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- Empty interior ring coordinates  (even if defined)
SELECT 'surface_13', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: Only 3 points in interior ring
SELECT 'surface_14', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>7,8 9,10 7,8</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: In interior ring: Last point is not the same as the first one 
SELECT 'surface_15', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,0</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: In interior 3D ring: Last point is not the same as the first one in Z
SELECT 'surface_16', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2,3 4,5,6 7,8,9 1,2,3</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>10,11,12 13,14,15 16,17,18 10,11,0</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- 3 rings
SELECT 'surface_17', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:interior><gml:interior><gml:LinearRing><gml:coordinates>13,14 15,16 17,18 13,14</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- Mixed dimension in rings
SELECT 'surface_18', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));
SELECT 'surface_19', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: two patches
SELECT 'surface_20', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- planar interpolation
SELECT 'surface_21', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch interpolation="planar"><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: interpolation not planar
SELECT 'surface_22', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch interpolation="not_planar"><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface>'));

-- ERROR: interior but no exterior 
SELECT 'surface_23', ST_AsEWKT(ST_GeomFromGML('<gml:Surface><gml:patches><gml:PolygonPatch><gml:interior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface>'));


--
-- MultiPoint
--

-- 1 point
SELECT 'mpoint_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>'));

-- 2 points
SELECT 'mpoint_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:coordinates>3,4</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>'));

-- srsName handle
SELECT 'mpoint_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint srsName="EPSG:4326"><gml:pointMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>'));

-- Empty MultiPoints 
SELECT 'mpoint_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMember></gml:pointMember></gml:MultiPoint>'));
SELECT 'mpoint_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint></gml:MultiPoint>'));

-- XML not elements handle
SELECT 'mpoint_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiPoint> <!-- --> <gml:pointMember> <!-- --> <gml:Point> <!-- --> <gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember> <!-- --> <gml:pointMember> <!-- --> <gml:Point> <!-- --> <gml:coordinates>3,4</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>'));

-- Mixed srsName
SELECT 'mpoint_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint srsName="EPSG:27582"><gml:pointMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember><gml:pointMember><gml:Point srsName="EPSG:27562"><gml:coordinates>400000,5000000</gml:coordinates></gml:Point></gml:pointMember></gml:MultiPoint>'));

-- 1 point in pointMembers
SELECT 'mpoint_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMembers><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMembers></gml:MultiPoint>'));

-- 2 points in pointMembers
SELECT 'mpoint_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMembers><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point><gml:Point><gml:coordinates>3,4</gml:coordinates></gml:Point></gml:pointMembers></gml:MultiPoint>'));

-- Empty pointMembers
SELECT 'mpoint_10', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPoint><gml:pointMembers></gml:pointMembers></gml:MultiPoint>'));


--
-- MultiLineString
--

-- 1 line
SELECT 'mline_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString><gml:lineStringMember><gml:LineString><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));

-- 2 lines
SELECT 'mline_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString><gml:lineStringMember><gml:LineString><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString><gml:coordinates>5,6 7,8</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));

-- srsName handle
SELECT 'mline_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString srsName="EPSG:4326"><gml:lineStringMember><gml:LineString><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));

-- Empty MultiLineString
SELECT 'mline_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString><gml:lineStringMember></gml:lineStringMember></gml:MultiLineString>'));
SELECT 'mline_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString></gml:MultiLineString>'));

-- XML not elements handle
SELECT 'mline_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiLineString> <!-- --> <gml:lineStringMember> <!-- --> <gml:LineString> <!-- --> <gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString></gml:lineStringMember> <!-- --> <gml:lineStringMember> <!-- --> <gml:LineString> <!-- --> <gml:coordinates>5,6 7,8</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));

-- Mixed dimension
SELECT 'mline_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString><gml:lineStringMember><gml:LineString><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString><gml:posList srsDimension="2">7 8 9 10</gml:posList></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));
SELECT 'mline_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString><gml:lineStringMember><gml:LineString><gml:posList srsDimension="2">1 2 3 4</gml:posList></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString><gml:posList srsDimension="3">5 6 7 8 9 10</gml:posList></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));

-- Mixed srsName
SELECT 'mline_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiLineString srsName="EPSG:27582"><gml:lineStringMember><gml:LineString><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineString></gml:lineStringMember><gml:lineStringMember><gml:LineString srsName="EPSG:27562"><gml:coordinates>400000,5000000 400010,5000010</gml:coordinates></gml:LineString></gml:lineStringMember></gml:MultiLineString>'));


--
-- MultiCurve
--

-- 1 curve
SELECT 'mcurve_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- 2 curves
SELECT 'mcurve_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- srsName handle
SELECT 'mcurve_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve srsName="EPSG:4326"><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- Empty Curve 
SELECT 'mcurve_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMember></gml:curveMember></gml:MultiCurve>'));
SELECT 'mcurve_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve></gml:MultiCurve>'));

-- XML not elements handle
SELECT 'mcurve_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiCurve> <!-- --> <gml:curveMember> <!-- --> <gml:Curve> <!-- --> <gml:segments> <!-- --> <gml:LineStringSegment> <!-- --> <gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember> <!-- --> <gml:curveMember> <!-- --> <gml:Curve> <!-- --> <gml:segments> <!-- --> <gml:LineStringSegment><gml:posList>5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- Mixed dimension
SELECT 'mcurve_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">7 8 9 10</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));
SELECT 'mcurve_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="3">5 6 7 8 9 10</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- Mixed srsName
SELECT 'mcurve_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve srsName="EPSG:27582"><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:coordinates>1,2 3,4</gml:coordinates></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve srsName="EPSG:27562"><gml:segments><gml:LineStringSegment><gml:coordinates>400000,5000000 400010,5000010</gml:coordinates></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve>'));

-- 1 curve in curveMembers
SELECT 'mcurve_10', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMembers><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMembers></gml:MultiCurve>'));

-- 2 curves in curveMembers
SELECT 'mcurve_11', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMembers><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMembers></gml:MultiCurve>'));

SELECT 'mcurve_12', ST_AsEWKT(ST_GeomFromGML('<gml:MultiCurve><gml:curveMembers></gml:curveMembers></gml:MultiCurve>'));


--
-- MultiPolygon
--

-- 1 polygon
SELECT 'mpoly_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));

-- 2 polygons
SELECT 'mpoly_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));

-- srsName handle
SELECT 'mpoly_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon srsName="EPSG:4326"><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));

-- Empty MultiPolygon
SELECT 'mpoly_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon><gml:polygonMember></gml:polygonMember></gml:MultiPolygon>'));
SELECT 'mpoly_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon></gml:MultiPolygon>'));


-- XML not elements handle
SELECT 'mpoly_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiPolygon> <!-- --> <gml:polygonMember> <!-- --> <gml:Polygon> <!-- --> <gml:outerBoundaryIs> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember> <!-- --> <gml:polygonMember> <!-- --> <gml:Polygon> <!-- --> <gml:outerBoundaryIs> <!-- --> <gml:LinearRing> <!-- --> <gml:coordinates>7,8 9,10 11,12 7,8</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));

-- Mixed dimension
SELECT 'mpoly_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));
SELECT 'mpoly_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));

-- Mixed srsName
SELECT 'mpoly_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiPolygon srsName="EPSG:27582"><gml:polygonMember><gml:Polygon><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon></gml:polygonMember><gml:polygonMember><gml:Polygon srsName="EPSG:27562"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>400000,5000000 400010,5000010 400020,5000020 400000,5000000</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs><gml:innerBoundaryIs><gml:LinearRing><gml:coordinates>400100,5000100 400110,5000110 400120,5000120 400100,5000100</gml:coordinates></gml:LinearRing></gml:innerBoundaryIs></gml:Polygon></gml:polygonMember></gml:MultiPolygon>'));




--
-- MultiSurface
--

-- 1 surface
SELECT 'msurface_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- 2 surfaces
SELECT 'msurface_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>7 8 9 10 11 12 7 8</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- srsName handle
SELECT 'msurface_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface srsName="EPSG:4326"><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- Empty MultiSurface 
SELECT 'msurface_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMember></gml:surfaceMember></gml:MultiSurface>'));
SELECT 'msurface_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface></gml:MultiSurface>'));

-- XML not elements handle
SELECT 'msurface_6', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiSurface> <!-- --> <gml:surfaceMember> <!-- --> <gml:Polygon> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior> <!-- --> </gml:Polygon> <!-- --> </gml:surfaceMember> <!-- --> <gml:surfaceMember> <!-- --> <gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>7 8 9 10 11 12 7 8</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- Mixed dimension
SELECT 'msurface_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));
SELECT 'msurface_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- Mixed srsName
SELECT 'msurface_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface srsName="EPSG:27582"><gml:surfaceMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:coordinates>1,2 3,4 5,6 1,2</gml:coordinates></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMember><gml:surfaceMember><gml:Polygon srsName="EPSG:27562"><gml:exterior><gml:LinearRing><gml:coordinates>400000,5000000 400010,5000010 400020,5000020 400000,5000000</gml:coordinates></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:coordinates>400100,5000100 400110,5000110 400120,5000120 400100,5000100</gml:coordinates></gml:LinearRing></gml:interior></gml:Polygon></gml:surfaceMember></gml:MultiSurface>'));

-- 1 surface in surfaceMembers
SELECT 'msurface_10', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMembers><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMembers></gml:MultiSurface>'));

-- 2 surfaces in surfaceMembers
SELECT 'msurface_11', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMembers><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>7 8 9 10 11 12 7 8</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:surfaceMembers></gml:MultiSurface>'));

-- Empty surfaceMembers
SELECT 'msurface_12', ST_AsEWKT(ST_GeomFromGML('<gml:MultiSurface><gml:surfaceMembers></gml:surfaceMembers></gml:MultiSurface>'));


--
-- PolyhedralSurface
--

-- 1 ring
SELECT 'polyhedralsurface_1', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- srsName handle
SELECT 'polyhedralsurface_2', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface srsName="EPSG:4326"><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: In exterior ring: Last point is not the same as the first one 
SELECT 'polyhedralsurface_3', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 0 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: In exterior 3D ring: Last point is not the same as the first one in Z
SELECT 'polyhedralsurface_4', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 0</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: Only 3 points in exterior ring
SELECT 'polyhedralsurface_5', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'polyhedralsurface_6', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));
SELECT 'polyhedralsurface_7', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));
SELECT 'polyhedralsurface_8', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- EMPTY
SELECT 'polyhedralsurface_9', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches></gml:polygonPatches></gml:PolyhedralSurface>'));
SELECT 'polyhedralsurface_10', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface></gml:PolyhedralSurface>'));

-- 2 rings
SELECT 'polyhedralsurface_11', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- 2 patches
SELECT 'polyhedralsurface_12', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- 2 patches & 2 rings
SELECT 'polyhedralsurface_13', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">19 20 21 22 23 24 25 26 27 19 20 21</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- XML not elements handle
SELECT 'polyhedralsurface_14', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:PolyhedralSurface> <!-- --> <gml:polygonPatches> <!-- --> <gml:PolygonPatch> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList> <!-- --> </gml:LinearRing> <!-- --> </gml:exterior> <!-- --> </gml:PolygonPatch> <!-- --> <gml:PolygonPatch> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList> <!-- --> </gml:LinearRing> <!-- --> </gml:exterior> <!-- --> <gml:interior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList srsDimension="3">19 20 21 22 23 24 25 26 27 19 20 21</gml:posList> <!-- --> </gml:LinearRing> <!-- --> </gml:interior> <!-- --> </gml:PolygonPatch> <!-- --> </gml:polygonPatches> <!-- --> </gml:PolyhedralSurface>'));

-- Empty interior ring coordinates  (even if defined)
SELECT 'polyhedralsurface_15', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: Only 3 points in interior ring
SELECT 'polyhedralsurface_16', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 10 11 12</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: In interior ring: Last point is not the same as the first one 
SELECT 'polyhedralsurface_17', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 0 12</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: In interior 3D ring: Last point is not the same as the first one in Z
SELECT 'polyhedralsurface_18', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 0</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- 3 rings
SELECT 'polyhedralsurface_19', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:interior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">19 20 21 22 23 24 25 26 27 19 20 21</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- Mixed dimension in rings
SELECT 'polyhedralsurface_20', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

SELECT 'polyhedralsurface_21', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- Mixed dimension in patches
SELECT 'polyhedralsurface_22', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

SELECT 'polyhedralsurface_23', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- planar interpolation
SELECT 'polyhedralsurface_24', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch interpolation="planar"><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));

-- ERROR: interpolation not planar
SELECT 'polyhedralsurface_25', ST_AsEWKT(ST_GeomFromGML('<gml:PolyhedralSurface><gml:polygonPatches><gml:PolygonPatch interpolation="not_planar"><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:polygonPatches></gml:PolyhedralSurface>'));


--
-- Tin and TriangulatedSurface
--

-- 1 patch
SELECT 'tin_1', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- srsName handle
SELECT 'tin_2', ST_AsEWKT(ST_GeomFromGML('<gml:Tin srsName="EPSG:4326"><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- ERROR: Last point is not the same as the first one 
SELECT 'tin_3', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 0 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- ERROR: Last point is not the same as the first one in Z
SELECT 'tin_4', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 0</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- ERROR: Only 3 points in exterior ring
SELECT 'tin_5', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- ERROR: Empty exterior ring coordinates 
SELECT 'tin_6', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));
SELECT 'tin_7', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- EMPTY
SELECT 'tin_8', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle></gml:Triangle></gml:patches></gml:Tin>'));
SELECT 'tin_9', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches></gml:patches></gml:Tin>'));
SELECT 'tin_10', ST_AsEWKT(ST_GeomFromGML('<gml:Tin></gml:Tin>'));

-- 2 patches
SELECT 'tin_11', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- XML not elements handle
SELECT 'tin_12', ST_AsEWKT(ST_GeomFromGML('<gml:Tin> <!-- --> <gml:patches> <!-- --> <gml:Triangle> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList> <!-- --> </gml:LinearRing> <!-- --> </gml:exterior> <!-- --> </gml:Triangle> <!-- --> <gml:Triangle> <!-- --> <gml:exterior> <!-- --> <gml:LinearRing> <!-- --> <gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList> <!-- --> </gml:LinearRing> <!-- --> </gml:exterior> <!-- --> </gml:Triangle> <!-- --> </gml:patches> <!-- --> </gml:Tin>'));

-- 3 patches
SELECT 'tin_13', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">10 11 12 13 14 15 16 17 18 10 11 12</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">19 20 21 22 23 24 25 26 27 19 20 21</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- Mixed dimension in patches
SELECT 'tin_14', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">10 11 12 13 14 15 10 11</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

SELECT 'tin_15', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">7 8 9 10 11 12 13 14 15 7 8 9</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:Tin>'));

-- TriangulatedSurface
SELECT 'tin_16', ST_AsEWKT(ST_GeomFromGML('<gml:TriangulatedSurface><gml:patches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:patches></gml:TriangulatedSurface>'));

-- trianglePatches
SELECT 'tin_17', ST_AsEWKT(ST_GeomFromGML('<gml:Tin><gml:trianglePatches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:trianglePatches></gml:Tin>'));

SELECT 'tin_18', ST_AsEWKT(ST_GeomFromGML('<gml:TriangulatedSurface><gml:trianglePatches><gml:Triangle><gml:exterior><gml:LinearRing><gml:posList srsDimension="3">1 2 3 4 5 6 7 8 9 1 2 3</gml:posList></gml:LinearRing></gml:exterior></gml:Triangle></gml:trianglePatches></gml:TriangulatedSurface>'));



--
-- GeometryCollection
--

-- 1 simple geom 
SELECT 'collection_1', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:geometryMember></gml:MultiGeometry>'));

-- 2 simples geom
SELECT 'collection_2', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>3 4 5 6</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember></gml:MultiGeometry>'));

-- 1 multi geom 
SELECT 'collection_3', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:MultiPoint><gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos>3 4</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint></gml:geometryMember></gml:MultiGeometry>'));

-- 2 multi geom 
SELECT 'collection_4', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:MultiPoint><gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos>3 4</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint></gml:geometryMember><gml:geometryMember><gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>5 6 7 8</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>9 10 11 12</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve></gml:geometryMember></gml:MultiGeometry>'));

-- 2 multi geom and 2 simples
SELECT 'collection_5', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:MultiPoint><gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember><gml:pointMember><gml:Point><gml:pos>3 4</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint></gml:geometryMember><gml:geometryMember><gml:Point><gml:pos>5 6</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiCurve><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>7 8 9 10</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>11 12 13 14</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve></gml:geometryMember><gml:geometryMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>15 16 17 18 19 20 15 16</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:geometryMember></gml:MultiGeometry>'));

-- Empty collection
SELECT 'collection_6', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember></gml:geometryMember></gml:MultiGeometry>'));
SELECT 'collection_7', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry></gml:MultiGeometry>'));

-- Collection of collection
SELECT 'collection_8', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:pointMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:pointMember><gml:geometryMember><gml:MultiGeometry><gml:pointMember><gml:Point><gml:coordinates>3,4</gml:coordinates></gml:Point></gml:pointMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));

-- Collection of collection of collection
SELECT 'collection_9', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:Point><gml:coordinates>3,4</gml:coordinates></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));

-- srsName handle
SELECT 'collection_10', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry srsName="EPSG:4326"><gml:geometryMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:geometryMember></gml:MultiGeometry>'));

-- XML not elements handle
SELECT 'collection_11', ST_AsEWKT(ST_GeomFromGML(' <!-- --> <gml:MultiGeometry> <!-- --> <gml:geometryMember> <!-- --> <gml:Point> <!-- --> <gml:pos srsDimension="2">1 2</gml:pos></gml:Point></gml:geometryMember> <!-- --> <gml:geometryMember> <!-- --> <gml:MultiGeometry> <!-- --> <gml:geometryMember> <!-- --> <gml:MultiGeometry> <!-- --> <gml:geometryMember><gml:Point><gml:pos srsDimension="3">3 4 5</gml:pos></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));

-- Mixed dimension
SELECT 'collection_12', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension="3">1 2 3</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension="2">4 5</gml:pos></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));
SELECT 'collection_13', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension="2">1 2</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:Point><gml:pos srsDimension="3">3 4 5</gml:pos></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));

-- Mixed srsName
SELECT 'collection_14', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry srsName="EPSG:27582"><gml:geometryMember><gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry><gml:geometryMember><gml:MultiGeometry srsName="EPSG:27562"><gml:geometryMember><gml:Point><gml:coordinates>400000,5000000</gml:coordinates></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry></gml:geometryMember></gml:MultiGeometry>'));




--
-- srsName
--

-- Supported srsName patterns
SELECT 'srs_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="urn:EPSG:geographicCRS:4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="urn:ogc:def:crs:EPSG:4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="urn:ogc:def:crs:EPSG::4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="urn:ogc:def:crs:EPSG:6.6:4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="urn:x-ogc:def:crs:EPSG:6.6:4326"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="http://www.opengis.net/gml/srs/epsg.xml#4326"><gml:pos>1 2</gml:pos></gml:Point>'));

-- Use default srsName
SELECT 'srs_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 2</gml:pos></gml:Point>', 4326));

-- ERROR not a valid pattern
SELECT 'srs_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="a:wrong:pattern:4326"><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: not a defined SRID
SELECT 'srs_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:01"><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: SRID is not an int
SELECT 'srs_11', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:abc"><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: SRID is not only int
SELECT 'srs_12', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:4326abc"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_13', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:abc4326"><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: srsName empty
SELECT 'srs_14', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:"><gml:pos>1 2</gml:pos></gml:Point>'));
SELECT 'srs_15', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName=""><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: srsName is defined as -1
SELECT 'srs_16', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:-1"><gml:pos>1 2</gml:pos></gml:Point>'));

-- Reverse axis with all kind of simples geometry types 
SELECT 'srs_17', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry srsName="urn:ogc:def:crs:EPSG::4326"><gml:geometryMember><gml:Point><gml:pos srsDimension="2">1 2</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:LineString><gml:posList srsDimension="2">3 4 5 6</gml:posList></gml:LineString></gml:geometryMember><gml:geometryMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList srsDimension="2">7 8 9 10</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember><gml:geometryMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">11 12 13 14 15 16 11 12</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="2">17 18 19 20 21 22 17 18</gml:posList></gml:LinearRing></gml:interior></gml:Polygon></gml:geometryMember><gml:geometryMember><gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList srsDimension="2">23 24 25 26 27 28 23 24</gml:posList></gml:LinearRing></gml:exterior><gml:interior><gml:LinearRing><gml:posList srsDimension="2">25 26 27 28 29 30 25 26</gml:posList></gml:LinearRing></gml:interior></gml:PolygonPatch></gml:patches></gml:Surface></gml:geometryMember></gml:MultiGeometry>'));

-- Reverse axis with severals multi geometry types 
-- TODO



--
-- GML Namespace
-- 

-- GML namespace
SELECT 'ns_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- GML namespace without explicit prefix 
SELECT 'ns_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- GML 3.2 namespace
SELECT 'ns_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns:gml="http://www.opengis.net/gml/3.2"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- ERROR wrong namespace
SELECT 'ns_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns:gml="http://foo.net"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Several namespaces
SELECT 'ns_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns:foo="http://bar.net" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Ignore other namespace element
SELECT 'ns_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point xmlns:foo="http://foo.net" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates><foo:coordinates>3,4</foo:coordinates></gml:Point>'));

-- Attribute without explicit namespace
SELECT 'ns_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Attribute with explicit GML namespace
SELECT 'ns_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point gml:srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Attribute with explicit GML 3.2 namespace
SELECT 'ns_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point gml:srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml/3.2"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Attribute with explicit prefix but unqualified namespace
SELECT 'ns_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point foo:srsName="EPSG:4326" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Ignore other namespace attribute
SELECT 'ns_11', ST_AsEWKT(ST_GeomFromGML('<gml:Point foo:srsName="EPSG:4326" xmlns:foo="http://foo.net" xmlns:gml="http://www.opengis.net/gml"><gml:coordinates>1,2</gml:coordinates><foo:coordinates>3,4</foo:coordinates></gml:Point>'));

-- ERROR Attribute with explicit prefix but unqualified namespace
SELECT 'ns_12', ST_AsEWKT(ST_GeomFromGML('<gml:Point foo:srsName="EPSG:4326" xmlns:gml="http://foo.net"><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- Explicit GML namespace (no prefix)
SELECT 'ns_13', ST_AsEWKT(ST_GeomFromGML('<Point srsName="EPSG:4326" xmlns="http://www.opengis.net/gml"><coordinates>1,2</coordinates></Point>'));


--
-- Coordinates (simple)
--

-- X,Y
SELECT 'coordinates_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2</gml:coordinates></gml:Point>'));

-- ERROR: single X
SELECT 'coordinates_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1</gml:coordinates></gml:Point>'));

-- X,Y,Z
SELECT 'coordinates_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2,3</gml:coordinates></gml:Point>'));

-- ERROR: 4 dimension
SELECT 'coordinates_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2,3,4</gml:coordinates></gml:Point>'));

-- ERROR: Only commas
SELECT 'coordinates_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>,</gml:coordinates></gml:Point>'));
SELECT 'coordinates_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates> , </gml:coordinates></gml:Point>'));

-- ERROR: empty or spaces
SELECT 'coordinates_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates></gml:coordinates></gml:Point>'));
SELECT 'coordinates_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>  </gml:coordinates></gml:Point>'));

-- ERROR: End on comma
SELECT 'coordinates_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2,3,</gml:coordinates></gml:Point>'));
SELECT 'coordinates_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates>1,2,</gml:coordinates></gml:Point>'));

-- ERROR: Begin on comma
SELECT 'coordinates_11', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>,1 2,3</gml:coordinates></gml:LineString>'));

-- Whitespaces before and after 
SELECT 'coordinates_12', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates> 1,2 3,4 </gml:coordinates></gml:LineString>'));
SELECT 'coordinates_13', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates> 
								1,2 3,4  
						   </gml:coordinates></gml:LineString>'));

-- Mixed dimension
SELECT 'coordinates_14', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>1,2,3 4,5</gml:coordinates></gml:LineString>'));

-- ERROR: Spaces insides 
SELECT 'coordinates_15', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>1, 2 3, 4</gml:coordinates></gml:LineString>'));
SELECT 'coordinates_16', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>1,2  3,4</gml:coordinates></gml:LineString>'));

-- ERROR: Junk
SELECT 'coordinates_17', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates>!@#$%^*()"</gml:coordinates></gml:LineString>'));



--
-- Coordinates (cs,ts,decimal)
--

-- Specify default CS separator
SELECT 'coordinates_cs_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates cs="," >1,2</gml:coordinates></gml:Point>'));

-- ERROR: wrong CS separator 
SELECT 'coordinates_cs_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates cs=";" >1,2</gml:coordinates></gml:Point>'));

-- Specify a CS separator
SELECT 'coordinates_cs_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates cs=";" >1;2</gml:coordinates></gml:Point>'));

-- ERROR: CS separator is a number
SELECT 'coordinates_cs_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates cs="0" >102</gml:coordinates></gml:Point>'));

-- ERROR: CS separator is multichar 
SELECT 'coordinates_cs_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coordinates cs="||" >1||2</gml:coordinates></gml:Point>'));

-- Specify default TS separator
SELECT 'coordinates_cs_6', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts=" ">1,2 3,4</gml:coordinates></gml:LineString>'));

-- ERROR: wrong TS separator 
SELECT 'coordinates_cs_7', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts=" ">1,2;3,4</gml:coordinates></gml:LineString>'));

-- Specify a TS separator
SELECT 'coordinates_cs_8', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts=";">1,2;3,4</gml:coordinates></gml:LineString>'));

-- ERROR: TS separator is a number
SELECT 'coordinates_cs_9', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts="0">1,203,4</gml:coordinates></gml:LineString>'));

-- ERROR: TS separator is multichar 
SELECT 'coordinates_cs_10', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts="||">1,2||3,4</gml:coordinates></gml:LineString>'));

-- Specify default Decimal separator
SELECT 'coordinates_cs_11', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates decimal=".">1.1,2.2 3.3,4.4</gml:coordinates></gml:LineString>'));

-- ERROR: wrong Decimal separator 
SELECT 'coordinates_cs_12', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates decimal=";">1;1,2;2 3;3,4;4</gml:coordinates></gml:LineString>'));

-- ERROR: Decimal separator is a number
SELECT 'coordinates_cs_13', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates decimal="0">101,202 303,404</gml:coordinates></gml:LineString>'));

-- ERROR: Decimal separator is multichar 
SELECT 'coordinates_cs_14', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates decimal="||">1||1,2||2 3||3,4||4</gml:coordinates></gml:LineString>'));

-- CS and TS and Decimal together
SELECT 'coordinates_cs_15', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates cs="." ts=";" decimal=",">1,1.2,2;3,3.4,4</gml:coordinates></gml:LineString>'));

-- ERROR: CS and Decimal are the same
SELECT 'coordinates_cs_16', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts=" " cs=" ">1 2 3 4</gml:coordinates></gml:LineString>'));
SELECT 'coordinates_cs_17', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates cs="." ts=";" decimal=".">1.1.2.2;3.3.4,4</gml:coordinates></gml:LineString>'));
SELECT 'coordinates_cs_18', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates cs="." ts=";" decimal=";">1;1.2;2;3;3.4,4</gml:coordinates></gml:LineString>'));


-- XML Entity substitution
SELECT 'coordinates_cs_19', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coordinates ts="," cs="&#x20;">1 2,3 4</gml:coordinates></gml:LineString>'));




--
-- pos
--

-- X,Y
SELECT 'pos_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 2</gml:pos></gml:Point>'));

-- ERROR: single X
SELECT 'pos_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1</gml:pos></gml:Point>'));

-- ERROR: X,Y,Z but without explicit dimension
SELECT 'pos_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 2 3</gml:pos></gml:Point>'));

-- ERROR: 4 dimension
SELECT 'pos_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 2 3 4</gml:pos></gml:Point>'));

-- ERROR: empty or spaces
SELECT 'pos_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos></gml:pos></gml:Point>'));
SELECT 'pos_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos> </gml:pos></gml:Point>'));
SELECT 'pos_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>  </gml:pos></gml:Point>'));

-- Whitespaces before and after 
SELECT 'pos_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos> 1 2 </gml:pos></gml:Point>'));
SELECT 'pos_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>  1 2  </gml:pos></gml:Point>'));
SELECT 'pos_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos> 
		 			         1 2
				           </gml:pos></gml:Point>'));
-- Several Spaces insides 
SELECT 'pos_11', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1  2</gml:pos></gml:Point>'));

-- ERROR: Junk
SELECT 'pos_12', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>@!#$#%</gml:pos></gml:Point>'));

-- ERROR: 1 dimension
SELECT 'pos_13', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos srsDimension="1">1</gml:pos></gml:Point>'));

-- 2 Dimensions explicit
SELECT 'pos_14', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos srsDimension="2">1 2</gml:pos></gml:Point>'));

-- ERROR: 2 Dimensions explicit but 3 dims
SELECT 'pos_15', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos srsDimension="2">1 2 3</gml:pos></gml:Point>'));

-- 3 Dimensions explicit
SELECT 'pos_16', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos srsDimension="3">1 2 3</gml:pos></gml:Point>'));

-- ERROR: 4 dimensions
SELECT 'pos_17', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos srsDimension="4">1 2 3 4</gml:pos></gml:Point>'));


--
-- posList
--
-- 2 coords
SELECT 'poslist_1', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>1 2 3 4</gml:posList></gml:LineString>'));

-- spaces before and after
SELECT 'poslist_2', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList> 1 2 3 4 </gml:posList></gml:LineString>'));
SELECT 'poslist_3', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>  1 2 3 4  </gml:posList></gml:LineString>'));
SELECT 'poslist_4', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>
	      						1 2 3 4  
						</gml:posList></gml:LineString>'));

-- explicit 2 dimension
SELECT 'poslist_5', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList srsDimension="2">1 2 3 4</gml:posList></gml:LineString>'));
-- ERROR: wrong dimension
SELECT 'poslist_6', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList> 1 2 3 4 5 </gml:posList></gml:LineString>'));
SELECT 'poslist_7', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList srsDimension="2">1 2 3 4 5</gml:posList></gml:LineString>'));

-- 2 coord 3D
SELECT 'poslist_8', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList srsDimension="3">1 2 3 4 5 6</gml:posList></gml:LineString>'));

-- ERROR: 1 dimension
SELECT 'poslist_9', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>1</gml:posList></gml:LineString>'));
SELECT 'poslist_10', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList srsDimension="1">1</gml:posList></gml:LineString>'));

-- ERROR: 4 dimensions
SELECT 'poslist_11', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList srsDimension="4">1 2 3 4 5 6 7 8</gml:posList></gml:LineString>'));

-- ERROR: 3D but no explicit dimension
SELECT 'poslist_12', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>1 2 3</gml:posList></gml:LineString>'));

-- ERROR: empty or spaces
SELECT 'poslist_13', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList></gml:posList></gml:LineString>'));
SELECT 'poslist_14', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList> </gml:posList></gml:LineString>'));
SELECT 'poslist_15', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>  </gml:posList></gml:LineString>'));

-- Several spaces insides posList
SELECT 'poslist_16', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>1 2  3 4</gml:posList></gml:LineString>'));
SELECT 'poslist_17', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList> 1  2  3 4 </gml:posList></gml:LineString>'));

-- ERROR: Junk
SELECT 'poslist_18', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:posList>!@#$%^*()"</gml:posList></gml:LineString>'));



--
-- Generic data
--

-- Mixed pos, posList, coordinates, coord
SELECT 'data_1', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:pos>1 2</gml:pos><gml:posList>3 4 5 6</gml:posList><gml:coordinates>7,8 9,10</gml:coordinates><gml:coord><gml:X>11</gml:X><gml:Y>12</gml:Y></gml:coord></gml:LineString>'));

-- Mixed pos, posList, pointProperty, pointRep
SELECT 'data_2', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:pos>1 2</gml:pos><gml:posList>3 4 5 6</gml:posList><gml:pointProperty><gml:Point><gml:pos>7 8</gml:pos></gml:Point></gml:pointProperty><gml:pointRep><gml:Point><gml:coordinates>9,10</gml:coordinates></gml:Point></gml:pointRep></gml:LineString>'));




--
-- XLink
--

-- xlink with pointProperty
SELECT 'xlink_1', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: xlink:href destination is not defined
SELECT 'xlink_2', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="#p2"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: no href 
SELECT 'xlink_3', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" /></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: empty href 
SELECT 'xlink_4', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href=""/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: no sharp char in href
SELECT 'xlink_5', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="p2"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: no xlink namespace
SELECT 'xlink_6', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: wrong xlink namespace
SELECT 'xlink_7', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://foo.net"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: no xlink:type 
SELECT 'xlink_8', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:href="#p1"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: xlink:type not simple
SELECT 'xlink_9', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="extended" xlink:href="#p1"/></gml:pointProperty><gml:pos>3 4</gml:pos></gml:LineString>'));

-- ERROR: more than one destination is defined
SELECT 'xlink_10', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointProperty><gml:pointProperty><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:pointProperty><gml:pointProperty><gml:Point gml:id="p1"><gml:pos>3 4</gml:pos></gml:Point></gml:pointProperty></gml:LineString>'));

-- xlink with pointRep
SELECT 'xlink_11', ST_AsEWKT(ST_GeomFromGML('<gml:LineString xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:pointRep><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:pointRep><gml:pointRep><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:pointRep><gml:pos>3 4</gml:pos></gml:LineString>'));

-- xlink on a point 
SELECT 'xlink_12', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:Point gml:id="p1"><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:Point xlink:type="simple" xlink:href="#p1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a linestring
SELECT 'xlink_13', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:LineString gml:id="l1"><gml:posList>1 2 3 4</gml:posList></gml:LineString></gml:geometryMember><gml:geometryMember><gml:LineString xlink:type="simple" xlink:href="#l1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a curve 
SELECT 'xlink_14', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:Curve gml:id="c1"><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:geometryMember><gml:geometryMember><gml:Curve xlink:type="simple" xlink:href="#c1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a polygon 
SELECT 'xlink_15', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:Polygon gml:id="p1"><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:geometryMember><gml:geometryMember><gml:Polygon xlink:type="simple" xlink:href="#p1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a surface 
SELECT 'xlink_16', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:Surface gml:id="s1"><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface></gml:geometryMember><gml:geometryMember><gml:Surface xlink:type="simple" xlink:href="#s1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multipoint 
SELECT 'xlink_17', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiPoint gml:id="mp1"><gml:pointMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:pointMember></gml:MultiPoint></gml:geometryMember><gml:geometryMember><gml:MultiPoint xlink:type="simple" xlink:href="#mp1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multiline 
SELECT 'xlink_18', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiLineString gml:id="ml1"><gml:lineStringMember><gml:LineString><gml:posList>1 2 3 4</gml:posList></gml:LineString></gml:lineStringMember></gml:MultiLineString></gml:geometryMember><gml:geometryMember><gml:MultiLineString xlink:type="simple" xlink:href="#ml1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multicurve 
SELECT 'xlink_19', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiCurve gml:id="mc1"><gml:curveMember><gml:Curve><gml:segments><gml:LineStringSegment><gml:posList>1 2 3 4</gml:posList></gml:LineStringSegment></gml:segments></gml:Curve></gml:curveMember></gml:MultiCurve></gml:geometryMember><gml:geometryMember><gml:MultiCurve xlink:type="simple" xlink:href="#mc1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multipolygon 
SELECT 'xlink_20', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiPolygon gml:id="mp1"><gml:polygonMember><gml:Polygon><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:Polygon></gml:polygonMember></gml:MultiPolygon></gml:geometryMember><gml:geometryMember><gml:MultiPolygon xlink:type="simple" xlink:href="#mp1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multisurface 
SELECT 'xlink_21', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiSurface gml:id="ms1"><gml:surfaceMember><gml:Surface><gml:patches><gml:PolygonPatch><gml:exterior><gml:LinearRing><gml:posList>1 2 3 4 5 6 1 2</gml:posList></gml:LinearRing></gml:exterior></gml:PolygonPatch></gml:patches></gml:Surface></gml:surfaceMember></gml:MultiSurface></gml:geometryMember><gml:geometryMember><gml:MultiSurface xlink:type="simple" xlink:href="#ms1"/></gml:geometryMember></gml:MultiGeometry>'));

-- xlink on a multigeometry 
SELECT 'xlink_22', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:MultiGeometry gml:id="mg1"><gml:geometryMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember></gml:MultiGeometry></gml:geometryMember><gml:geometryMember><gml:MultiGeometry xlink:type="simple" xlink:href="#mg1"/></gml:geometryMember></gml:MultiGeometry>'));

-- ERROR circular ref
SELECT 'xlink_23', ST_AsEWKT(ST_GeomFromGML('<gml:MultiGeometry gml:id="mg1" xmlns:gml="http://www.opengis.net/gml" xmlns:xlink="http://www.w3.org/1999/xlink"><gml:geometryMember><gml:Point><gml:pos>1 2</gml:pos></gml:Point></gml:geometryMember><gml:geometryMember><gml:MultiGeometry xlink:type="simple" xlink:href="#mg1"/></gml:geometryMember></gml:MultiGeometry>'));



--
-- Bijective PostGIS GML test
--

-- Point GML 2
SELECT 'gml_1', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('POINT(1 2)'))));

-- Point GML 2 - 3D
SELECT 'gml_2', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('POINT(1 2 3)'))));

-- Point GML 2 & SRID planar
SELECT 'gml_3', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;POINT(1 2)'))));

-- Point GML 3
SELECT 'gml_4', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('POINT(1 2)'))));

-- Point GML 3 & SRID lat/lon
SELECT 'gml_5', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;POINT(1 2)'), 16)));

-- Point GML 3 - 3D
SELECT 'gml_6', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('POINT(1 2 3)'))));

-- Point GML 3 - 3D & SRID lat/lon
SELECT 'gml_7', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;POINT(1 2 3)'), 16)));

-- Linestring GML 2
SELECT 'gml_8', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('LINESTRING(1 2,3 4)'))));

-- Linestring GML 2 - 3D
SELECT 'gml_9', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('LINESTRING(1 2 3,4 5 6)'))));

-- Linestring GML 2 & SRID planar
SELECT 'gml_10', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;LINESTRING(1 2,3 4)'))));

-- Linestring GML 3
SELECT 'gml_11', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('LINESTRING(1 2,3 4)'))));

-- Linestring GML 3 & SRID lat/lon
SELECT 'gml_12', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;LINESTRING(1 2,3 4)'), 16)));

-- Linestring GML 3 - 3D
SELECT 'gml_13', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('LINESTRING(1 2 3,4 5 6)'))));

-- Linestring GML 3 - 3D & SRID lat/lon
SELECT 'gml_14', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;LINESTRING(1 2 3,4 5 6)'), 16)));

-- Polygon GML 2
SELECT 'gml_15', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('POLYGON((1 2,3 4,5 6,1 2))'))));

-- Polygon GML 2 - 3D
SELECT 'gml_16', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('POLYGON((1 2 3,4 5 6,7 8 9,1 2 3))'))));

-- Polygon GML 2 & SRID planar
SELECT 'gml_17', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;POLYGON((1 2,3 4,5 6,1 2))'))));

-- Polygon GML 3
SELECT 'gml_18', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('POLYGON((1 2,3 4,5 6,1 2))'))));

-- Polygon GML 3 & SRID lat/lon
SELECT 'gml_19', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;POLYGON((1 2,3 4,5 6,1 2))'), 16)));

-- Polygon GML 3 - 3D
SELECT 'gml_20', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('POLYGON((1 2 3,4 5 6,7 8 9,1 2 3))'))));

-- Polygon GML 3 - 3D & SRID lat/lon
SELECT 'gml_21', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;POLYGON((1 2 3,4 5 6,7 8 9,1 2 3))'), 16)));

-- Triangle GML 3
SELECT 'gml_22', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('TRIANGLE((1 2,3 4,5 6,1 2))'))));

-- Triangle GML 3 & SRID lat/lon
SELECT 'gml_23', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;TRIANGLE((1 2,3 4,5 6,1 2))'), 16)));

-- Triangle GML 3 & SRID planar
SELECT 'gml_24', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=27582;TRIANGLE((1 2,3 4,5 6,1 2))'))));

-- Triangle GML 3 - 3D
SELECT 'gml_25', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('TRIANGLE((1 2 3,4 5 6,7 8 9,1 2 3))'))));

-- Triangle GML 3 - 3D & SRID lat/lon
SELECT 'gml_26', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;TRIANGLE((1 2 3,4 5 6,7 8 9,1 2 3))'), 16)));

-- Multipoint GML 2
SELECT 'gml_27', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTIPOINT(1 2)'))));

-- Multipoint GML 2 - 3D
SELECT 'gml_28', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTIPOINT(1 2 3)'))));

-- Multipoint GML 2 & SRID planar
SELECT 'gml_29', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;MULTIPOINT(1 2)'))));

-- Multipoint GML 3
SELECT 'gml_30', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTIPOINT(1 2)'))));

-- Multipoint GML 3 & SRID lat/lon
SELECT 'gml_31', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTIPOINT(1 2)'), 16)));

-- Multipoint GML 3 - 3D
SELECT 'gml_32', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTIPOINT(1 2 3)'))));

-- Multipoint GML 3 - 3D & SRID lat/lon
SELECT 'gml_33', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTIPOINT(1 2 3)'), 16)));

-- Multilinestring GML 2
SELECT 'gml_34', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTILINESTRING((1 2,3 4))'))));

-- Multilinestring GML 2 - 3D
SELECT 'gml_35', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTILINESTRING((1 2 3,4 5 6))'))));

-- Multilinestring GML 2 & SRID planar
SELECT 'gml_36', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;MULTILINESTRING((1 2,3 4))'))));

-- Multilinestring GML 3
SELECT 'gml_37', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTILINESTRING((1 2,3 4))'))));

-- Multilinestring GML 3 & SRID lat/lon
SELECT 'gml_38', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTILINESTRING((1 2,3 4))'), 16)));

-- Multilinestring GML 3 - 3D
SELECT 'gml_39', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTILINESTRING((1 2 3,4 5 6))'))));

-- Multilinestring GML 3 - 3D & SRID lat/lon
SELECT 'gml_40', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTILINESTRING((1 2 3,4 5 6))'), 16)));

-- Multipolygon GML 2
SELECT 'gml_41', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTIPOLYGON(((1 2,3 4,5 6,1 2)))'))));

-- Multipolygon GML 2 - 3D
SELECT 'gml_42', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)))'))));

-- Multipolygon GML 2 & SRID planar
SELECT 'gml_43', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;MULTIPOLYGON(((1 2,3 4,5 6,1 2)))'))));

-- Multipolygon GML 3
SELECT 'gml_44', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTIPOLYGON(((1 2,3 4,5 6,1 2)))'))));

-- Multipolygon GML 3 & SRID lat/lon
SELECT 'gml_45', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTIPOLYGON(((1 2,3 4,5 6,1 2)))'), 16)));

-- Multipolygon GML 3 - 3D
SELECT 'gml_46', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)))'))));

-- Multipolygon GML 3 - 3D & SRID planar
SELECT 'gml_47', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=27582;MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)))'))));

-- Multipolygon GML 3 - 3D & SRID lat/lon
SELECT 'gml_48', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;MULTIPOLYGON(((1 2 3,4 5 6,7 8 9,1 2 3)))'), 16)));

-- Tin GML 3
SELECT 'gml_49', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('TIN(((1 2,3 4,5 6,1 2)))'))));

-- Tin GML 3 & SRID planar
SELECT 'gml_50', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=27582;TIN(((1 2,3 4,5 6,1 2)))'))));

-- Tin GML 3 & SRID lat/lon
SELECT 'gml_51', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;TIN(((1 2,3 4,5 6,1 2)))'), 16)));

-- Tin GML 3 - 3D
SELECT 'gml_52', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('TIN(((1 2 3,4 5 6,7 8 9,1 2 3)))'))));

-- Tin GML 3 - 3D & SRID planar
SELECT 'gml_53', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=27582;TIN(((1 2 3,4 5 6,7 8 9,1 2 3)))'))));

-- Tin GML 3 - 3D & SRID lat/lon
SELECT 'gml_54', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;TIN(((1 2 3,4 5 6,7 8 9,1 2 3)))'), 16)));

-- Collection GML 2
SELECT 'gml_55', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('GEOMETRYCOLLECTION(POINT(1 2))'))));

-- Collection GML 2 - 3D
SELECT 'gml_56', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('GEOMETRYCOLLECTION(POINT(1 2 3))'))));

-- Collection GML 2 & SRID planar
SELECT 'gml_57', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(ST_AsEWKT('SRID=27582;GEOMETRYCOLLECTION(POINT(1 2))'))));

-- Collection GML 3
SELECT 'gml_58', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('GEOMETRYCOLLECTION(POINT(1 2))'))));

-- Collection GML 3 & SRID lat/lon
SELECT 'gml_59', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;GEOMETRYCOLLECTION(POINT(1 2))'), 16)));

-- Collection GML 3 - 3D
SELECT 'gml_60', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('GEOMETRYCOLLECTION(POINT(1 2 3))'))));

-- Collection GML 3 - 3D & SRID lat/lon
SELECT 'gml_61', ST_AsEWKT(ST_GeomFromGML(ST_AsGML(3, ST_AsEWKT('SRID=4326;GEOMETRYCOLLECTION(POINT(1 2 3))'), 16)));





--
-- coord
--

-- X,Y
SELECT 'coord_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y>2</gml:Y></gml:coord></gml:Point>'));

-- X,Y,Z
SELECT 'coord_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y>2</gml:Y><gml:Z>3</gml:Z></gml:coord></gml:Point>'));

-- ERROR no X or Y
SELECT 'coord_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Z>2</gml:Z></gml:coord></gml:Point>'));
SELECT 'coord_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:Y>1</gml:Y><gml:Z>2</gml:Z></gml:coord></gml:Point>'));
SELECT 'coord_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:Z>1</gml:Z></gml:coord></gml:Point>'));

-- ERROR empty coord even if defined
SELECT 'coord_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X></gml:X><gml:Y></gml:Y></gml:coord></gml:Point>'));
SELECT 'coord_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord></gml:coord></gml:Point>'));

-- ERROR space in coord 
SELECT 'coord_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y> </gml:Y></gml:coord></gml:Point>'));
SELECT 'coord_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y>   </gml:Y></gml:coord></gml:Point>'));

-- Spaces before and after coord
SELECT 'coord_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y> 2 </gml:Y></gml:coord></gml:Point>'));
SELECT 'coord_11', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y>  2  </gml:Y></gml:coord></gml:Point>'));
SELECT 'coord_12', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:coord><gml:X>1</gml:X><gml:Y>  
	                  				2   
					     </gml:Y></gml:coord></gml:Point>'));

-- Several coords
SELECT 'coord_13', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coord><gml:X>1</gml:X><gml:Y>2</gml:Y></gml:coord><gml:coord><gml:X>3</gml:X><gml:Y>4</gml:Y></gml:coord></gml:LineString>'));

-- Several coords mixed dimension
SELECT 'coord_14', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coord><gml:X>1</gml:X><gml:Y>2</gml:Y><gml:Z>3</gml:Z></gml:coord><gml:coord><gml:X>4</gml:X><gml:Y>5</gml:Y></gml:coord></gml:LineString>'));
SELECT 'coord_15', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coord><gml:X>1</gml:X><gml:Y>2</gml:Y></gml:coord><gml:coord><gml:X>3</gml:X><gml:Y>4</gml:Y><gml:Z>5</gml:Z></gml:coord></gml:LineString>'));

-- XML not elements handle
SELECT 'coord_16', ST_AsEWKT(ST_GeomFromGML('<gml:LineString><gml:coord> <!-- --> <gml:X>1</gml:X> <!-- --> <gml:Y>2</gml:Y> <!-- --> </gml:coord> <!-- --> <gml:coord> <!-- --> <gml:X>3</gml:X> <!-- --> <gml:Y>4</gml:Y></gml:coord></gml:LineString>'));


--
-- Double
--

-- Several digits
SELECT 'double_1', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1234567890</gml:pos></gml:Point>'));

-- Sign +/- 
SELECT 'double_2', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1</gml:pos></gml:Point>'));
SELECT 'double_3', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 +1</gml:pos></gml:Point>'));

-- ERROR: double sign
SELECT 'double_4', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 --1</gml:pos></gml:Point>'));

-- ERROR: sign inside digit
SELECT 'double_5', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1-1</gml:pos></gml:Point>'));

-- Decimal part
SELECT 'double_6', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.2</gml:pos></gml:Point>'));
SELECT 'double_7', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.23</gml:pos></gml:Point>'));

-- no digit after dot
SELECT 'double_8', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.</gml:pos></gml:Point>'));

-- ERROR: several dots
SELECT 'double_9', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.2.3</gml:pos></gml:Point>'));

-- ERROR: no digit before dot
SELECT 'double_10', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 .1</gml:pos></gml:Point>'));
SELECT 'double_11', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -.1</gml:pos></gml:Point>'));

-- ERROR: not a digit
SELECT 'double_12', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 a</gml:pos></gml:Point>'));
SELECT 'double_13', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1a</gml:pos></gml:Point>'));
SELECT 'double_14', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1a2</gml:pos></gml:Point>'));

-- Exp 
SELECT 'double_15', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1e2</gml:pos></gml:Point>'));
SELECT 'double_16', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1E+2</gml:pos></gml:Point>'));
SELECT 'double_17', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1e-2</gml:pos></gml:Point>'));
SELECT 'double_18', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1E-2</gml:pos></gml:Point>'));

-- Exp with decimal parts
SELECT 'double_19', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.23E2</gml:pos></gml:Point>'));
SELECT 'double_20', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1.23e2</gml:pos></gml:Point>'));
SELECT 'double_21', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1.23E2</gml:pos></gml:Point>'));

-- ERROR: no exp digit
SELECT 'double_22', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1E</gml:pos></gml:Point>'));
SELECT 'double_23', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1e</gml:pos></gml:Point>'));

-- ERROR: dot inside exp digits
SELECT 'double_24', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1e2.3</gml:pos></gml:Point>'));
SELECT 'double_25', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 1E2.3</gml:pos></gml:Point>'));

-- ERROR: spaces inside 
SELECT 'double_26', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 - 1.23</gml:pos></gml:Point>'));
SELECT 'double_27', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1 .23</gml:pos></gml:Point>'));
SELECT 'double_28', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1. 23</gml:pos></gml:Point>'));
SELECT 'double_29', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1.23 E2</gml:pos></gml:Point>'));
SELECT 'double_30', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 -1.23E 2</gml:pos></gml:Point>'));

-- ERROR: Junk 
SELECT 'double_31', ST_AsEWKT(ST_GeomFromGML('<gml:Point><gml:pos>1 $0%@#$^%#</gml:pos></gml:Point>'));




--
-- Delete inserted spatial data
--
DELETE FROM spatial_ref_sys WHERE srid = 4326;
DELETE FROM spatial_ref_sys WHERE srid = 27562;
DELETE FROM spatial_ref_sys WHERE srid = 27582;
