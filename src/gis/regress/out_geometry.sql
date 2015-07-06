--
-- spatial_ref_sys data
--
DELETE FROM "spatial_ref_sys";
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","proj4text") VALUES (4326,'EPSG',4326,'+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs ');

--- EPSG 1021892 : Bogota 1975 / Colombia Bogota zone (deprecated)
INSERT INTO "spatial_ref_sys" ("srid", "proj4text") VALUES (102189, '+proj=tmerc +lat_0=4.599047222222222 +lon_0=-74.08091666666667 +k=1.000000 +x_0=1000000 +y_0=1000000 +ellps=intl +towgs84=307,304,-318,0,0,0,0 +units=m +no_defs ');


--
-- GML
--

-- Empty Geometry
SELECT 'gml_empty_geom', ST_AsGML(GeomFromEWKT(NULL));

-- Precision
SELECT 'gml_precision_01', ST_AsGML(GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), -2);
SELECT 'gml_precision_02', ST_AsGML(GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), 19);

-- Version
SELECT 'gml_version_01', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'gml_version_02', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'gml_version_03', ST_AsGML(21, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'gml_version_04', ST_AsGML(-4, GeomFromEWKT('SRID=4326;POINT(1 1)'));

-- Option
SELECT 'gml_option_01', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 1)'), 0, 0);
SELECT 'gml_option_02', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 1)'), 0, 1);
SELECT 'gml_option_03', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 1)'), 0, 2);

-- Deegree data
SELECT 'gml_deegree_01', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 0);
SELECT 'gml_deegree_02', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16);
SELECT 'gml_deegree_03', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16);
SELECT 'gml_deegree_04', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2 3)'), 0, 0);
SELECT 'gml_deegree_05', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 2 3)'), 0, 16);
SELECT 'gml_deegree_06', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2 3)'), 0, 16);

-- Prefix
SELECT 'gml_prefix_01', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16, '');
SELECT 'gml_prefix_02', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16, '');
SELECT 'gml_prefix_03', ST_AsGML(2, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16, 'foo');
SELECT 'gml_prefix_04', ST_AsGML(3, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 16, 'foo');

-- LineString
SELECT 'gml_shortline_01', ST_AsGML(3, GeomFromEWKT('LINESTRING(1 2, 3 4)'), 0, 6, '');
SELECT 'gml_shortline_02', ST_AsGML(3, GeomFromEWKT('LINESTRING(1 2, 3 4)'), 0, 2, '');
SELECT 'gml_shortline_03', ST_AsGML(3, GeomFromEWKT('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))'), 0, 6, '');
SELECT 'gml_shortline_04', ST_AsGML(3, GeomFromEWKT('MULTILINESTRING((1 2, 3 4), (5 6, 7 8))'), 0, 2, '');

-- CIRCULARSTRING / COMPOUNDCURVE / CURVEPOLYGON / MULTISURFACE / MULTICURVE
SELECT 'gml_out_curve_01', ST_AsGML( 3, GeomFromEWKT('CIRCULARSTRING(-2 0,0 2,2 0,0 2,2 4)'));
SELECT 'gml_out_curve_02', ST_ASGML( 3, GeomFromEWKT('COMPOUNDCURVE(CIRCULARSTRING(0 0,1 1,1 0),(1 0,0 1))'));
SELECT 'gml_out_curve_03', ST_AsGML( 3, GeomFromEWKT('CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0))'));
SELECT 'gml_out_curve_04', ST_AsGML( 3, GeomFromEWKT('MULTICURVE((5 5,3 5,3 3,0 3),CIRCULARSTRING(0 0,2 1,2 2))'));
SELECT 'gml_out_curve_05', ST_AsGML( 3, GeomFromEWKT('MULTISURFACE(CURVEPOLYGON(CIRCULARSTRING(-2 0,-1 -1,0 0,1 -1,2 0,0 2,-2 0),(-1 0,0 0.5,1 0,0 1,-1 0)),((7 8,10 10,6 14,4 11,7 8)))'));

--
-- KML
--

-- SRID
SELECT 'kml_srid_01', ST_AsKML(GeomFromEWKT('SRID=10;POINT(0 1)'));
SELECT 'kml_srid_02', ST_AsKML(GeomFromEWKT('POINT(0 1)'));

-- Empty Geometry
SELECT 'kml_empty_geom', ST_AsKML(GeomFromEWKT(NULL));

-- Precision
SELECT 'kml_precision_01', ST_AsKML(ST_GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), -2);
SELECT 'kml_precision_02', ST_AsKML(ST_GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), 19);

-- Version
SELECT 'kml_version_01', ST_AsKML(2, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'kml_version_02', ST_AsKML(3, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'kml_version_03', ST_AsKML(-4, GeomFromEWKT('SRID=4326;POINT(1 1)'));

-- Prefix
SELECT 'kml_prefix_01', ST_AsKML(2, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, '');
SELECT 'kml_prefix_02', ST_AsKML(2, GeomFromEWKT('SRID=4326;POINT(1 2)'), 0, 'kml');

-- Projected 
-- National Astronomical Observatory of Colombia - Bogota, Colombia (Placemark)
SELECT 'kml_projection_01', ST_AsKML(ST_GeomFromEWKT('SRID=102189;POINT(1000000 1000000)'), 3);

--
-- Encoded Polyline
--
SELECT 'encoded_polyline_01', ST_AsEncodedPolyline(GeomFromEWKT('SRID=4326;LINESTRING(-120.2 38.5,-120.95 40.7,-126.453 43.252)'));
SELECT 'encoded_polyline_02', ST_AsEncodedPolyline(GeomFromEWKT('SRID=4326;MULTIPOINT(-120.2 38.5,-120.95 40.7,-126.453 43.252)'));
SELECT 'encoded_polyline_03', ST_AsEncodedPolyline(GeomFromEWKT('LINESTRING(-120.2 38.5,-120.95 40.7,-126.453 43.252)'));
SELECT 'encoded_polyline_04', ST_AsEncodedPolyline(GeomFromEWKT('SRID=4326;LINESTRING(-120.234467 38.5,-120.95 40.7343495,-126.453 43.252)'), 6);

--
-- SVG
--

-- Empty Geometry
SELECT 'svg_empty_geom', ST_AsSVG(GeomFromEWKT(NULL));

-- Option
SELECT 'svg_option_01', ST_AsSVG(GeomFromEWKT('LINESTRING(1 1, 4 4, 5 7)'), 0);
SELECT 'svg_option_02', ST_AsSVG(GeomFromEWKT('LINESTRING(1 1, 4 4, 5 7)'), 1);
SELECT 'svg_option_03', ST_AsSVG(GeomFromEWKT('LINESTRING(1 1, 4 4, 5 7)'), 0, 0);
SELECT 'svg_option_04', ST_AsSVG(GeomFromEWKT('LINESTRING(1 1, 4 4, 5 7)'), 1, 0);

-- Precision
SELECT 'svg_precision_01', ST_AsSVG(GeomFromEWKT('POINT(1.1111111 1.1111111)'), 1, -2);
SELECT 'svg_precision_02', ST_AsSVG(GeomFromEWKT('POINT(1.1111111 1.1111111)'), 1, 19);


--
-- GeoJson
--

-- Empty Geometry
SELECT 'geojson_empty_geom', ST_AsGeoJson(GeomFromEWKT(NULL));

-- Precision
SELECT 'geojson_precision_01', ST_AsGeoJson(GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), -2);
SELECT 'geojson_precision_02', ST_AsGeoJson(GeomFromEWKT('SRID=4326;POINT(1.1111111 1.1111111)'), 19);

-- Version
SELECT 'geojson_version_01', ST_AsGeoJson(1, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'geojson_version_02', ST_AsGeoJson(21, GeomFromEWKT('SRID=4326;POINT(1 1)'));
SELECT 'geojson_version_03', ST_AsGeoJson(-4, GeomFromEWKT('SRID=4326;POINT(1 1)'));

-- CRS
SELECT 'geojson_crs_01', ST_AsGeoJson(GeomFromEWKT('SRID=4326;POINT(1 1)'), 0, 2);
SELECT 'geojson_crs_02', ST_AsGeoJson(GeomFromEWKT('SRID=0;POINT(1 1)'), 0, 2);
SELECT 'geojson_crs_03', ST_AsGeoJson(GeomFromEWKT('SRID=4326;POINT(1 1)'), 0, 4);
SELECT 'geojson_crs_04', ST_AsGeoJson(GeomFromEWKT('SRID=0;POINT(1 1)'), 0, 4);
SELECT 'geojson_crs_05', ST_AsGeoJson(GeomFromEWKT('SRID=1;POINT(1 1)'), 0, 2);
SELECT 'geojson_crs_06', ST_AsGeoJson(GeomFromEWKT('SRID=1;POINT(1 1)'), 0, 4);

-- Bbox
SELECT 'geojson_bbox_01', ST_AsGeoJson(GeomFromEWKT('LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0);
SELECT 'geojson_bbox_02', ST_AsGeoJson(GeomFromEWKT('LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 1);
SELECT 'geojson_bbox_03', ST_AsGeoJson(GeomFromEWKT('LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 3);
SELECT 'geojson_bbox_04', ST_AsGeoJson(GeomFromEWKT('LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 5);

-- CRS and Bbox
SELECT 'geojson_options_01', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 0);
SELECT 'geojson_options_02', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0);
SELECT 'geojson_options_03', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 1);
SELECT 'geojson_options_04', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 1);
SELECT 'geojson_options_05', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 2);
SELECT 'geojson_options_06', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 2);
SELECT 'geojson_options_07', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 3);
SELECT 'geojson_options_08', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 3);
SELECT 'geojson_options_09', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 4);
SELECT 'geojson_options_10', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 4);
SELECT 'geojson_options_11', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 5);
SELECT 'geojson_options_12', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 5);
SELECT 'geojson_options_13', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 6);
SELECT 'geojson_options_14', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 6);
SELECT 'geojson_options_15', ST_AsGeoJson(GeomFromEWKT('SRID=0;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 7);
SELECT 'geojson_options_16', ST_AsGeoJson(GeomFromEWKT('SRID=4326;LINESTRING(1 1, 2 2, 3 3, 4 4)'), 0, 7);

-- Out and in to PostgreSQL native geometric types
WITH p AS ( SELECT '((0,0),(0,1),(1,1),(1,0),(0,0))'::text AS p ) 
  SELECT 'pgcast_01', p = p::polygon::geometry::polygon::text FROM p;
WITH p AS ( SELECT '[(0,0),(1,1)]'::text AS p ) 
  SELECT 'pgcast_02', p = p::path::geometry::path::text FROM p;
WITH p AS ( SELECT '(1,1)'::text AS p ) 
  SELECT 'pgcast_03', p = p::point::geometry::point::text FROM p;
SELECT 'pgcast_03','POLYGON EMPTY'::geometry::polygon IS NULL;
SELECT 'pgcast_04','LINESTRING EMPTY'::geometry::path IS NULL;
SELECT 'pgcast_05','POINT EMPTY'::geometry::point IS NULL;
SELECT 'pgcast_06',ST_AsText('((0,0),(0,1),(1,1),(1,0))'::polygon::geometry);

--
-- Delete inserted spatial data
--
DELETE FROM spatial_ref_sys;
