DROP TABLE IF EXISTS raster_gdalwarp_src;
DROP TABLE IF EXISTS raster_gdalwarp_dst;
CREATE TABLE raster_gdalwarp_src (
	rast raster
);
CREATE TABLE raster_gdalwarp_dst (
	rid varchar,
	rast raster
);

CREATE OR REPLACE FUNCTION make_test_raster()
	RETURNS void
	AS $$
	DECLARE
		width int := 10;
		height int := 10;
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, -500000, 600000, 1000, -1000, 0, 0, 992163);
		rast := ST_AddBand(rast, 1, '64BF', 0, 0);

		FOR x IN 1..width LOOP
			FOR y IN 1..height LOOP
				rast := ST_SetValue(rast, 1, x, y, ((x::double precision * y) + (x + y) + (x + y * x)) / (x + y + 1));
			END LOOP;
		END LOOP;

		INSERT INTO raster_gdalwarp_src VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION make_test_raster();

DELETE FROM "spatial_ref_sys" WHERE srid = 992163;
DELETE FROM "spatial_ref_sys" WHERE srid = 993309;
DELETE FROM "spatial_ref_sys" WHERE srid = 993310;
DELETE FROM "spatial_ref_sys" WHERE srid = 994269;
DELETE FROM "spatial_ref_sys" WHERE srid = 984269;
DELETE FROM "spatial_ref_sys" WHERE srid = 974269;

INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (992163,'EPSG',2163,'PROJCS["unnamed",GEOGCS["unnamed ellipse",DATUM["unknown",SPHEROID["unnamed",6370997,0]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Lambert_Azimuthal_Equal_Area"],PARAMETER["latitude_of_center",45],PARAMETER["longitude_of_center",-100],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1],AUTHORITY["EPSG","2163"]]','+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (993309,'EPSG',3309,'PROJCS["NAD27 / California Albers",GEOGCS["NAD27",DATUM["North_American_Datum_1927",SPHEROID["Clarke 1866",6378206.4,294.9786982139006,AUTHORITY["EPSG","7008"]],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4267"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",34],PARAMETER["standard_parallel_2",40.5],PARAMETER["latitude_of_center",0],PARAMETER["longitude_of_center",-120],PARAMETER["false_easting",0],PARAMETER["false_northing",-4000000],AUTHORITY["EPSG","3309"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=aea +lat_1=34 +lat_2=40.5 +lat_0=0 +lon_0=-120 +x_0=0 +y_0=-4000000 +ellps=clrk66 +datum=NAD27 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (993310,'EPSG',3310,'PROJCS["NAD83 / California Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",34],PARAMETER["standard_parallel_2",40.5],PARAMETER["latitude_of_center",0],PARAMETER["longitude_of_center",-120],PARAMETER["false_easting",0],PARAMETER["false_northing",-4000000],AUTHORITY["EPSG","3310"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=aea +lat_1=34 +lat_2=40.5 +lat_0=0 +lon_0=-120 +x_0=0 +y_0=-4000000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (994269,'EPSG',4269,'GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]]','+proj=longlat +ellps=GRS80 +datum=NAD83 +no_defs ');

INSERT INTO "spatial_ref_sys" ("srid","auth_name","srtext","proj4text") VALUES (984269,'EPSG','GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]]','+proj=longlat +ellps=GRS80 +datum=NAD83 +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","srtext") VALUES (974269,'GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]]');

-- _st_gdalwarp
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	0.0, (SELECT _st_gdalwarp(
		NULL
	))
), (
	0.1, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbour', 0.125,
		993310
	) FROM raster_gdalwarp_src)
), (
	0.2, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbour', 0.125,
		993309
	) FROM raster_gdalwarp_src)
), (
	0.3, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbour', 0.125,
		994269
	) FROM raster_gdalwarp_src)
), (
	0.4, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		993310,
		500., 500.,
		NULL, NULL,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	0.5, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		100., NULL
	) FROM raster_gdalwarp_src)
), (
	0.6, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL::double precision, 100.
	) FROM raster_gdalwarp_src)
), (
	0.7, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		500., 500.
	) FROM raster_gdalwarp_src)
), (
	0.8, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		250., 250.,
		NULL, NULL,
		NULL, NULL
	) FROM raster_gdalwarp_src)
), (
	0.9, (SELECT _st_gdalwarp(
		rast,
		'Bilinear', 0,
		NULL,
		250., 250.,
		NULL, NULL,
		NULL, NULL
	) FROM raster_gdalwarp_src)
), (
	0.10, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		-500000, 600000
	) FROM raster_gdalwarp_src)
), (
	0.11, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		-500001, 600000
	) FROM raster_gdalwarp_src)
), (
	0.12, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		-500000, 600009
	) FROM raster_gdalwarp_src)
), (
	0.13, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		-500100, 599950
	) FROM raster_gdalwarp_src)
), (
	0.14, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	0.15, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		121., 121.,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	0.16, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		993310,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	0.17, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		993309,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	0.18, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		NULL, NULL,
		3, 3
	) FROM raster_gdalwarp_src)
), (
	0.19, (SELECT _st_gdalwarp(
		rast,
		'Cubic', 0,
		993310,
		NULL, NULL,
		NULL, NULL,
		3, 3
	) FROM raster_gdalwarp_src)
), (
	0.20, (SELECT _st_gdalwarp(
		rast,
		'Bilinear', 0.125,
		993309,
		NULL, NULL,
		NULL, NULL,
		1, 3
	) FROM raster_gdalwarp_src)
), (
	0.21, (SELECT _st_gdalwarp(
		rast,
		'Cubic', 0,
		993310,
		500., 500.,
		NULL, NULL,
		3, 3
	) FROM raster_gdalwarp_src)
), (
	0.22, (SELECT _st_gdalwarp(
		rast,
		'CubicSpline', 0.125,
		993310,
		500., 500.,
		-12048, 14682,
		0, 6
	) FROM raster_gdalwarp_src)
), (
	0.23, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		984269
	) FROM raster_gdalwarp_src)
), (
	0.24, (SELECT _st_gdalwarp(
		rast,
		'NearestNeighbor', 0.125,
		974269
	) FROM raster_gdalwarp_src)
), (
	0.25, (SELECT _st_gdalwarp(
		ST_SetGeoReference(ST_SetSRID(rast, 0), '1 0 0 -1 0 0'),
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		NULL, NULL,
		NULL, NULL,
		5, 5
	) FROM raster_gdalwarp_src)
), (
	0.26, (SELECT _st_gdalwarp(
		ST_SetGeoReference(ST_SetSRID(rast, 0), '1 0 0 -1 0 0'),
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		NULL, NULL,
		NULL, NULL,
		2, 2
	) FROM raster_gdalwarp_src)
), (
	0.27, (SELECT _st_gdalwarp(
		ST_SetGeoReference(ST_SetSRID(rast, 0), '1 0 0 -1 0 0'),
		'NearestNeighbor', 0.125,
		NULL,
		NULL, NULL,
		NULL, NULL,
		NULL, NULL,
		100, 100
	) FROM raster_gdalwarp_src)
);

-- ST_Resample
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	1.0, (SELECT ST_Resample(
		NULL
	))
), (
	1.1, (SELECT ST_Resample(
		rast
	) FROM raster_gdalwarp_src)
), (
	1.2, (SELECT ST_Resample(
		rast,
		500., 500.,
		NULL, NULL,
		0, 0,
		'NearestNeighbor', 0.125
	) FROM raster_gdalwarp_src)
), (
	1.3, (SELECT ST_Resample(
		rast,
		100., NULL
	) FROM raster_gdalwarp_src)
), (
	1.4, (SELECT ST_Resample(
		rast,
		NULL::double precision, 100.
	) FROM raster_gdalwarp_src)
), (
	1.5, (SELECT ST_Resample(
		rast,
		500., 500.
	) FROM raster_gdalwarp_src)
), (
	1.6, (SELECT ST_Resample(
		rast,
		250., 250.,
		NULL, NULL,
		NULL, NULL
	) FROM raster_gdalwarp_src)
), (
	1.7, (SELECT ST_Resample(
		rast,
		250., 250.,
		NULL, NULL,
		NULL, NULL,
		'Bilinear', 0
	) FROM raster_gdalwarp_src)
), (
	1.8, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL::double precision,
		-500000, 600000
	) FROM raster_gdalwarp_src)
), (
	1.9, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL::double precision,
		-500001, 600000
	) FROM raster_gdalwarp_src)
), (
	1.10, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL::double precision,
		-500000, 600009
	) FROM raster_gdalwarp_src)
), (
	1.11, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL::double precision,
		-500100, 599950
	) FROM raster_gdalwarp_src)
), (
	1.12, (SELECT ST_Resample(
		rast,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	1.13, (SELECT ST_Resample(
		rast,
		121., 121.,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	1.14, (SELECT ST_Resample(
		rast,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	1.15, (SELECT ST_Resample(
		rast,
		50., 50.,
		-290, 7
	) FROM raster_gdalwarp_src)
), (
	1.16, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL,
		NULL, NULL,
		3, 3
	) FROM raster_gdalwarp_src)
), (
	1.17, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL,
		NULL, NULL,
		3, 3,
		'Cubic', 0
	) FROM raster_gdalwarp_src)
), (
	1.18, (SELECT ST_Resample(
		rast,
		NULL::double precision, NULL,
		NULL, NULL,
		1, 3,
		'Bilinear'
	) FROM raster_gdalwarp_src)
), (
	1.19, (SELECT ST_Resample(
		rast,
		500., 500.,
		NULL, NULL,
		3, 3,
		'Cubic', 0
	) FROM raster_gdalwarp_src)
), (
	1.20, (SELECT ST_Resample(
		rast,
		500., 500.,
		-12048, 14682,
		0, 6,
		'CubicSpline'
	) FROM raster_gdalwarp_src)
), (
	1.21, (SELECT ST_Resample(
		rast,
		ST_MakeEmptyRaster(5, 5, -654321, 123456, 50, -100, 3, 0, 992163)
	) FROM raster_gdalwarp_src)
), (
	1.22, (SELECT ST_Resample(
		rast,
		ST_MakeEmptyRaster(5, 5, -654321, 123456, 50, -100, 3, 0, 992163),
		TRUE
	) FROM raster_gdalwarp_src)
), (
	1.23, (SELECT ST_Resample(
		rast,
		150::int, 150::int
	) FROM raster_gdalwarp_src)
), (
	1.24, (SELECT ST_Resample(
		rast,
		ST_MakeEmptyRaster(5, 5, -654321, 123456, 100, 100, 0, 0, 992163),
		FALSE
	) FROM raster_gdalwarp_src)
), (
	1.25, (SELECT ST_Resample(
		rast,
		NULL::raster,
		FALSE
	) FROM raster_gdalwarp_src)
);

-- ST_Transform
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	2.1, (SELECT ST_Transform(
		rast,
		993310
	) FROM raster_gdalwarp_src)
), (
	2.2, (SELECT ST_Transform(
		rast,
		993309
	) FROM raster_gdalwarp_src)
), (
	2.3, (SELECT ST_Transform(
		rast,
		994269
	) FROM raster_gdalwarp_src)
), (
	2.4, (SELECT ST_Transform(
		rast,
		993310, NULL
	) FROM raster_gdalwarp_src)
), (
	2.5, (SELECT ST_Transform(
		rast,
		993310, 'Bilinear'
	) FROM raster_gdalwarp_src)
), (
	2.6, (SELECT ST_Transform(
		rast,
		993310, 'Bilinear', NULL::double precision
	) FROM raster_gdalwarp_src)
), (
	2.7, (SELECT ST_Transform(
		rast,
		993310, 'Cubic', 0.0
	) FROM raster_gdalwarp_src)
), (
	2.8, (SELECT ST_Transform(
		rast,
		993310, 'NearestNeighbour', 0.0
	) FROM raster_gdalwarp_src)
), (
	2.9, (SELECT ST_Transform(
		rast,
		993310, 'NearestNeighbor', 0.0
	) FROM raster_gdalwarp_src)
), (
	2.10, (SELECT ST_Transform(
		rast,
		993310, 'NearestNeighbor', 0.125, 500, 500
	) FROM raster_gdalwarp_src)
), (
	2.11, (SELECT ST_Transform(
		rast,
		993309, 'Cubic', 0., 100, 100
	) FROM raster_gdalwarp_src)
), (
	2.12, (SELECT ST_Transform(
		rast,
		993310, 'CubicSpline', 0., 2000, 2000
	) FROM raster_gdalwarp_src)
), (
	2.13, (SELECT ST_Transform(
		rast,
		993310, 'CubicSpline', 0.1, 1500, 1500
	) FROM raster_gdalwarp_src)
), (
	2.14, (SELECT ST_Transform(
		rast,
		993310, 500, 500
	) FROM raster_gdalwarp_src)
), (
	2.15, (SELECT ST_Transform(
		rast,
		993310, 750
	) FROM raster_gdalwarp_src)
);

-- ST_Rescale
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	3.1, (SELECT ST_Rescale(
		rast,
		100, 100
	) FROM raster_gdalwarp_src)
), (
	3.2, (SELECT ST_Rescale(
		rast,
		100
	) FROM raster_gdalwarp_src)
), (
	3.3, (SELECT ST_Rescale(
		rast,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	3.4, (SELECT ST_Rescale(
		rast,
		0
	) FROM raster_gdalwarp_src)
), (
	3.5, (SELECT ST_Rescale(
		rast,
		100, 100,
		'Bilinear', 0
	) FROM raster_gdalwarp_src)
), (
	3.6, (SELECT ST_Rescale(
		rast,
		150, 125,
		'Cubic'
	) FROM raster_gdalwarp_src)
);

-- ST_Reskew
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	4.1, (SELECT ST_Reskew(
		rast,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	4.2, (SELECT ST_Reskew(
		rast,
		1, 1
	) FROM raster_gdalwarp_src)
), (
	4.3, (SELECT ST_Reskew(
		rast,
		0.5, 0
	) FROM raster_gdalwarp_src)
), (
	4.4, (SELECT ST_Reskew(
		rast,
		10
	) FROM raster_gdalwarp_src)
), (
	4.5, (SELECT ST_Reskew(
		rast,
		10,
		'CubicSpline'
	) FROM raster_gdalwarp_src)
), (
	4.6, (SELECT ST_Reskew(
		rast,
		10,
		'Bilinear', 0
	) FROM raster_gdalwarp_src)
);

-- ST_SnapToGrid
INSERT INTO raster_gdalwarp_dst (rid, rast) VALUES (
	5.1, (SELECT ST_SnapToGrid(
		rast,
		-500000, 600000
	) FROM raster_gdalwarp_src)
), (
	5.2, (SELECT ST_SnapToGrid(
		rast,
		0, 0
	) FROM raster_gdalwarp_src)
), (
	5.3, (SELECT ST_SnapToGrid(
		rast,
		-1, 600000
	) FROM raster_gdalwarp_src)
), (
	5.4, (SELECT ST_SnapToGrid(
		rast,
		-500001, 600000
	) FROM raster_gdalwarp_src)
), (
	5.5, (SELECT ST_SnapToGrid(
		rast,
		1, 600000
	) FROM raster_gdalwarp_src)
), (
	5.6, (SELECT ST_SnapToGrid(
		rast,
		-500000, -1
	) FROM raster_gdalwarp_src)
), (
	5.7, (SELECT ST_SnapToGrid(
		rast,
		-500000, -9
	) FROM raster_gdalwarp_src)
), (
	5.8, (SELECT ST_SnapToGrid(
		rast,
		-500000, 1
	) FROM raster_gdalwarp_src)
), (
	5.9, (SELECT ST_SnapToGrid(
		rast,
		-500000, 9
	) FROM raster_gdalwarp_src)
), (
	5.10, (SELECT ST_SnapToGrid(
		rast,
		-5, 1
	) FROM raster_gdalwarp_src)
), (
	5.11, (SELECT ST_SnapToGrid(
		rast,
		9, -9
	) FROM raster_gdalwarp_src)
), (
	5.12, (SELECT ST_SnapToGrid(
		rast,
		-500000, 600000,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.13, (SELECT ST_SnapToGrid(
		rast,
		0, 0,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.14, (SELECT ST_SnapToGrid(
		rast,
		-1, 600000,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.15, (SELECT ST_SnapToGrid(
		rast,
		-500001, 600000,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.16, (SELECT ST_SnapToGrid(
		rast,
		1, 600000,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.17, (SELECT ST_SnapToGrid(
		rast,
		-500000, -1,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.18, (SELECT ST_SnapToGrid(
		rast,
		-500000, -9,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.19, (SELECT ST_SnapToGrid(
		rast,
		-500000, 1,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.20, (SELECT ST_SnapToGrid(
		rast,
		-500000, 9,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.21, (SELECT ST_SnapToGrid(
		rast,
		-5, 1,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.22, (SELECT ST_SnapToGrid(
		rast,
		9, -9,
		50, 50
	) FROM raster_gdalwarp_src)
), (
	5.23, (SELECT ST_SnapToGrid(
		rast,
		0, 0,
		121
	) FROM raster_gdalwarp_src)
), (
	5.24, (SELECT ST_SnapToGrid(
		rast,
		-500000, 1,
		121
	) FROM raster_gdalwarp_src)
), (
	5.25, (SELECT ST_SnapToGrid(
		rast,
		-500000, 9,
		121
	) FROM raster_gdalwarp_src)
), (
	5.26, (SELECT ST_SnapToGrid(
		rast,
		-5, 1,
		121
	) FROM raster_gdalwarp_src)
), (
	5.27, (SELECT ST_SnapToGrid(
		rast,
		9, -9,
		121
	) FROM raster_gdalwarp_src)
);

SELECT
	rid,
	srid,
	width,
	height,
	numbands,
	round(scalex::numeric, 3) AS scalex,
	round(scaley::numeric, 3) AS scaley,
	round(skewx::numeric, 3) AS skewx,
	round(skewy::numeric, 3) AS skewy,
	round(upperleftx::numeric, 3) AS upperleftx,
	round(upperlefty::numeric, 3) AS upperlefty,
	count > 0 AS count_check,
	round(min::numeric, 3) >= 1.667 AS min_check,
	round(max::numeric, 3) <= 100.995 AS max_check
FROM (
	SELECT
		rid,
		(ST_MetaData(rast)).*,
		(ST_SummaryStats(rast)).*
	FROM raster_gdalwarp_dst
	ORDER BY rid
) foo;

DROP TABLE raster_gdalwarp_src;
DROP TABLE raster_gdalwarp_dst;

WITH foo AS (
	SELECT 0 AS rid, ST_AddBand(ST_MakeEmptyRaster(2, 2, -500000, 600000, 100, -100, 0, 0, 992163), 1, '16BUI', 1, 0) AS rast UNION ALL
	SELECT 1, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499800, 600000, 100, -100, 0, 0, 992163), 1, '16BUI', 2, 0) AS rast UNION ALL
	SELECT 2, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499600, 600000, 100, -100, 0, 0, 992163), 1, '16BUI', 3, 0) AS rast UNION ALL

	SELECT 3, ST_AddBand(ST_MakeEmptyRaster(2, 2, -500000, 599800, 100, -100, 0, 0, 992163), 1, '16BUI', 10, 0) AS rast UNION ALL
	SELECT 4, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499800, 599800, 100, -100, 0, 0, 992163), 1, '16BUI', 20, 0) AS rast UNION ALL
	SELECT 5, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499600, 599800, 100, -100, 0, 0, 992163), 1, '16BUI', 30, 0) AS rast UNION ALL

	SELECT 6, ST_AddBand(ST_MakeEmptyRaster(2, 2, -500000, 599600, 100, -100, 0, 0, 992163), 1, '16BUI', 100, 0) AS rast UNION ALL
	SELECT 7, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499800, 599600, 100, -100, 0, 0, 992163), 1, '16BUI', 200, 0) AS rast UNION ALL
	SELECT 8, ST_AddBand(ST_MakeEmptyRaster(2, 2, -499600, 599600, 100, -100, 0, 0, 992163), 1, '16BUI', 300, 0) AS rast
), bar AS (
	SELECT
		ST_Transform(rast, 994269) AS alignto
	FROM foo
	LIMIT 1
), baz AS (
	SELECT
		rid,
		rast,
		ST_Transform(rast, 994269) AS not_aligned,
		ST_Transform(rast, alignto) AS aligned
	FROM foo
	CROSS JOIN bar
)
SELECT
	ST_SameAlignment(rast) AS rast,
	ST_SameAlignment(not_aligned) AS not_aligned,
	ST_SameAlignment(aligned) AS aligned
FROM baz;

DELETE FROM "spatial_ref_sys" WHERE srid = 992163;
DELETE FROM "spatial_ref_sys" WHERE srid = 993309;
DELETE FROM "spatial_ref_sys" WHERE srid = 993310;
DELETE FROM "spatial_ref_sys" WHERE srid = 994269;
DELETE FROM "spatial_ref_sys" WHERE srid = 984269;
DELETE FROM "spatial_ref_sys" WHERE srid = 974269;

-- ST_Resize()
WITH foo AS(
SELECT
	1 AS rid, 
	ST_Resize(
		ST_AddBand(
			ST_MakeEmptyRaster(1000, 1000, 0, 0, 1, -1, 0, 0, 0)
			, 1, '8BUI', 255, 0
		)
		, '50%', '500'
	) AS rast
UNION ALL
SELECT
	2 AS rid, 
	ST_Resize(
		ST_AddBand(
			ST_MakeEmptyRaster(1000, 1000, 0, 0, 1, -1, 0, 0, 0)
			, 1, '8BUI', 255, 0
		)
		, 500, 100
	) AS rast
UNION ALL
SELECT
	3 AS rid, 
	ST_Resize(
		ST_AddBand(
			ST_MakeEmptyRaster(1000, 1000, 0, 0, 1, -1, 0, 0, 0)
			, 1, '8BUI', 255, 0
		)
		, 0.25, 0.8
	) AS rast
UNION ALL
SELECT -- ticket #2188
	4 AS rid, 
	ST_Resize(
		ST_AddBand(
			ST_MakeEmptyRaster(1000, 1000, 0, 0, 1, -1, 0, 0, 0)
			, 1, '8BUI', 255, 0
		)
		, 0.5, 0.5
	) AS rast
), bar AS (
	SELECT rid, ST_Metadata(rast) AS meta, ST_SummaryStats(rast) AS stats FROM foo
)
SELECT rid, (meta).*, (stats).* FROM bar;

-- edge case
WITH foo AS (
	SELECT ST_AddBand(ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast
)
SELECT
	ST_Metadata(ST_Rescale(rast, 2, 2)) AS rescale,
	ST_Metadata(ST_Resize(rast, 0.5, 0.5)) AS resize
FROM foo;
