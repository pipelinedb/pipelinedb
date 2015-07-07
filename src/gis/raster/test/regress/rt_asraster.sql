DROP TABLE IF EXISTS raster_asraster_geom;
DROP TABLE IF EXISTS raster_asraster_rast;
DROP TABLE IF EXISTS raster_asraster_dst;

CREATE TABLE raster_asraster_geom (
	geom geometry
);
CREATE TABLE raster_asraster_rast (
	rast raster
);
CREATE TABLE raster_asraster_dst (
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

		INSERT INTO raster_asraster_rast VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION make_test_raster();

DELETE FROM "spatial_ref_sys" WHERE srid = 992163;
DELETE FROM "spatial_ref_sys" WHERE srid = 993309;
DELETE FROM "spatial_ref_sys" WHERE srid = 993310;
DELETE FROM "spatial_ref_sys" WHERE srid = 994269;

INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (992163,'EPSG',2163,'PROJCS["unnamed",GEOGCS["unnamed ellipse",DATUM["unknown",SPHEROID["unnamed",6370997,0]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Lambert_Azimuthal_Equal_Area"],PARAMETER["latitude_of_center",45],PARAMETER["longitude_of_center",-100],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["Meter",1],AUTHORITY["EPSG","2163"]]','+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (993309,'EPSG',3309,'PROJCS["NAD27 / California Albers",GEOGCS["NAD27",DATUM["North_American_Datum_1927",SPHEROID["Clarke 1866",6378206.4,294.9786982139006,AUTHORITY["EPSG","7008"]],AUTHORITY["EPSG","6267"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4267"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",34],PARAMETER["standard_parallel_2",40.5],PARAMETER["latitude_of_center",0],PARAMETER["longitude_of_center",-120],PARAMETER["false_easting",0],PARAMETER["false_northing",-4000000],AUTHORITY["EPSG","3309"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=aea +lat_1=34 +lat_2=40.5 +lat_0=0 +lon_0=-120 +x_0=0 +y_0=-4000000 +ellps=clrk66 +datum=NAD27 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (993310,'EPSG',3310,'PROJCS["NAD83 / California Albers",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],UNIT["metre",1,AUTHORITY["EPSG","9001"]],PROJECTION["Albers_Conic_Equal_Area"],PARAMETER["standard_parallel_1",34],PARAMETER["standard_parallel_2",40.5],PARAMETER["latitude_of_center",0],PARAMETER["longitude_of_center",-120],PARAMETER["false_easting",0],PARAMETER["false_northing",-4000000],AUTHORITY["EPSG","3310"],AXIS["X",EAST],AXIS["Y",NORTH]]','+proj=aea +lat_1=34 +lat_2=40.5 +lat_0=0 +lon_0=-120 +x_0=0 +y_0=-4000000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs ');
INSERT INTO "spatial_ref_sys" ("srid","auth_name","auth_srid","srtext","proj4text") VALUES (994269,'EPSG',4269,'GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]]','+proj=longlat +ellps=GRS80 +datum=NAD83 +no_defs ');

INSERT INTO raster_asraster_geom VALUES (
ST_GeomFromText('MULTIPOLYGON(((-172210.499109288 114987.660953018,-175453.086381862 29201.5994536821,-151944.038528546 28257.4637483698,-151755.193144738 64618.6592845297,-129779.244489461 63766.2346307114,-132720.730482521 29365.7452160339,-110176.183408147 28076.2457866343,-113336.283431208 112064.985603184,-135659.619600536 112878.300914729,-134301.95687566 79576.8821948012,-153850.618867315 80395.4252778995,-151346.215838074 112678.410158427,-172210.499109288 114987.660953018)),((-86135.5150847774 77502.7616508612,-87105.1850870571 30678.0039829779,-69362.3449961895 29072.3373203999,-70858.5814585458 78310.0439012805,-86135.5150847774 77502.7616508612)),((-86888.5102830273 96546.8546876945,-86065.7795470885 84169.9977753228,-70801.2145468401 84976.5822106288,-72118.6159803197 97829.7405064492,-86888.5102830273 96546.8546876945)),((-50136.8809020698 111909.445130098,-48631.3614059008 44728.8885465469,-36172.0195739627 45621.806341459,-39695.018962698 109480.225649309,-50136.8809020698 111909.445130098)),((-47695.3501850868 40894.9976787795,-47761.6362577873 29399.0052930373,-34799.4262271112 30293.0638067261,-35717.8219710071 39877.2161100041,-47695.3501850868 40894.9976787795)))', 993310)
);

-- scale or width & height, pixtype, value and nodata
INSERT INTO raster_asraster_dst (rid, rast) VALUES (
	1.0, (SELECT ST_AsRaster(
		NULL,
		100, 100
	))
), (
	1.1, (SELECT ST_AsRaster(
		geom,
		100, 100
	) FROM raster_asraster_geom)
), (
	1.2, (SELECT ST_AsRaster(
		geom,
		100., -100.
	) FROM raster_asraster_geom)
), (
	1.3, (SELECT ST_AsRaster(
		geom,
		500, 500
	) FROM raster_asraster_geom)
), (
	1.4, (SELECT ST_AsRaster(
		geom,
		1000., -1000.
	) FROM raster_asraster_geom)
), (
	1.5, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BSI'
	) FROM raster_asraster_geom)
), (
	1.6, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'16BUI'
	) FROM raster_asraster_geom)
), (
	1.7, (SELECT ST_AsRaster(
		geom,
		100., -100.,
		'32BF'
	) FROM raster_asraster_geom)
), (
	1.8, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['8BSI']
	) FROM raster_asraster_geom)
), (
	1.9, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['16BUI']
	) FROM raster_asraster_geom)
), (
	1.10, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['32BF']
	) FROM raster_asraster_geom)
), (
	1.11, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['8BSI']
	) FROM raster_asraster_geom)
), (
	1.12, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'16BUI'
	) FROM raster_asraster_geom)
), (
	1.13, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['32BF'],
		ARRAY[255]
	) FROM raster_asraster_geom)
), (
	1.14, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['32BF'],
		ARRAY[255],
		ARRAY[1]
	) FROM raster_asraster_geom)
), (
	1.15, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['32BF'],
		ARRAY[255],
		NULL
	) FROM raster_asraster_geom)
), (
	1.16, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['32BF'],
		ARRAY[255],
		ARRAY[NULL]::double precision[]
	) FROM raster_asraster_geom)
), (
	1.17, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['32BF', '16BUI'],
		ARRAY[255, 1],
		ARRAY[NULL, 0]::double precision[]
	) FROM raster_asraster_geom)
), (
	1.18, (SELECT ST_AsRaster(
		geom,
		10, 10,
		ARRAY['8BUI', '16BUI'],
		ARRAY[255, 255],
		ARRAY[0, NULL]::double precision[]
	) FROM raster_asraster_geom)
), (
	1.19, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['32BF', '16BUI', '64BF'],
		ARRAY[255, 1, -1],
		ARRAY[NULL, 0, NULL]::double precision[]
	) FROM raster_asraster_geom)
), (
	1.20, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		ARRAY['1BB', '2BUI'],
		ARRAY[1, 1],
		ARRAY[1, 0]::double precision[]
	) FROM raster_asraster_geom)
);

-- upper left
INSERT INTO raster_asraster_dst (rid, rast) VALUES (
	2.0, (SELECT ST_AsRaster(
		NULL,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-175453
	))
), (
	2.1, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-175453
	) FROM raster_asraster_geom)
), (
	2.2, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-175400, 115000
	) FROM raster_asraster_geom)
), (
	2.3, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-170000, 114988
	) FROM raster_asraster_geom)
), (
	2.4, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-170000, 110000
	) FROM raster_asraster_geom)
), (
	2.5, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		'8BUI',
		255,
		0,
		-179000, 119000
	) FROM raster_asraster_geom)
), (
	2.6, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		-179000, 119000
	) FROM raster_asraster_geom)
), (
	2.7, (SELECT ST_AsRaster(
		geom,
		100, 100,
		ARRAY['8BUI'],
		ARRAY[255],
		ARRAY[0],
		-179000, 119000
	) FROM raster_asraster_geom)
);

-- skew
INSERT INTO raster_asraster_dst (rid, rast) VALUES (
	3.0, (SELECT ST_AsRaster(
		NULL,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		0
	))
), (
	3.1, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		0
	) FROM raster_asraster_geom)
), (
	3.2, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		0, 0
	) FROM raster_asraster_geom)
), (
	3.3, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		1, 0
	) FROM raster_asraster_geom)
), (
	3.4, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		0, 1
	) FROM raster_asraster_geom)
), (
	3.5, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		10, -5
	) FROM raster_asraster_geom)
), (
	3.6, (SELECT ST_AsRaster(
		geom,
		100, 100,
		'8BUI',
		255,
		0,
		NULL, NULL,
		-5, 10
	) FROM raster_asraster_geom)
);

-- snap to grid
INSERT INTO raster_asraster_dst (rid, rast) VALUES (
	4.0, (
		SELECT ST_AsRaster(
			NULL,
			rast
		)
		FROM raster_asraster_rast
	)
), (
	4.1, (
		SELECT ST_AsRaster(
			geom,
			rast
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.2, (
		SELECT ST_AsRaster(
			geom,
			rast,
			'64BF'
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.3, (
		SELECT ST_AsRaster(
			geom,
			rast,
			'16BUI',
			13
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.4, (
		SELECT ST_AsRaster(
			geom,
			rast,
			'16BUI',
			13,
			NULL
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.5, (
		SELECT ST_AsRaster(
			geom,
			rast,
			ARRAY['16BUI'],
			ARRAY[13]
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.6, (
		SELECT ST_AsRaster(
			geom,
			rast,
			ARRAY['16BUI'],
			ARRAY[13],
			ARRAY[NULL]::double precision[]
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.7, (
		SELECT ST_AsRaster(
			geom,
			rast,
			ARRAY['16BUI'],
			ARRAY[13],
			ARRAY[0]
		)
		FROM raster_asraster_geom, raster_asraster_rast
	)
), (
	4.8, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		0, 0,
		ARRAY['16BUI'],
		ARRAY[13],
		ARRAY[0]
	)
	FROM raster_asraster_geom)
), (
	4.9, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		-175453, 114987,
		ARRAY['16BUI'],
		ARRAY[13],
		ARRAY[0]
	)
	FROM raster_asraster_geom)
), (
	4.10, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		-100, 100,
		ARRAY['16BUI'],
		ARRAY[13],
		ARRAY[0]
	)
	FROM raster_asraster_geom)
), (
	4.11, (SELECT ST_AsRaster(
		geom,
		1000., -1000.,
		-100, 100,
		'16BUI',
		13,
		0
	)
	FROM raster_asraster_geom)
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
	pixeltype,
	round(nodatavalue::numeric, 3) AS nodatavalue,
	count > 0 AS count_check,
	round(min::numeric, 3) AS min,
	round(max::numeric, 3) AS max,
	same_alignment
FROM (
	SELECT
		d.rid,
		(ST_MetaData(d.rast)).*,
		(ST_SummaryStats(d.rast)).*,
		(ST_BandMetaData(d.rast)).*,
		CASE
			WHEN d.rid LIKE '4.%'
				THEN ST_SameAlignment(ST_Transform(d.rast, 992163), r.rast)
			ELSE NULL
		END AS same_alignment
	FROM raster_asraster_dst d
	CROSS JOIN raster_asraster_rast r
	ORDER BY d.rid
) foo;

DELETE FROM "spatial_ref_sys" WHERE srid = 992163;
DELETE FROM "spatial_ref_sys" WHERE srid = 993309;
DELETE FROM "spatial_ref_sys" WHERE srid = 993310;
DELETE FROM "spatial_ref_sys" WHERE srid = 994269;

DROP TABLE raster_asraster_geom;
DROP TABLE raster_asraster_rast;
DROP TABLE raster_asraster_dst;
