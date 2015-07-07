CREATE OR REPLACE FUNCTION make_test_raster(
	width integer DEFAULT 10,
	height integer DEFAULT 10,
	ul_x double precision DEFAULT 0,
	ul_y double precision DEFAULT 0,
	skew_x double precision DEFAULT 0,
	skew_y double precision DEFAULT 0,
	numbands integer DEFAULT 1,
	autofill boolean DEFAULT FALSE
)
	RETURNS raster
	AS $$
	DECLARE
		i int;
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		FOR i IN 1..numbands LOOP
			rast := ST_AddBand(rast, i, '8BUI', 0, i);

			IF autofill IS TRUE THEN
				FOR x IN 1..width LOOP
					FOR y IN 1..height LOOP
						rast := ST_SetValue(rast, i, x, y, ((x - 1) * width) + y);
					END LOOP;
				END LOOP;
			END IF;
		END LOOP;

		RETURN rast;
	END;
	$$ LANGUAGE 'plpgsql';

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	ST_SetBandNoDataValue(make_test_raster(10, 10, 0, 0, 0, 0), NULL)
);

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0)
);

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 2),
	2
);

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 3, TRUE),
	3
);

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 5, TRUE),
	4
);

SELECT
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 5, TRUE),
	6
);

SELECT
	bandnum
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 5, TRUE),
	ARRAY[1,2,5]
);

SELECT
	bandnum
	pixeltype,
	round(nodatavalue::numeric, 3),
	isoutdb,
	path
FROM ST_BandMetaData(
	make_test_raster(10, 10, 0, 0, 0, 0, 5, TRUE),
	ARRAY[]::int[]
);

DROP FUNCTION IF EXISTS make_test_raster(
	integer,
	integer,
	double precision,
	double precision,
	double precision,
	double precision,
	integer,
	boolean
);
