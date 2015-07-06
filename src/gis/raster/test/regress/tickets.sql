/******************************************************************************
 #1485
******************************************************************************/

SELECT '#1485', count(*) FROM geometry_columns
WHERE f_table_name = 'raster_columns';

/******************************************************************************
 #2532
******************************************************************************/

SELECT '#2532.1', NULL::raster @ null::geometry;
SELECT '#2532.2', NULL::geometry @ null::raster;

/******************************************************************************
 #2911
******************************************************************************/

WITH data AS ( SELECT '#2911' l, ST_Metadata(ST_Rescale(
 ST_AddBand(
  ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0, 0, 0),
  1, '8BUI', 0, 0
 ),
 2.0,
 -2.0
 )) m
) SELECT l, (m).* FROM data;

/******************************************************************************
 #3006
******************************************************************************/

SET client_min_messages TO ERROR;

DROP TABLE IF EXISTS test_raster_scale_regular;
DROP TABLE IF EXISTS test_raster_scale_big;
DROP TABLE IF EXISTS test_raster_scale_small;

CREATE TABLE test_raster_scale_regular (
  rid integer,
 	rast raster
);

CREATE TABLE test_raster_scale_big (
  rid integer,
 	rast raster
);

CREATE TABLE test_raster_scale_small (
 	rid integer,
 	rast raster
);

CREATE OR REPLACE FUNCTION make_test_raster(
  table_suffix text,
	rid integer,
  scale_x double precision,
  scale_y double precision DEFAULT 1.0
)
RETURNS void
AS $$
DECLARE
	rast raster;
 	width integer := 2;
 	height integer := 2;
 	ul_x double precision := 0;
 	ul_y double precision := 0;
 	skew_x double precision := 0;
 	skew_y double precision := 0;
 	initvalue double precision := 1;
 	nodataval double precision := 0;
BEGIN
	rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, scale_x, scale_y, skew_x, skew_y, 0);
	rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);

	EXECUTE format('INSERT INTO test_raster_scale_%s VALUES (%L, %L)', table_suffix, rid, rast);
	RETURN;
END;
$$ LANGUAGE 'plpgsql';

SELECT make_test_raster('regular', 0, 1);
SELECT make_test_raster('regular', 1, 1.0000001);
SELECT make_test_raster('regular', 2, 0.9999999);
SELECT AddRasterConstraints('test_raster_scale_regular'::name, 'rast'::name, 'scale_x', 'scale_y');
SELECT r_table_name, r_raster_column, scale_x, scale_y FROM raster_columns
  WHERE  r_raster_column = 'rast' AND r_table_name = 'test_raster_scale_regular';

-- Issues enforce_scaley_rast constraint violation
SELECT make_test_raster('regular', 3, 1.001, 0.9999999);

SELECT make_test_raster('big', 0, -1234567890123456789.0);
SELECT AddRasterConstraints('test_raster_scale_big'::name, 'rast'::name, 'scale_x', 'scale_y');
SELECT r_table_name, r_raster_column, scale_x, scale_y FROM raster_columns
  WHERE  r_raster_column = 'rast' AND r_table_name = 'test_raster_scale_big';

-- Issues enforce_scalex_rast constraint violation
SELECT make_test_raster('big', 1, -12345678901234567890.0);

SELECT make_test_raster('small', 0, 0.00001);
SELECT make_test_raster('small', 1, 0.000011);
SELECT make_test_raster('small', 2, 0.00000999);
SELECT AddRasterConstraints('test_raster_scale_small'::name, 'rast'::name, 'scale_x', 'scale_y');
SELECT r_table_name, r_raster_column, scale_x, scale_y FROM raster_columns
  WHERE  r_raster_column = 'rast' AND r_table_name = 'test_raster_scale_small';

-- Issues enforce_scaley_rast constraint violation
SELECT make_test_raster('small', 3, 0.00001, 1.00001);

DROP FUNCTION make_test_raster(text, integer, double precision, double precision);
DROP TABLE IF EXISTS test_raster_scale_regular;
DROP TABLE IF EXISTS test_raster_scale_big;
DROP TABLE IF EXISTS test_raster_scale_small;

SET client_min_messages TO DEFAULT;
