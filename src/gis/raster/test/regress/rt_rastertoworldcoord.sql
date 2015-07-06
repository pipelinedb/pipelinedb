CREATE TABLE raster_raster2world (
	rid integer,
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster(
	rid integer,
	width integer DEFAULT 2,
	height integer DEFAULT 2,
	ul_x double precision DEFAULT 0,
	ul_y double precision DEFAULT 0,
	skew_x double precision DEFAULT 0,
	skew_y double precision DEFAULT 0,
	initvalue double precision DEFAULT 1,
	nodataval double precision DEFAULT 0
)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);

		INSERT INTO raster_raster2world VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';

-- no skew
SELECT make_test_raster(0, 4, 4, -2, -2);
SELECT make_test_raster(1, 2, 2, 0, 0, 0, 0, 2);
SELECT make_test_raster(2, 2, 2, 1, -1, 0, 0, 3);
SELECT make_test_raster(3, 2, 2, 1, 1, 0, 0, 4);
SELECT make_test_raster(4, 2, 2, 2, 2, 0, 0, 5);

-- skew
SELECT make_test_raster(10, 4, 4, -2, -2, 1, -1);
SELECT make_test_raster(11, 2, 2, 0, 0, 1, -1, 2);
SELECT make_test_raster(12, 2, 2, 1, -1, 1, -1, 3);
SELECT make_test_raster(13, 2, 2, 1, 1, 1, -1, 4);
SELECT make_test_raster(14, 2, 2, 2, 2, 1, -1, 5);

DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);

SELECT
	rid,
	(ST_RasterToWorldCoord(rast, 1, 1)).*
FROM raster_raster2world;
SELECT
	rid,
	(ST_RasterToWorldCoord(rast, 1, 2)).*
FROM raster_raster2world;
SELECT
	rid,
	(ST_RasterToWorldCoord(rast, 2, 2)).*
FROM raster_raster2world;
SELECT
	rid,
	(ST_RasterToWorldCoord(rast, 0, 0)).*
FROM raster_raster2world;
SELECT
	rid,
	(ST_RasterToWorldCoord(rast, -1, 0)).*
FROM raster_raster2world;

SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 1, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 1, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 2, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 0, 0)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, -1, 0)
FROM raster_raster2world;

SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, 0)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordX(rast, -1)
FROM raster_raster2world;

SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 1, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 1, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 2, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 0, 0)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, -1, 0)
FROM raster_raster2world;

SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 1)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 2)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, 0)
FROM raster_raster2world;
SELECT
	rid,
	ST_RasterToWorldCoordY(rast, -1)
FROM raster_raster2world;

DROP TABLE raster_raster2world;
