DROP TABLE IF EXISTS raster_neighborhood;
CREATE TABLE raster_neighborhood (
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster()
	RETURNS void
	AS $$
	DECLARE
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0, 0, 0);
		rast := ST_AddBand(rast, 1, '8BUI', 1, 0);

		rast := ST_SetValue(rast, 1, 1, 0);
		rast := ST_SetValue(rast, 4, 1, 0);
		rast := ST_SetValue(rast, 7, 1, 0);
		rast := ST_SetValue(rast, 10, 1, 0);
		rast := ST_SetValue(rast, 2, 3, 0);
		rast := ST_SetValue(rast, 5, 3, 0);
		rast := ST_SetValue(rast, 8, 3, 0);
		rast := ST_SetValue(rast, 3, 5, 0);
		rast := ST_SetValue(rast, 6, 5, 0);
		rast := ST_SetValue(rast, 9, 5, 0);
		rast := ST_SetValue(rast, 1, 7, 0);
		rast := ST_SetValue(rast, 4, 7, 0);
		rast := ST_SetValue(rast, 7, 7, 0);
		rast := ST_SetValue(rast, 10, 7, 0);
		rast := ST_SetValue(rast, 2, 9, 0);
		rast := ST_SetValue(rast, 5, 9, 0);
		rast := ST_SetValue(rast, 8, 9, 0);

		INSERT INTO raster_neighborhood VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION IF EXISTS make_test_raster();

SELECT 
	ST_Neighborhood(rast, 1, 1, 1, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(ST_SetBandNoDataValue(rast, NULL), 1, 1, 1, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 2, 2, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 5, 5, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 5, 5, 2, 2)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 11, 11, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 12, 12, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 0, 0, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 0, 2, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, -1, 3, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, -9, 3, 3, 3)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, -9, 3, 3, 3, FALSE)
FROM raster_neighborhood;

SELECT 
	ST_Neighborhood(rast, 1, 4, 4, 1, 1)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 4, 4, 2, 2)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 4, 4, 1, 2)
FROM raster_neighborhood;
SELECT 
	ST_Neighborhood(rast, 1, 4, 4, 1, 0)
FROM raster_neighborhood;

SELECT
	ST_Neighborhood(rast, 1, 'POINT(0 0)'::geometry, 1, 1)
FROM raster_neighborhood;
SELECT
	ST_Neighborhood(rast, 1, 'POINT(3 -3)'::geometry, 2, 2)
FROM raster_neighborhood;

DROP TABLE IF EXISTS raster_neighborhood;
