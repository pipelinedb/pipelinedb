DROP TABLE IF EXISTS raster_nearestvalue;
CREATE TABLE raster_nearestvalue (
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
		valset double precision[][];
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, 0, 0, 1, -1, 0, 0, 0);
		rast := ST_AddBand(rast, 1, '8BUI', 1, 0);


		valset := array_fill(0., ARRAY[height, width]);
		FOR y IN 1..height LOOP
			FOR x IN 1..width LOOP
				valset[y][x] := 2 * x + (1/3) * y;
			END LOOP;
		END LOOP;
		rast := ST_SetValues(rast, 1, 1, 1, valset);

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

		INSERT INTO raster_nearestvalue VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION IF EXISTS make_test_raster();

SELECT
	ST_NearestValue(rast, 1, 1, 1)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(ST_SetBandNoDataValue(rast, NULL), 1, 1, 1)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 2, 2)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 5, 5)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 5, 5)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 11, 11)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 12, 12)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 0, 0)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, 0, 2)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, -1, 3)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, -9, 3)
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, -9, 3, FALSE)
FROM raster_nearestvalue;

SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(1, 1))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(2, 2))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(5, 5))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(5, 5))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(11, 11))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(12, 12))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(0, 0))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(0, 2))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(-1, 3))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(-9, 3))
FROM raster_nearestvalue;
SELECT
	ST_NearestValue(rast, 1, ST_MakePoint(-9, 3), FALSE)
FROM raster_nearestvalue;

DROP TABLE IF EXISTS raster_nearestvalue;
