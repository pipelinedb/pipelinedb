DROP TABLE IF EXISTS raster_pixelofvalue;
CREATE TABLE raster_pixelofvalue (
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
		rast := ST_MakeEmptyRaster(width, height, 0, 0, 1, -1, 0, 0, 0);
		rast := ST_AddBand(rast, 1, '32BUI', 0, 0);

		FOR x IN 1..width LOOP
			FOR y IN 1..height LOOP
				IF (x + y) % 2 = 1 THEN
					rast := ST_SetValue(rast, 1, x, y, x + y);
				END IF;
			END LOOP;
		END LOOP;

		INSERT INTO raster_pixelofvalue VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION make_test_raster();

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast, 1,
			ARRAY[3, 11]
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast, 1,
			ARRAY[5]
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast,
			ARRAY[0]
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast,
			ARRAY[0],
			FALSE
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast, 1,
			7
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast,
			2	
		) AS pixval
	FROM raster_pixelofvalue
) foo;

SELECT
	(pixval).*
FROM (
	SELECT
		ST_PixelOfValue(
			rast,
			0,
			FALSE
		) AS pixval
	FROM raster_pixelofvalue
) foo;

DROP TABLE IF EXISTS raster_pixelofvalue;
