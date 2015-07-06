DROP TABLE IF EXISTS raster_pixelascentroids;
CREATE TABLE raster_pixelascentroids (
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

		INSERT INTO raster_pixelascentroids VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION make_test_raster();

SELECT
	(pix).x,
	(pix).y,
	ST_RasterToWorldCoordX(rast, (pix).x, (pix).y),
	ST_RasterToWorldCoordY(rast, (pix).x, (pix).y),
	(pix).val,
	ST_AsText((pix).geom)
FROM (SELECT rast, ST_PixelAsCentroids(rast) AS pix FROM raster_pixelascentroids) foo
ORDER BY 1, 2, 3, 4, 6;

SELECT
	(pix).x,
	(pix).y,
	ST_RasterToWorldCoordX(rast, (pix).x, (pix).y),
	ST_RasterToWorldCoordY(rast, (pix).x, (pix).y),
	(pix).val,
	ST_AsText((pix).geom)
FROM (SELECT rast, ST_PixelAsCentroids(rast, NULL) AS pix FROM raster_pixelascentroids) foo
ORDER BY 1, 2, 3, 4, 6;

SELECT
	(pix).x,
	(pix).y,
	ST_RasterToWorldCoordX(rast, (pix).x, (pix).y),
	ST_RasterToWorldCoordY(rast, (pix).x, (pix).y),
	(pix).val,
	ST_AsText((pix).geom)
FROM (SELECT rast, ST_PixelAsCentroids(rast, 1, FALSE) AS pix FROM raster_pixelascentroids) foo
ORDER BY 1, 2, 3, 4, 6;

SELECT ST_AsText(ST_PixelAsCentroid(rast, 1, 1)) FROM raster_pixelascentroids;
SELECT ST_AsText(ST_PixelAsCentroid(rast, 1, 2)) FROM raster_pixelascentroids;
SELECT ST_AsText(ST_PixelAsCentroid(rast, -1, -1)) FROM raster_pixelascentroids;

DROP TABLE IF EXISTS raster_pixelascentroids;
