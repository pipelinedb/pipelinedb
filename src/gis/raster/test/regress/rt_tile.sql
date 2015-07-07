DROP TABLE IF EXISTS raster_tile;
CREATE TABLE raster_tile AS
	WITH foo AS (
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0), 2, '8BUI', 10, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 3, 0, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '8BUI', 20, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 6, 0, 1, -1, 0, 0, 0), 1, '8BUI', 3, 0), 2, '8BUI', 30, 0) AS rast UNION ALL

		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 0, -3, 1, -1, 0, 0, 0), 1, '8BUI', 4, 0), 2, '8BUI', 40, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 3, -3, 1, -1, 0, 0, 0), 1, '8BUI', 5, 0), 2, '8BUI', 50, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 6, -3, 1, -1, 0, 0, 0), 1, '8BUI', 6, 0), 2, '8BUI', 60, 0) AS rast UNION ALL

		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 0, -6, 1, -1, 0, 0, 0), 1, '8BUI', 7, 0), 2, '8BUI', 70, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 3, -6, 1, -1, 0, 0, 0), 1, '8BUI', 8, 0), 2, '8BUI', 80, 0) AS rast UNION ALL
		SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(3, 3, 6, -6, 1, -1, 0, 0, 0), 1, '8BUI', 9, 0), 2, '8BUI', 90, 0) AS rast
	)
	SELECT ST_Union(rast) AS rast FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 3, 3, TRUE) AS rast FROM raster_tile
)
SELECT
	1, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, ARRAY[1], 3, 3, TRUE) AS rast FROM raster_tile
)
SELECT
	2, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, ARRAY[2, 1], 3, 3, TRUE) AS rast FROM raster_tile
)
SELECT
	3, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 3, 3, TRUE) AS rast FROM raster_tile
)
SELECT
	4, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 2, TRUE) AS rast FROM raster_tile
)
SELECT
	5, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 1, 1, TRUE) AS rast FROM raster_tile
)
SELECT
	6, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 5, 5, TRUE) AS rast FROM raster_tile
)
SELECT
	7, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 3, TRUE) AS rast FROM raster_tile
)
SELECT
	8, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 3, 2, TRUE) AS rast FROM raster_tile
)
SELECT
	9, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 3, 3) AS rast FROM raster_tile
)
SELECT
	11, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, ARRAY[1], 3, 3) AS rast FROM raster_tile
)
SELECT
	12, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, ARRAY[2, 1], 3, 3) AS rast FROM raster_tile
)
SELECT
	13, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 3, 3) AS rast FROM raster_tile
)
SELECT
	14, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 2) AS rast FROM raster_tile
)
SELECT
	15, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 1, 1) AS rast FROM raster_tile
)
SELECT
	16, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 5, 5) AS rast FROM raster_tile
)
SELECT
	17, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 2, 3) AS rast FROM raster_tile
)
SELECT
	18, ST_DumpValues(rast)
FROM foo;

WITH foo AS (
	SELECT ST_Tile(rast, 3, 2) AS rast FROM raster_tile
)
SELECT
	19, ST_DumpValues(rast)
FROM foo;

DROP TABLE IF EXISTS raster_tile;
