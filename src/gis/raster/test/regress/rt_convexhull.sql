DROP TABLE IF EXISTS test_raster_convexhull;

CREATE TABLE test_raster_convexhull AS
	SELECT ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(9, 9, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 0, 0), 2, '8BUI', 1, 0) AS rast;

SELECT
	ST_AsText(ST_ConvexHull(rast)),
	ST_AsText(ST_MinConvexHull(rast)),
	ST_AsText(ST_MinConvexHull(rast, 1)),
	ST_AsText(ST_MinConvexHull(rast, 2))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 1, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 1))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 1, 1, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 1))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 1, 0, 0, 0],
			[0, 0, 0, 1, 1, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 1))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 0, 0, 0, 1],
			[0, 0, 0, 1, 1, 0, 0, 0, 0],
			[0, 0, 0, 1, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 1))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 2, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[1, 0, 0, 0, 0, 1, 0, 0, 0],
			[0, 0, 0, 0, 1, 1, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 2)),
	ST_AsText(ST_MinConvexHull(rast))
FROM test_raster_convexhull;

UPDATE test_raster_convexhull SET
	rast = ST_SetValues(
		rast, 2, 1, 1,
		ARRAY[
			[0, 0, 0, 0, 0, 0, 0, 0, 1],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[1, 0, 0, 0, 0, 1, 0, 0, 0],
			[0, 0, 0, 0, 1, 1, 0, 0, 0],
			[0, 0, 0, 0, 0, 1, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 0, 0, 0, 0, 0, 0, 0],
			[0, 0, 1, 0, 0, 0, 0, 0, 0]
		]::double precision[][]
	);

SELECT
	ST_AsText(ST_MinConvexHull(rast, 1)),
	ST_AsText(ST_MinConvexHull(rast, 2)),
	ST_AsText(ST_MinConvexHull(rast))
FROM test_raster_convexhull;

DROP TABLE IF EXISTS test_raster_convexhull;
