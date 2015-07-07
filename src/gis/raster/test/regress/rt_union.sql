DROP TABLE IF EXISTS raster_union_in;
CREATE TABLE raster_union_in (
	rid integer,
	rast raster
);
DROP TABLE IF EXISTS raster_union_out;
CREATE TABLE raster_union_out (
	uniontype text,
	rast raster
);

INSERT INTO raster_union_in
	SELECT 0, NULL::raster AS rast UNION ALL
	SELECT 1, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 2, ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast, 1) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'FIRST',
		ST_Union(rast, 1, 'FIRST') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MIN',
		ST_Union(rast, 1, 'MIN') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MAX',
		ST_Union(rast, 1, 'MAX') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'COUNT',
		ST_Union(rast, 1, 'COUNT') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'SUM',
		ST_Union(rast, 1, 'SUM') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MEAN',
		ST_Union(rast, 'mean') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'RANGE',
		ST_Union(rast, 'range') AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 10, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 11, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 12, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL

	SELECT 13, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, -2, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 14, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, -2, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 15, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, -2, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL

	SELECT 16, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, -4, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 17, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, -4, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 18, ST_AddBand(ST_MakeEmptyRaster(2, 2, 4, -4, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast, 1) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'FIRST',
		ST_Union(rast, 1, 'FIRST') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MIN',
		ST_Union(rast, 1, 'MIN') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MAX',
		ST_Union(rast, 1, 'MAX') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'COUNT',
		ST_Union(rast, 1, 'COUNT') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'SUM',
		ST_Union(rast, 1, 'SUM') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MEAN',
		ST_Union(rast, 1, 'mean') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'RANGE',
		ST_Union(rast, 1, 'RANGE') AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 20, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0), 2, '32BF', 1, -9999) AS rast UNION ALL
	SELECT 21, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 2, -9999) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast, 2) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'FIRST',
		ST_Union(rast, 2, 'FIRST') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MIN',
		ST_Union(rast, 2, 'MIN') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MAX',
		ST_Union(rast, 2, 'MAX') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'COUNT',
		ST_Union(rast, 2, 'COUNT') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'SUM',
		ST_Union(rast, 2, 'SUM') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MEAN',
		ST_Union(rast, 2, 'mean') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'RANGE',
		ST_Union(rast, 2, 'RANGE') AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 30, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, NULL) AS rast UNION ALL
	SELECT 31, ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, NULL) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast, 1) AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 40, NULL::raster AS rast UNION ALL
	SELECT 41, NULL::raster AS rast UNION ALL
	SELECT 42, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0), 2, '32BF', 100, -9999) AS rast UNION ALL
	SELECT 43, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 200, -9999) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast, ARRAY[ROW(1, 'LAST'), ROW(2, 'LAST')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'FIRST',
		ST_Union(rast, ARRAY[ROW(1, 'FIRST'), ROW(2, 'FIRST')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MIN',
		ST_Union(rast, ARRAY[ROW(1, 'MIN'), ROW(2, 'MIN')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MAX',
		ST_Union(rast, ARRAY[ROW(1, 'MAX'), ROW(2, 'MAX')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'COUNT',
		ST_Union(rast, ARRAY[ROW(1, 'COUNT'), ROW(2, 'COUNT')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'SUM',
		ST_Union(rast, ARRAY[ROW(1, 'SUM'), ROW(2, 'SUM')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MEAN',
		ST_Union(rast, ARRAY[ROW(1, 'MEAN'), ROW(2, 'MEAN')]::unionarg[]) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'RANGE',
		ST_Union(rast, ARRAY[ROW(1, 'RANGE'), ROW(2, 'RANGE')]::unionarg[]) AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast, 2)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 50, NULL::raster AS rast UNION ALL
	SELECT 51, NULL::raster AS rast UNION ALL
	SELECT 52, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0), 2, '32BF', 100, -9999) AS rast UNION ALL
	SELECT 53, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, -1, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0), 2, '32BF', 200, -9999) AS rast UNION ALL
	SELECT 54, NULL::raster AS rast UNION ALL
	SELECT 55, ST_AddBand(ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, -1, 1, 1, -1, 0, 0, 0), 1, '8BUI', 3, 0), 2, '32BF', 300, -9999), 3, '8BSI', -1, -10) AS rast UNION ALL
	SELECT 56, ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2, 2, 1, 1, 1, -1, 0, 0, 0), 1, '8BUI', 4, 0), 2, '32BF', 400, -9999) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST-1',
		ST_Union(rast) AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'LAST-2',
		ST_Union(rast, 'last') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'FIRST-2',
		ST_Union(rast, 'first') AS rast
	FROM raster_union_in;

INSERT INTO raster_union_out
	SELECT
		'MEAN-2',
		ST_Union(rast, 'mean') AS rast
	FROM raster_union_in;

SELECT
	uniontype,
	(ST_Metadata(rast)).*
FROM raster_union_out
ORDER BY uniontype;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast, 2)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast, 3)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

TRUNCATE raster_union_out;
TRUNCATE raster_union_in;

INSERT INTO raster_union_in
	SELECT 60, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, -2, 1, -1, 0, 0, 0), 1, '8BUI', 1, 0) AS rast UNION ALL
	SELECT 61, ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, -1, 0, 0, 0), 1, '8BUI', 2, 0) AS rast UNION ALL
	SELECT 62, ST_AddBand(ST_MakeEmptyRaster(2, 2, 2, 0, 1, -1, 0, 0, 0), 1, '8BUI', 3, 0) AS rast UNION ALL
	SELECT 63, ST_AddBand(ST_MakeEmptyRaster(2, 2, -1, -3, 1, -1, 0, 0, 0), 1, '8BUI', 4, 0) AS rast UNION ALL
	SELECT  0, NULL::raster UNION ALL -- toxic element embedded
	SELECT 64, ST_AddBand(ST_MakeEmptyRaster(2, 2, -2, 4, 1, -1, 0, 0, 0), 1, '8BUI', 5, 0) AS rast
;

INSERT INTO raster_union_out
	SELECT
		'LAST',
		ST_Union(rast) AS rast
	FROM raster_union_in;

SELECT
	(ST_Metadata(rast)).*
FROM raster_union_out;

SELECT
	uniontype,
	x,
	y,
	val
FROM (
	SELECT
		uniontype,
		(ST_PixelAsPoints(rast)).*
	FROM raster_union_out
) foo
ORDER BY uniontype, y, x;

DROP TABLE IF EXISTS raster_union_in;
DROP TABLE IF EXISTS raster_union_out;

-- Some toxic input
SELECT 'none', ST_Union(r) from ( select null::raster r where false ) f;
SELECT 'null', ST_Union(null::raster);
