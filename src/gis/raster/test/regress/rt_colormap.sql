DROP TABLE IF EXISTS raster_colormap_out;
CREATE TABLE raster_colormap_out (
	testid integer,
	rid integer,
	rast raster
);
DROP TABLE IF EXISTS raster_colormap_in;
CREATE TABLE raster_colormap_in (
	rid integer,
	rast raster
);

INSERT INTO raster_colormap_in
SELECT
	1 AS rid,
	ST_SetValues(
		ST_AddBand(
			ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0, 0, 0),
			1, '8BUI', 0, 0
		),
		1, 1, 1, ARRAY[
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
			[20, 21, 22, 23, 24, 25, 26, 27, 28, 29],
			[30, 31, 32, 33, 34, 35, 36, 37, 38, 39],
			[40, 41, 42, 43, 44, 45, 46, 47, 48, 49],
			[50, 51, 52, 53, 54, 55, 56, 57, 58, 59],
			[60, 61, 62, 63, 64, 65, 66, 67, 68, 69],
			[70, 71, 72, 73, 74, 75, 76, 77, 78, 79],
			[80, 81, 82, 83, 84, 85, 86, 87, 88, 89],
			[90, 91, 92, 93, 94, 95, 96, 97, 98, 99]
		]::double precision[]
	) AS rast
UNION ALL
SELECT
	2 AS rid,
	ST_SetValues(
		ST_AddBand(
			ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0, 0, 0),
			1, '8BUI', 0, 0
		),
		1, 1, 1, ARRAY[
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9],
			[ 0,  1,  2,  3,  4,  5,  6,  7,  8,  9]
		]::double precision[]
	) AS rast
;

INSERT INTO raster_colormap_out
SELECT
	1 AS testid,
	rid,
	_ST_ColorMap(
		rast, 1,
		'
100% 255:255 255 256

  0%   0   0   0 255
  nv   0   0   0   0
'
	) AS rast
FROM raster_colormap_in
UNION ALL
SELECT
	2 AS testid,
	rid,
	_ST_ColorMap(
		rast, 1,
		'
100% 255:255 255 256
 75% 127 255 255 255
 50% 127,  0,127 255
 25%   0 127   0 255
  0%   0   0   0 255
  nv   0   0   0   0
'
	) AS rast
FROM raster_colormap_in
UNION ALL
SELECT
	3 AS testid,
	rid,
	_ST_ColorMap(
		rast, 1,
		'
  0%   1   127   0 255
'
	) AS rast
FROM raster_colormap_in
UNION ALL
SELECT
	4 AS testid,
	rid,
	_ST_ColorMap(
		rast, 1,
		'
9   0   0 255
5   0 255   0
4   0 255   0
0 255   0   0
'
	) AS rast
FROM raster_colormap_in
WHERE rid = 2
UNION ALL
SELECT
	5 AS testid,
	rid,
	ST_ColorMap(
		rast, 1,
		'grayscale'
	) AS rast
FROM raster_colormap_in
WHERE rid = 2
;

SELECT
	testid
	rid,
	(ST_DumpValues(rast)).*
FROM raster_colormap_out
ORDER BY 1, 2;

DROP TABLE IF EXISTS raster_colormap_in;
DROP TABLE IF EXISTS raster_colormap_out;
