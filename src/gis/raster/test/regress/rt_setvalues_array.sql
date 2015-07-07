SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_setvalues_rast;
DROP TABLE IF EXISTS raster_setvalues_out;
CREATE TABLE raster_setvalues_rast (
	rid integer,
	rast raster
);
CREATE TABLE raster_setvalues_out (
	rid integer,
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster(rid integer, width integer DEFAULT 2, height integer DEFAULT 2, ul_x double precision DEFAULT 0, ul_y double precision DEFAULT 0, skew_x double precision DEFAULT 0, skew_y double precision DEFAULT 0)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', 1, 0);

		INSERT INTO raster_setvalues_rast VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster(0, 5, 3);
DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision);

INSERT INTO raster_setvalues_out VALUES (
	1, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	2, (
	SELECT ST_SetValues(
		rast, 1, 2, 1,
		ARRAY[10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	3, (
	SELECT ST_SetValues(
		rast, 1, 3, 1,
		ARRAY[10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	4, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[10, 10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	5, (
	SELECT ST_SetValues(
		rast, 1, 2, 2,
		ARRAY[10, 10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	6, (
	SELECT ST_SetValues(
		rast, 1, 3, 3,
		ARRAY[10, 10, 10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	7, (
	SELECT ST_SetValues(
		rast, 1, 4, 3,
		ARRAY[10, 10, 10]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	8, (
	SELECT ST_SetValues(
		rast, 1, 2, 1,
		ARRAY[[5, 5, 5, 5], [6, 6, 6, 6]]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	9, (
	SELECT ST_SetValues(
		rast, 1, 2, 1,
		ARRAY[[5, 5, 5, 5], [6, 6, 6, 6], [7, 7, 7, NULL]]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	10, (
	SELECT ST_SetValues(
		rast, 1, 2, 1,
		ARRAY[[5, 5, 5, 5, 10], [6, 6, 6, 6, 10], [7, 7, 7, NULL, 10]]::double precision[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	11, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[10, 10, 10]::double precision[],
		ARRAY[false, true]::boolean[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	12, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[NULL, 10, 0]::double precision[],
		ARRAY[false, NULL, false]::boolean[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	13, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[NULL, 10, 0]::double precision[],
		ARRAY[false, NULL, true]::boolean[]
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	21, (
	SELECT ST_SetValues(
		rast, 1,
		1, 1,
		5, 3, 
		100
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	22, (
	SELECT ST_SetValues(
		rast, 1,
		1, 1,
		5, 3, 
		NULL
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	23, (
	SELECT ST_SetValues(
		rast, 1,
		1, 1,
		5, 3, 
		0
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	31, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[-1, 31, -1]::double precision[],
		-1::double precision
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	32, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[[-1, 32, -1], [32, -1, 32]]::double precision[],
		-1::double precision
	)
	FROM raster_setvalues_rast
));

INSERT INTO raster_setvalues_out VALUES (
	33, (
	SELECT ST_SetValues(
		rast, 1, 1, 1,
		ARRAY[[NULL, 33, NULL], [33, NULL, 33]]::double precision[],
		NULL::double precision
	)
	FROM raster_setvalues_rast
));

SELECT
	rid,
	(poly).x,
	(poly).y,
	(poly).val
FROM (
	SELECT
		rid,
		ST_PixelAsPolygons(rast) AS poly
	FROM raster_setvalues_out
) AS foo
ORDER BY 1, 2, 3;

DROP TABLE IF EXISTS raster_setvalues_rast;
DROP TABLE IF EXISTS raster_setvalues_out;
