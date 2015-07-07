SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_polygon;
CREATE TABLE raster_polygon (
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster()
	RETURNS void
	AS $$
	DECLARE
		width int := 5;
		height int := 5;
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, 0, 0, 1, -1, 0, 0, 0);
		rast := ST_AddBand(rast, 1, '32BUI', 1, 0);

		INSERT INTO raster_polygon VALUES (rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster();
DROP FUNCTION make_test_raster();

CREATE OR REPLACE FUNCTION temp_geos_version()
	RETURNS float
	AS $$ SELECT ((regexp_matches(split_part(postgis_geos_version(), '-', 1), E'^([[:digit:]]+\.[[:digit:]]+)')))[1]::float $$
	LANGUAGE 'sql' IMMUTABLE STRICT;

SELECT
	ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((0 0,0 -5,5 -5,5 0,0 0)))'
FROM raster_polygon;

SELECT
	ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 0,1 -1,0 -1,0 -5,4 -5,5 -5,5 0,1 0)))'
FROM (
	SELECT
		ST_SetValue(
			rast, 1, 1, 1, 0
		) AS rast
	FROM raster_polygon
) foo;

SELECT
	ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 0,1 -1,0 -1,0 -5,4 -5,5 -5,5 0,1 0),(1 -1,1 -2,2 -2,2 -1,1 -1)))'
FROM (
	SELECT
		ST_SetValue(
			ST_SetValue(
				rast, 1, 1, 1, 0
			),
			1, 2, 2, 0
		) AS rast
	FROM raster_polygon
) foo;

SELECT
	CASE
		WHEN temp_geos_version() >= 3.3
			THEN ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 -1,1 0,5 0,5 -5,4 -5,0 -5,0 -1,1 -1),(1 -1,1 -2,2 -2,2 -1,1 -1),(2 -2,2 -3,3 -3,3 -2,2 -2)))'
		ELSE ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 0,1 -1,0 -1,0 -5,4 -5,5 -5,5 0,1 0),(1 -1,1 -2,2 -2,2 -3,3 -3,3 -2,2 -2,2 -1,1 -1)))'
	END
FROM (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					rast, 1, 1, 1, 0
				),
				1, 2, 2, 0
			),
			1, 3, 3, 0
		) AS rast
	FROM raster_polygon
) foo;

SELECT
	CASE
		WHEN temp_geos_version() >= 3.3
			THEN ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 -1,1 0,5 0,5 -5,4 -5,0 -5,0 -1,1 -1),(1 -1,1 -2,2 -2,2 -1,1 -1),(2 -2,2 -3,3 -3,3 -2,2 -2),(3 -3,3 -4,4 -4,4 -3,3 -3)))'
		ELSE ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 0,1 -1,0 -1,0 -5,4 -5,5 -5,5 0,1 0),(1 -1,1 -2,2 -2,2 -3,3 -3,3 -4,4 -4,4 -3,3 -3,3 -2,2 -2,2 -1,1 -1)))'
	END
FROM (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_SetValue(
						rast, 1, 1, 1, 0
					),
					1, 2, 2, 0
				),
				1, 3, 3, 0
			),
			1, 4, 4, 0
		) AS rast
	FROM raster_polygon
) foo;

SELECT
	ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((4 -4,4 -5,0 -5,0 -1,1 -1,1 -2,2 -2,2 -3,3 -3,3 -4,4 -4)),((1 -1,1 0,5 0,5 -4,4 -4,4 -3,3 -3,3 -2,2 -2,2 -1,1 -1)))'
FROM (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_SetValue(
						ST_SetValue(
							rast, 1, 1, 1, 0
						),
						1, 2, 2, 0
					),
					1, 3, 3, 0
				),
				1, 4, 4, 0
			),
			1, 5, 5, 0
		) AS rast
	FROM raster_polygon
) foo;

SELECT
	ST_AsText(ST_Polygon(rast)) = 'MULTIPOLYGON(((1 -4,2 -4,2 -3,3 -3,3 -4,4 -4,4 -5,3 -5,1 -5,1 -4)),((1 -4,0 -4,0 -1,1 -1,1 -2,2 -2,2 -3,1 -3,1 -4)),((3 -2,4 -2,4 -1,5 -1,5 -4,4 -4,4 -3,3 -3,3 -2)),((3 -2,2 -2,2 -1,1 -1,1 0,4 0,4 -1,3 -1,3 -2)))'
FROM (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_SetValue(
						ST_SetValue(
							ST_SetValue(
								ST_SetValue(
									ST_SetValue(
										ST_SetValue(
											rast, 1, 1, 1, 0
										),
										1, 2, 2, 0
									),
									1, 3, 3, 0
								),
								1, 4, 4, 0
							),
							1, 5, 5, 0
						),
						1, 5, 1, 0
					),
					1, 4, 2, 0
				),
				1, 2, 4, 0
			),
			1, 1, 5, 0
		) AS rast
	FROM raster_polygon
) foo;

DROP FUNCTION temp_geos_version();
DROP TABLE IF EXISTS raster_polygon;
