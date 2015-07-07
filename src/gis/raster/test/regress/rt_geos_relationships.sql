-- These tests are for raster functions that use GEOS spatial relationship functions

SET client_min_messages TO warning;

DROP TABLE IF EXISTS raster_geos_rast;
CREATE TABLE raster_geos_rast (
	rid integer,
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster(
	rid integer,
	width integer DEFAULT 2, height integer DEFAULT 2,
	ul_x double precision DEFAULT 0, ul_y double precision DEFAULT 0,
	skew_x double precision DEFAULT 0, skew_y double precision DEFAULT 0
)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', 1, 0);


		INSERT INTO raster_geos_rast VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
SELECT make_test_raster(0, 2, 2, -1, -1);
SELECT make_test_raster(1, 2, 2);
SELECT make_test_raster(2, 3, 3);
SELECT make_test_raster(3, 2, 2, -1, -10);
DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision);

INSERT INTO raster_geos_rast VALUES (10, (
	SELECT
		ST_SetValue(rast, 1, 1, 1, 0)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (11, (
	SELECT
		ST_SetValue(rast, 1, 2, 1, 0)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (12, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(rast, 1, 1, 1, 0),
				1, 2, 1, 0
			),
			1, 1, 2, 0
		)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (13, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(
					ST_SetValue(rast, 1, 1, 1, 0),
					1, 2, 1, 0
				),
				1, 1, 2, 0
			),
			1, 2, 2, 0
		)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (14, (
	SELECT
		ST_SetUpperLeft(rast, 2, 0)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (15, (
	SELECT
		ST_SetScale(
			ST_SetUpperLeft(rast, 0.1, 0.1),
			0.4, 0.4
		)
	FROM raster_geos_rast
	WHERE rid = 1
));
INSERT INTO raster_geos_rast VALUES (16, (
	SELECT
		ST_SetScale(
			ST_SetUpperLeft(rast, -0.1, 0.1),
			0.4, 0.4
		)
	FROM raster_geos_rast
	WHERE rid = 1
));

INSERT INTO raster_geos_rast VALUES (20, (
	SELECT
		ST_SetUpperLeft(rast, -2, -2)
	FROM raster_geos_rast
	WHERE rid = 2
));
INSERT INTO raster_geos_rast VALUES (21, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				ST_SetValue(rast, 1, 1, 1, 0),
				1, 2, 2, 0
			),
			1, 3, 3, 0
		)
	FROM raster_geos_rast
	WHERE rid = 20
));
INSERT INTO raster_geos_rast VALUES (22, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				rast, 1, 3, 2, 0
			),
			1, 2, 3, 0
		)
	FROM raster_geos_rast
	WHERE rid = 21
));
INSERT INTO raster_geos_rast VALUES (23, (
	SELECT
		ST_SetValue(
			ST_SetValue(
				rast, 1, 3, 1, 0
			),
			1, 1, 3, 0
		)
	FROM raster_geos_rast
	WHERE rid = 22
));

INSERT INTO raster_geos_rast VALUES (30, (
	SELECT
		ST_SetSkew(rast, -0.5, 0.5)
	FROM raster_geos_rast
	WHERE rid = 2
));
INSERT INTO raster_geos_rast VALUES (31, (
	SELECT
		ST_SetSkew(rast, -1, 1)
	FROM raster_geos_rast
	WHERE rid = 2
));
INSERT INTO raster_geos_rast VALUES (32, (
	SELECT
		ST_SetSkew(rast, 1, -1)
	FROM raster_geos_rast
	WHERE rid = 2
));

SELECT
	'1.1',
	r1.rid,
	r2.rid,
	ST_Contains(r1.rast, NULL, r2.rast, NULL),
	ST_ContainsProperly(r1.rast, NULL, r2.rast, NULL),
	ST_CoveredBy(r1.rast, NULL, r2.rast, NULL),
	ST_Covers(r1.rast, NULL, r2.rast, NULL),
	ST_Disjoint(r1.rast, NULL, r2.rast, NULL),
	ST_Overlaps(r1.rast, NULL, r2.rast, NULL),
	ST_Touches(r1.rast, NULL, r2.rast, NULL),
	ST_Within(r1.rast, NULL, r2.rast, NULL),
	ST_DWithin(r1.rast, NULL, r2.rast, NULL, 0),
	ST_DWithin(r1.rast, 1, r2.rast, 1, 0),
	ST_DWithin(r1.rast, 1, r2.rast, 1, 1),
	ST_DWithin(r1.rast, 1, r2.rast, 1, 2),
	ST_DWithin(r1.rast, 1, r2.rast, 1, 7),
	ST_DFullyWithin(r1.rast, NULL, r2.rast, NULL, 0),
	ST_DFullyWithin(r1.rast, 1, r2.rast, 1, 0),
	ST_DFullyWithin(r1.rast, 1, r2.rast, 1, 1),
	ST_DFullyWithin(r1.rast, 1, r2.rast, 1, 2),
	ST_DFullyWithin(r1.rast, 1, r2.rast, 1, 7)
FROM raster_geos_rast r1
CROSS JOIN raster_geos_rast r2
WHERE r1.rid = 0;

SELECT
	'1.2',
	r1.rid,
	r2.rid,
	ST_Contains(r1.rast, 1, r2.rast, 1),
	ST_ContainsProperly(r1.rast, 1, r2.rast, 1),
	ST_CoveredBy(r1.rast, 1, r2.rast, 1),
	ST_Covers(r1.rast, 1, r2.rast, 1),
	ST_Disjoint(r1.rast, 1, r2.rast, 1),
	ST_Overlaps(r1.rast, 1, r2.rast, 1),
	ST_Touches(r1.rast, 1, r2.rast, 1),
	ST_Within(r1.rast, 1, r2.rast, 1)
FROM raster_geos_rast r1
JOIN raster_geos_rast r2
	ON r1.rid != r2.rid
WHERE r1.rid = 0;

SELECT
	'1.3',
	r1.rid,
	r2.rid,
	ST_Contains(r1.rast, NULL, r2.rast, NULL),
	ST_ContainsProperly(r1.rast, NULL, r2.rast, NULL),
	ST_CoveredBy(r1.rast, NULL, r2.rast, NULL),
	ST_Covers(r1.rast, NULL, r2.rast, NULL),
	ST_Within(r1.rast, NULL, r2.rast, NULL)
FROM raster_geos_rast r1
JOIN raster_geos_rast r2
	ON r1.rid != r2.rid
WHERE r2.rid = 0;

SELECT
	'1.4',
	r1.rid,
	r2.rid,
	ST_Contains(r1.rast, 1, r2.rast, 1),
	ST_ContainsProperly(r1.rast, 1, r2.rast, 1),
	ST_CoveredBy(r1.rast, 1, r2.rast, 1),
	ST_Covers(r1.rast, 1, r2.rast, 1),
	ST_Within(r1.rast, 1, r2.rast, 1)
FROM raster_geos_rast r1
JOIN raster_geos_rast r2
	ON r1.rid != r2.rid
WHERE r2.rid = 0;


SELECT
	'2.1',
	r1.rid,
	r2.rid,
	NULL AS distance,
	ST_DWithin(r1.rast, NULL, r2.rast, NULL, NULL)
FROM raster_geos_rast r1
CROSS JOIN raster_geos_rast r2
WHERE r1.rid = 0;

SELECT
	'2.2',
	r1.rid,
	r2.rid,
	-1 AS distance,
	ST_DWithin(r1.rast, NULL, r2.rast, NULL, -1)
FROM raster_geos_rast r1
CROSS JOIN raster_geos_rast r2
WHERE r1.rid = 0;

SELECT
	'2.3',
	r1.rid,
	r2.rid,
	NULL AS distance,
	ST_DFullyWithin(r1.rast, NULL, r2.rast, NULL, NULL)
FROM raster_geos_rast r1
CROSS JOIN raster_geos_rast r2
WHERE r1.rid = 0;

SELECT
	'2.4',
	r1.rid,
	r2.rid,
	-1 AS distance,
	ST_DFullyWithin(r1.rast, NULL, r2.rast, NULL, -1)
FROM raster_geos_rast r1
CROSS JOIN raster_geos_rast r2
WHERE r1.rid = 0;

DROP TABLE IF EXISTS raster_geos_rast;
