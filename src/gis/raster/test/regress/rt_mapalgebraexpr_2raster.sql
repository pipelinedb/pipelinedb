DROP TABLE IF EXISTS raster_mapalgebra;
CREATE TABLE raster_mapalgebra (
	rid integer,
	rast raster
);
DROP TABLE IF EXISTS raster_mapalgebra_out;
CREATE TABLE raster_mapalgebra_out (
	rid1 integer,
	rid2 integer,
	extent varchar,
	rast raster
);
CREATE OR REPLACE FUNCTION make_test_raster(
	rid integer,
	width integer DEFAULT 2,
	height integer DEFAULT 2,
	ul_x double precision DEFAULT 0,
	ul_y double precision DEFAULT 0,
	skew_x double precision DEFAULT 0,
	skew_y double precision DEFAULT 0,
	initvalue double precision DEFAULT 1,
	nodataval double precision DEFAULT 0
)
	RETURNS void
	AS $$
	DECLARE
		x int;
		y int;
		rast raster;
	BEGIN
		rast := ST_MakeEmptyRaster(width, height, ul_x, ul_y, 1, 1, skew_x, skew_y, 0);
		rast := ST_AddBand(rast, 1, '8BUI', initvalue, nodataval);


		INSERT INTO raster_mapalgebra VALUES (rid, rast);

		RETURN;
	END;
	$$ LANGUAGE 'plpgsql';
-- no skew
SELECT make_test_raster(0, 4, 4, -2, -2);
SELECT make_test_raster(1, 2, 2, 0, 0, 0, 0, 2);
SELECT make_test_raster(2, 2, 2, 1, -1, 0, 0, 3);
SELECT make_test_raster(3, 2, 2, 1, 1, 0, 0, 4);
SELECT make_test_raster(4, 2, 2, 2, 2, 0, 0, 5);

-- skew
SELECT make_test_raster(10, 4, 4, -2, -2, 1, -1);
SELECT make_test_raster(11, 2, 2, 0, 0, 1, -1, 2);
SELECT make_test_raster(12, 2, 2, 1, -1, 1, -1, 3);
SELECT make_test_raster(13, 2, 2, 1, 1, 1, -1, 4);
SELECT make_test_raster(14, 2, 2, 2, 2, 1, -1, 5);

DROP FUNCTION make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);

-- INTERSECTION
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebraexpr(
		r1.rast, r2.rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebraexpr(
		r1.rast, r2.rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'INTERSECTION', st_mapalgebraexpr(
		NULL::raster, rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'INTERSECTION', st_mapalgebraexpr(
		rast, NULL::raster, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'INTERSECTION', st_mapalgebraexpr(
		NULL::raster, NULL::raster, '[rast1.val]', '32BF', 'INTERSECTION'
	)
;

-- UNION
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'UNION', st_mapalgebraexpr(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'UNION', st_mapalgebraexpr(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'UNION', st_mapalgebraexpr(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '100', '200', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'UNION', st_mapalgebraexpr(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '100', '200', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'UNION', st_mapalgebraexpr(
		NULL::raster, rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'UNION', st_mapalgebraexpr(
		rast, NULL::raster, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'UNION', st_mapalgebraexpr(
		NULL::raster, NULL::raster, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
;

-- FIRST
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebraexpr(
		r1.rast, r2.rast, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebraexpr(
		r1.rast, r2.rast, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'FIRST', st_mapalgebraexpr(
		NULL::raster, rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'FIRST', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'FIRST', st_mapalgebraexpr(
		rast, NULL::raster, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'FIRST', st_mapalgebraexpr(
		NULL::raster, NULL::raster, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
;

-- SECOND
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebraexpr(
		r1.rast, r2.rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebraexpr(
		r1.rast, r2.rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'SECOND', st_mapalgebraexpr(
		NULL::raster, rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'SECOND', st_mapalgebraexpr(
		rast, NULL::raster, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'SECOND', st_mapalgebraexpr(
		NULL::raster, NULL::raster, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
;

-- output
SELECT
	rid1,
	rid2,
	extent,
	round(upperleftx::numeric, 3) AS upperleftx,
	round(upperlefty::numeric, 3) AS upperlefty,
	width,
	height,
	round(scalex::numeric, 3) AS scalex,
	round(scaley::numeric, 3) AS scaley,
	round(skewx::numeric, 3) AS skewx,
	round(skewy::numeric, 3) AS skewy,
	srid,
	numbands,
	pixeltype,
	round(nodatavalue::numeric, 3) AS nodatavalue,
	round(firstvalue::numeric, 3) AS firstvalue,
	round(lastvalue::numeric, 3) AS lastvalue
FROM (
	SELECT
		rid1,
		rid2,
		extent,
		(ST_Metadata(rast)).*,
		(ST_BandMetadata(rast, 1)).*,
		ST_Value(rast, 1, 1, 1) AS firstvalue,
		ST_Value(rast, 1, ST_Width(rast), ST_Height(rast)) AS lastvalue
	FROM raster_mapalgebra_out
) AS r;

DROP TABLE IF EXISTS raster_mapalgebra;
DROP TABLE IF EXISTS raster_mapalgebra_out;
