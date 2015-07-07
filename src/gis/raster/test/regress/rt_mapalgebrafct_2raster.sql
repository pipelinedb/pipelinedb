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

DROP FUNCTION IF EXISTS make_test_raster(integer, integer, integer, double precision, double precision, double precision, double precision, double precision, double precision);

CREATE OR REPLACE FUNCTION raster_mapalgebra_intersection(
	rast1 double precision,
	rast2 double precision,
	xy int[],
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	DECLARE
	BEGIN
		IF rast1 IS NOT NULL AND rast2 IS NOT NULL THEN
			RETURN rast1;
		ELSE
			RETURN NULL;
		END IF;

		RETURN NULL;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_mapalgebra_union(
	rast1 double precision,
	rast2 double precision,
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	DECLARE
	BEGIN
		CASE
			WHEN rast1 IS NOT NULL AND rast2 IS NOT NULL THEN
				RETURN ((rast1 + rast2)/2.);
			WHEN rast1 IS NULL AND rast2 IS NULL THEN
				RETURN NULL;
			WHEN rast1 IS NULL THEN
				RETURN rast2;
			ELSE
				RETURN rast1;
		END CASE;

		RETURN NULL;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_mapalgebra_first(
	rast1 double precision,
	rast2 double precision,
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	DECLARE
	BEGIN
		CASE
			WHEN rast1 IS NOT NULL AND rast2 IS NOT NULL THEN
				RETURN NULL;
			WHEN rast1 IS NOT NULL THEN
				RETURN rast1;
			ELSE
				RETURN NULL;
		END CASE;

		RETURN NULL;
	END;
	$$ LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_mapalgebra_second(
	rast1 double precision,
	rast2 double precision,
	VARIADIC userargs text[]
)
	RETURNS double precision
	AS $$
	DECLARE
	BEGIN
		CASE
			WHEN rast1 IS NOT NULL AND rast2 IS NOT NULL THEN
				RETURN NULL;
			WHEN rast2 IS NOT NULL THEN
				RETURN rast2;
			ELSE
				RETURN NULL;
		END CASE;

		RETURN NULL;
	END;
	$$ LANGUAGE 'plpgsql';

-- INTERSECTION
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_intersection(double precision, double precision, int[], text[])'::regprocedure, '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_intersection(double precision, double precision, int[], text[])'::regprocedure, '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'INTERSECTION', st_mapalgebrafct(
		NULL::raster, rast, 'raster_mapalgebra_intersection(double precision, double precision, int[], text[])'::regprocedure, '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'INTERSECTION', st_mapalgebrafct(
		rast, NULL::raster, 'raster_mapalgebra_intersection(double precision, double precision, int[], text[])'::regprocedure, '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'INTERSECTION', st_mapalgebrafct(
		NULL::raster, NULL::raster, 'raster_mapalgebra_intersection(double precision, double precision, int[], text[])'::regprocedure, '32BF', 'INTERSECTION'
	)
;

-- UNION
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'UNION', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_union(double precision, double precision, text[])'::regprocedure, '32BF', 'UNION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'UNION', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_union(double precision, double precision, text[])'::regprocedure, '32BF', 'UNION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'UNION', st_mapalgebrafct(
		NULL::raster, rast, 'raster_mapalgebra_union(double precision, double precision, text[])'::regprocedure, '32BF', 'UNION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'UNION', st_mapalgebrafct(
		rast, NULL::raster, 'raster_mapalgebra_union(double precision, double precision, text[])'::regprocedure, '32BF', 'UNION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'UNION', st_mapalgebrafct(
		NULL::raster, NULL::raster, 'raster_mapalgebra_union(double precision, double precision, text[])'::regprocedure, '32BF', 'UNION'
	)
;

-- FIRST
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_first(double precision, double precision, text[])'::regprocedure, '32BF', 'FIRST'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_first(double precision, double precision, text[])'::regprocedure, '32BF', 'FIRST'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'FIRST', st_mapalgebrafct(
		NULL::raster, rast, 'raster_mapalgebra_first(double precision, double precision, text[])'::regprocedure, '32BF', 'FIRST'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'FIRST', st_mapalgebrafct(
		rast, NULL::raster, 'raster_mapalgebra_first(double precision, double precision, text[])'::regprocedure, '32BF', 'FIRST'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'FIRST', st_mapalgebrafct(
		NULL::raster, NULL::raster, 'raster_mapalgebra_first(double precision, double precision, text[])'::regprocedure, '32BF', 'FIRST'
	)
;

-- SECOND
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_second(double precision, double precision, text[])'::regprocedure, '32BF', 'SECOND'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebrafct(
		r1.rast, r2.rast, 'raster_mapalgebra_second(double precision, double precision, text[])'::regprocedure, '32BF', 'SECOND'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'SECOND', st_mapalgebrafct(
		NULL::raster, rast, 'raster_mapalgebra_second(double precision, double precision, text[])'::regprocedure, '32BF', 'SECOND'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'SECOND', st_mapalgebrafct(
		rast, NULL::raster, 'raster_mapalgebra_second(double precision, double precision, text[])'::regprocedure, '32BF', 'SECOND'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'SECOND', st_mapalgebrafct(
		NULL::raster, NULL::raster, 'raster_mapalgebra_second(double precision, double precision, text[])'::regprocedure, '32BF', 'SECOND'
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

DROP FUNCTION IF EXISTS raster_mapalgebra_intersection(double precision, double precision, int[], VARIADIC text[]);
DROP FUNCTION IF EXISTS raster_mapalgebra_union(double precision, double precision, VARIADIC text[]);
DROP FUNCTION IF EXISTS raster_mapalgebra_first(double precision, double precision, VARIADIC text[]);
DROP FUNCTION IF EXISTS raster_mapalgebra_second(double precision, double precision, VARIADIC text[]);

DROP TABLE IF EXISTS raster_mapalgebra;
DROP TABLE IF EXISTS raster_mapalgebra_out;
