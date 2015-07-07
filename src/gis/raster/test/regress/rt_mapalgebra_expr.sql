SET client_min_messages TO warning;

CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(10, 10, ulx, uly, 1, 1, 0, 0, 0), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION raster_plus_twenty(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS 
    $$
    BEGIN
        IF pixel IS NULL THEN
            RAISE NOTICE 'Pixel value is null.';
        END IF;
        RETURN pixel + 20;
    END;
    $$ 
    LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_plus_arg1(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS 
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + x;
    END;
    $$ 
    LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION raster_polynomial(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        m float := 1;
        b float := 0;
    BEGIN
        IF NOT args[1] is NULL THEN
            m := args[1]::float;
        END IF;
        IF NOT args[2] is NULL THEN
            b := args[2]::float;
        END IF;
        RETURN m * pixel + b;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_nullage(pixel FLOAT, VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    BEGIN
        RETURN NULL;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_x_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + pos[1] + x;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

 CREATE OR REPLACE FUNCTION raster_y_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[])
    RETURNS FLOAT AS
    $$
    DECLARE
        x float := 0;
    BEGIN
        IF NOT args[1] IS NULL THEN
            x := args[1]::float;
        END IF;
        RETURN pixel + pos[2] + x;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE;

-- Test NULL raster
SELECT ST_MapAlgebra(NULL, 1, NULL, '[rast] + 20', 2) IS NULL FROM ST_TestRaster(0, 0, -1) rast;

-- Test empty raster
SELECT 'T1', ST_IsEmpty(ST_MapAlgebra(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, '[rast] + 20', 2));

-- Test hasnoband raster
SELECT 'T2', ST_HasNoBand(ST_MapAlgebra(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, 0), 1, NULL, '[rast] + 20', 2));

-- Test hasnodata value
SELECT 'T3', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(ST_SetBandNoDataValue(rast, NULL), 1, NULL, '[rast] + 20', 2), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;

-- Test nodata value
SELECT 'T4', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, NULL, '[rast] + 20', 2), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;

-- Test 'rast' expression
SELECT 'T5', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, NULL, '[rast]', 2), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;
SELECT 'T6', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(ST_SetBandNoDataValue(rast, NULL), 1, NULL, '[rast]'), 1, 1) FROM ST_TestRaster(0, 0, -1) rast;

-- Test pixeltype
SELECT 'T7', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, '4BUI', '[rast] + 20', 2), 1, 1) FROM ST_TestRaster(0, 0, 100) rast;
SELECT 'T8', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, '4BUId', '[rast] + 20', 2), 1, 1) FROM ST_TestRaster(0, 0, 100) rast;
SELECT 'T9', ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, '2BUI', '[rast] + 20', 2), 1, 1) FROM ST_TestRaster(0, 0, 101) rast;

-- Test '[rast.x]', '[rast.y]' and '[rast.val]' substitutions expression
SELECT 'T10.'||x||'.'||y, ST_Value(rast, x, y),
  ST_Value(ST_MapAlgebra(rast, 1, NULL, 'ceil([rast]*[rast.x]/[rast.y]+[rast.val])'), x, y)
FROM ST_TestRaster(0, 0, 10) rast,
     generate_series(6, 8) as x,
     generate_series(2, 4) as y
ORDER BY x, y;

-- Test divide by zero; blows up the whole query, not just the SPI
SELECT ST_Value(rast, 1, 1),
    ST_Value(ST_MapAlgebra(rast, 1, NULL, '[rast]/0'), 1, 1)
FROM ST_TestRaster(0, 0, 10) rast;

-- Test evaluations to null (see #1523)
WITH op AS ( select rast AS rin,
  ST_MapAlgebra(rast, 1, NULL, 'SELECT g from (SELECT NULL::double precision as g) as foo', 2)
  AS rout FROM ST_TestRaster(0, 0, 10) rast )
SELECT 'T11.1', ST_Value(rin, 1, 1), ST_Value(rout, 1, 1) FROM op;
WITH op AS ( select rast AS rin,
  ST_MapAlgebra(rast, 1, NULL, 'SELECT g from (select [rast],NULL::double precision as g) as foo', 2)
  AS rout FROM ST_TestRaster(0, 0, 10) rast )
SELECT 'T11.2', ST_Value(rin, 1, 1), ST_Value(rout, 1, 1) FROM op;

-- Test pracine's new bug #1638
SELECT 'T12',
  ST_Value(rast, 1, 2) = 1,
  ST_Value(rast, 2, 1) = 2,
  ST_Value(rast, 4, 3) = 4,
  ST_Value(rast, 3, 4) = 3
  FROM ST_MapAlgebra(
    ST_AddBand(
      ST_MakeEmptyRaster(10, 10, 0, 0, 0.001, 0.001, 0, 0, 4269), 
      '8BUI'::text, 
      1, 
      0
    ), 
    '32BUI', 
    '[rast.x]'
  ) AS rast; 

DROP FUNCTION ST_TestRaster(ulx float8, uly float8, val float8);
DROP FUNCTION raster_plus_twenty(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_plus_arg1(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_polynomial(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_nullage(pixel FLOAT, VARIADIC args TEXT[]);
DROP FUNCTION raster_x_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[]);
DROP FUNCTION raster_y_plus_arg(pixel FLOAT, pos INT[], VARIADIC args TEXT[]);

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
	(SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebra(
		r1.rast, r2.rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'INTERSECTION', st_mapalgebra(
		r1.rast, r2.rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'INTERSECTION', st_mapalgebra(
		NULL::raster, rast, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'INTERSECTION', st_mapalgebra(
		rast, NULL::raster, '[rast1.val]', '32BF', 'INTERSECTION'
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'INTERSECTION', st_mapalgebra(
		NULL::raster, NULL::raster, '[rast1.val]', '32BF', 'INTERSECTION'
	)
;

-- UNION
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'UNION', st_mapalgebra(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'UNION', st_mapalgebra(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'UNION', st_mapalgebra(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '100', '200', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'UNION', st_mapalgebra(
		r1.rast, r2.rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '100', '200', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'UNION', st_mapalgebra(
		NULL::raster, rast, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'UNION', st_mapalgebra(
		rast, NULL::raster, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'UNION', st_mapalgebra(
		NULL::raster, NULL::raster, '(([rast1.val] + [rast2.val])/2.)::numeric', '32BF', 'UNION', '[rast2.val]', '[rast1.val]', NULL
	)
;

-- FIRST
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebra(
		r1.rast, r2.rast, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'FIRST', st_mapalgebra(
		r1.rast, r2.rast, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'FIRST', st_mapalgebra(
		NULL::raster, rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'FIRST', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'FIRST', st_mapalgebra(
		rast, NULL::raster, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'FIRST', st_mapalgebra(
		NULL::raster, NULL::raster, 'CASE WHEN [rast2.val] IS NOT NULL THEN NULL ELSE [rast1.val] END', '32BF', 'FIRST', NULL, '[rast1.val]', NULL
	)
;

-- SECOND
INSERT INTO raster_mapalgebra_out
	(SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebra(
		r1.rast, r2.rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 0
		AND r2.rid BETWEEN 1 AND 9
	) UNION ALL (
	SELECT r1.rid, r2.rid, 'SECOND', st_mapalgebra(
		r1.rast, r2.rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra r1
	JOIN raster_mapalgebra r2
		ON r1.rid != r2.rid
	WHERE r1.rid = 10
		AND r2.rid BETWEEN 11 AND 19)
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, rid, 'SECOND', st_mapalgebra(
		NULL::raster, rast, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT rid, NULL AS rid, 'SECOND', st_mapalgebra(
		rast, NULL::raster, 'CASE WHEN [rast1.val] IS NOT NULL THEN NULL ELSE [rast2.val] END', '32BF', 'SECOND', '[rast2.val]', NULL, NULL
	)
	FROM raster_mapalgebra
;

INSERT INTO raster_mapalgebra_out
	SELECT NULL AS rid, NULL AS rid, 'SECOND', st_mapalgebra(
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
