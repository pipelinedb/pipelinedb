----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
-- 
----------------------------------------------------------------------

-- Note: The functions found in this file are for exclusive usage of ST_MapAlgebra2

CREATE OR REPLACE FUNCTION max(a int, b int)
    RETURNS int
    AS 'SELECT CASE WHEN $1 < $2 THEN $2 ELSE $1 END'
    LANGUAGE 'SQL' IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION min(a int, b int)
    RETURNS int
    AS 'SELECT CASE WHEN $1 < $2 THEN $1 ELSE $2 END'
    LANGUAGE 'SQL' IMMUTABLE STRICT;

--DROP FUNCTION _MapAlgebraParts(r1x int, r1y int, r1w int, r1h int, r2x int, r2y int, r2w int, r2h int);
CREATE OR REPLACE FUNCTION _MapAlgebraParts(r1x int,
                                            r1y int,
                                            r1w int,
                                            r1h int,
                                            r2x int,
                                            r2y int,
                                            r2w int,
                                            r2h int)
    RETURNS int[]
    AS $$
    DECLARE
        z11x int;
        z11y int;
        z11w int;
        z11h int;
        z12x int;
        z12y int;
        z12w int;
        z12h int;
        z13x int;
        z13y int;
        z13w int;
        z13h int;
        z14x int;
        z14y int;
        z14w int;
        z14h int;
        z21x int;
        z21y int;
        z21w int;
        z21h int;
        z22x int;
        z22y int;
        z22w int;
        z22h int;
        z23x int;
        z23y int;
        z23w int;
        z23h int;
        z24x int;
        z24y int;
        z24w int;
        z24h int;
        zcx int;
        zcy int;
        zcw int;
        zch int;
    BEGIN
        z11x := r1x;
        z11y := r1y;
        z11w := r1w;
        z11h := min(max(0, r2y - r1y), r1h);

        z12x := r1x;
        z12y := z11y + z11h;
        z12w := min(max(0, r2x - r1x), r1w);
        z12h := max(0, min(max(0, r2y + r2h - (r1y + z11h)), z11y + r1h - z12y));

        z13x := max(min(r1x + r1w, r2x + r2w), r1x);
        z13y := z12y;
        z13w := min(max(0, r1x + r1w - (r2x + r2w)), r1w);
        z13h := z12h;

        z14x := r1x;
        z14y := z12y + z12h;
        z14w := r1w;
        z14h := min(max(0, r1y + r1h - (r2y + r2h)), r1h);

        z21x := r2x;
        z21y := r2y;
        z21w := r2w;
        z21h := min(max(0, r1y - r2y), r2h);

        z22x := r2x;
        z22y := z21y + z21h;
        z22w := min(max(0, r1x - r2x), r2w);
        z22h := max(0, min(max(0, r1y + r1h - (r2y + z21h)), z21y + r2h - z22y));

        z23x := max(min(r2x + r2w, r1x + r1w), r2x);
        z23y := z22y;
        z23w := min(max(0, r2x + r2w - (r1x + r1w)), r2w);
        z23h := z22h;

        z24x := r2x;
        z24y := z22y + z22h;
        z24w := r2w;
        z24h := min(max(0, r2y + r2h - (r1y + r1h)), r2h);

        zcx := z12x + z12w;
        zcy := z12y;
        zcw := z13x - (z12x + z12w);
        zch := z14y - z12y;

        -- Merge z11 with z12 if they are continuous parts of the same vertical bar
        IF z12h > 0 AND z12x = z11x AND z12w = z11w THEN
            z12y := z11y;
            z12h := z11h + z12h;
            z11h := 0;
        END IF;

        -- Merge z11 with z13 if they are continuous parts of the same vertical bar
        IF z13h > 0 AND z13x = z11x AND z13w = z11w THEN
            z13y := z11y;
            z13h := z11h + z13h;
            z11h := 0;
        END IF;

        -- Merge z12 with z14 if they are continuous parts of the same vertical bar
        IF z14h > 0 AND z14x = z12x AND z14w = z12w THEN
            z14y := z12y;
            z14h := z12h + z14h;
            z12h := 0;
        END IF;

        -- Merge z13 with z14 if they are continuous parts of the same vertical bar
        IF z14h > 0 AND z14x = z13x AND z14w = z13w THEN
            z14y := z13y;
            z14h := z13h + z14h;
            z13h := 0;
        END IF;

        -- Merge z21 with z22 if they are continuous parts of the same vertical bar
        IF z22h > 0 AND z22x = z21x AND z22w = z21w THEN
            z22y := z21y;
            z22h := z21h + z22h;
            z21h := 0;
        END IF;

        -- Merge z21 with z23 if they are continuous parts of the same vertical bar
        IF z23h > 0 AND z23x = z21x AND z23w = z21w THEN
            z23y := z21y;
            z23h := z21h + z23h;
            z21h := 0;
        END IF;

        -- Merge z22 with z24 if they are continuous parts of the same vertical bar
        IF z24h > 0 AND z24x = z22x AND z24w = z22w THEN
            z24y := z22y;
            z24h := z22h + z24h;
            z22h := 0;
        END IF;

        -- Merge z23 with z24 if they are continuous parts of the same vertical bar
        IF z24h > 0 AND z24x = z23x AND z24w = z23w THEN
            z24y := z23y;
            z24h := z23h + z24h;
            z23h := 0;
        END IF;
        RETURN ARRAY[z11x, z11y, z11w, z11h, z12x, z12y, z12w, z12h, z13x, z13y, z13w, z13h, z14x, z14y, z14w, z14h, z21x, z21y, z21w, z21h, z22x, z22y, z22w, z22h, z23x, z23y, z23w, z23h, z24x, z24y, z24w, z24h, zcx, zcy, zcw, zch];
    END;
    $$
    LANGUAGE 'plpgsql';

--DROP FUNCTION _MapAlgebraPartsGeom(r1x int, r1y int, r1w int, r1h int, r2x int, r2y int, r2w int, r2h int);
CREATE OR REPLACE FUNCTION _MapAlgebraPartsGeom(nx int,
                                                ny int,
                                                r1x int,
                                                r1y int,
                                                r1w int,
                                                r1h int,
                                                r2x int,
                                                r2y int,
                                                r2w int,
                                                r2h int) 
    RETURNS SETOF geometry AS 
    $$
    DECLARE
    BEGIN
	RETURN NEXT ST_MakeBox2D(ST_Point(10 * ny + r1x, -10 * nx + 5 - r1y), ST_Point(10 * ny + r1x + r1w, -10 * nx + 5 - (r1y + r1h)))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(10 * ny + r2x, -10 * nx + 5 - r2y), ST_Point(10 * ny + r2x + r2w, -10 * nx + 5 - (r2y + r2h)))::geometry;
	RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION _MapAlgebraAllPartsGeom(r1x int,
                                                r1y int,
                                                r1w int,
                                                r1h int,
                                                r2x int,
                                                r2y int,
                                                r2w int,
                                                r2h int) 
    RETURNS SETOF geometry AS 
    $$
    DECLARE
	z int[];
    BEGIN
	z := _MapAlgebraParts(r1x, r1y, r1w, r1h, r2x, r2y, r2w, r2h);
	RETURN NEXT ST_MakeBox2D(ST_Point(z[1], z[2]), ST_Point(z[1] + z[3], z[2] + z[4]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[5], z[6]), ST_Point(z[5] + z[7], z[6] + z[8]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[9], z[10]), ST_Point(z[9] + z[11], z[10] + z[12]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[13], z[14]), ST_Point(z[13] + z[15], z[14] + z[16]))::geometry;

	RETURN NEXT ST_MakeBox2D(ST_Point(z[17], z[18]), ST_Point(z[17] + z[19], z[18] + z[20]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[21], z[22]), ST_Point(z[21] + z[23], z[22] + z[24]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[25], z[26]), ST_Point(z[25] + z[27], z[26] + z[28]))::geometry;
	RETURN NEXT ST_MakeBox2D(ST_Point(z[29], z[30]), ST_Point(z[29] + z[31], z[30] + z[32]))::geometry;

	RETURN NEXT ST_MakeBox2D(ST_Point(z[33], z[34]), ST_Point(z[33] + z[35], z[34] + z[36]))::geometry;
	RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

SELECT asbinary(_MapAlgebraAllPartsGeom(0, 0, 1, 1, 1, 0, 1, 1))

CREATE OR REPLACE FUNCTION X1W1X2W2() 
    RETURNS SETOF record AS 
    $$
    DECLARE
	x1w1x2w2 record;
    BEGIN
	x1w1x2w2 := (0, 3-4, 2, 0-4, 2);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (1, 2-4, 3, 0-4, 2);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (2, 2-4, 3, 0-4, 3);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (3, 2-4, 3, 0-4, 5);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (4, 1-4, 3, 0-4, 5);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (5, 1-4, 4, 1-4, 2);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (6, 1-4, 3, 1-4, 3);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (7, 1-4, 2, 1-4, 4);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (8, 0-4, 5, 1-4, 3);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (9, 0-4, 5, 1-4, 4);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (10, 0-4, 3, 2-4, 3);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (11, 0-4, 3, 3-4, 2);
	RETURN NEXT x1w1x2w2;
	x1w1x2w2 := (12, 0-4, 2, 3-4, 2);
	RETURN NEXT x1w1x2w2;
	RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION Y1H1Y2H2() 
    RETURNS SETOF record AS 
    $$
    DECLARE
	y1h1y2h2 record;
    BEGIN
	y1h1y2h2 := (0, 3-4, 2, 0-4, 2);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (1, 2-4, 3, 0-4, 2);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (2, 2-4, 3, 0-4, 3);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (3, 2-4, 3, 0-4, 5);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (4, 1-4, 3, 0-4, 5);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (5, 1-4, 4, 1-4, 2);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (6, 1-4, 3, 1-4, 3);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (7, 1-4, 2, 1-4, 4);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (8, 0-4, 5, 1-4, 3);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (9, 0-4, 5, 1-4, 4);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (10, 0-4, 3, 2-4, 3);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (11, 0-4, 3, 3-4, 2);
	RETURN NEXT y1h1y2h2;
	y1h1y2h2 := (12, 0-4, 2, 3-4, 2);
	RETURN NEXT y1h1y2h2;
	RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

_MapAlgebraParts(r1x, r1y, r1w, r1h, r2x, r2y, r2w, r2h) 

SELECT nx, x1, w1, x2, w2 FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int);

SELECT nx, ny, x1, w1, x2, w2, y1, h1, y2, h2 
FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int);

SELECT nx, ny, x1, w1, x2, w2, y1, h1, y2, h2, asbinary(_MapAlgebraPartsGeom(nx, ny, x1, y1, w1, h1, x2, y2, w2, h2)) 
FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int);

SELECT asbinary(_MapAlgebraPartsGeom(nx, ny, x1, y1, w1, h1, x2, y2, w2, h2)) 
FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int);


-- First series of zones covering raster 1
SELECT nx, ny, map[1], map[2], map[3], map[4],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[1], -10 * nx + 5 - map[2]), ST_Point(10 * ny + map[1] + map[3], -10 * nx + 5 - (map[2] + map[4])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Second series of zones covering raster 1
SELECT nx, ny, map[5], map[6], map[7], map[8],
       asbinary(ST_Point(10 * ny + map[5], -10 * nx + 5 - map[6])::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

SELECT nx, ny, map[5], map[6], map[7], map[8],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[5], -10 * nx + 5 - map[6]), ST_Point(10 * ny + map[5] + map[7], -10 * nx + 5 - (map[6] + map[8])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Third series of zones covering raster 1
SELECT nx, ny, map[9], map[10], map[11], map[12],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[9], -10 * nx + 5 - map[10]), ST_Point(10 * ny + map[9] + map[11], -10 * nx + 5 - (map[10] + map[12])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Fourth series of zones covering raster 1
SELECT nx, ny, map[13], map[14], map[15], map[16],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[13], -10 * nx + 5 - map[14]), ST_Point(10 * ny + map[13] + map[15], -10 * nx + 5 - (map[14] + map[16])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- First series of zones covering raster 2
SELECT nx, ny, map[17], map[18], map[19], map[20],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[17], -10 * nx + 5 - map[18]), ST_Point(10 * ny + map[17] + map[19], -10 * nx + 5 - (map[18] + map[20])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Second series of zones covering raster 2
SELECT nx, ny, map[21], map[22], map[23], map[24],
       asbinary(ST_Point(10 * ny + map[21], -10 * nx + 5 - map[22])::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

SELECT nx, ny, map[21], map[22], map[23], map[24],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[21], -10 * nx + 5 - map[22]), ST_Point(10 * ny + map[21] + map[23], -10 * nx + 5 - (map[22] + map[24])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Third series of zones covering raster 2
SELECT nx, ny, map[25], map[26], map[27], map[28],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[25], -10 * nx + 5 - map[26]), ST_Point(10 * ny + map[25] + map[27], -10 * nx + 5 - (map[26] + map[28])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Fourth series of zones covering raster 2
SELECT nx, ny, map[29], map[30], map[31], map[32],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[28], -10 * nx + 5 - map[29]), ST_Point(10 * ny + map[28] + map[30], -10 * nx + 5 - (map[29] + map[31])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;

-- Common zone
SELECT nx, ny, map[33], map[34], map[35], map[36],
       asbinary(ST_MakeBox2D(ST_Point(10 * ny + map[33], -10 * nx + 5 - map[34]), ST_Point(10 * ny + map[33] + map[35], -10 * nx + 5 - (map[34] + map[36])))::geometry)
FROM (
      SELECT nx, ny, x1, y1, w1, h1, x2, y2, w2, h2, _MapAlgebraParts(x1, y1, w1, h1, x2, y2, w2, h2) as map
      FROM X1W1X2W2() as (nx int, x1 int, w1 int, x2 int, w2 int), Y1H1Y2H2() as (ny int, y1 int, h1 int, y2 int, h2 int)
     ) as foo;