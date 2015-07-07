----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- Note: The functions provided in this script are in developement. Do not use.

DROP TYPE IF EXISTS rastexpr CASCADE;
CREATE TYPE rastexpr AS (
    rast raster,
    f_expression text,
    f_nodata1expr text, 
    f_nodata2expr text,
    f_nodatanodataval double precision 
);

--DROP FUNCTION MapAlgebra4Union(rast1 raster, rast2 raster, expression text);
CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 raster, 
                                            rast2 raster, 
                                            p_expression text, 
                                            p_nodata1expr text, 
                                            p_nodata2expr text, 
                                            p_nodatanodataval double precision, 
                                            t_expression text, 
                                            t_nodata1expr text, 
                                            t_nodata2expr text, 
                                            t_nodatanodataval double precision)
    RETURNS raster AS 
    $$
    DECLARE
        t_raster raster;
        p_raster raster;
    BEGIN
        -- With the new ST_MapAlgebraExpr we must split the main expression in three expressions: expression, nodata1expr, nodata2expr
        -- ST_MapAlgebraExpr(rast1 raster, band1 integer, rast2 raster, band2 integer, expression text, pixeltype text, extentexpr text, nodata1expr text, nodata2expr text, nodatanodatadaexpr double precision)
        -- We must make sure that when NULL is passed as the first raster to ST_MapAlgebraExpr, ST_MapAlgebraExpr resolve the nodata1expr
        IF upper(p_expression) = 'LAST' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, '[rast2.val]'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
        ELSIF upper(p_expression) = 'FIRST' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, '[rast1.val]'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
        ELSIF upper(p_expression) = 'MIN' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, 'LEAST([rast1.val], [rast2.val])'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
        ELSIF upper(p_expression) = 'MAX' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, 'GREATEST([rast1.val], [rast2.val])'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
        ELSIF upper(p_expression) = 'COUNT' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, '[rast1.val] + 1'::text, NULL::text, 'UNION'::text, '1'::text, '[rast1.val]'::text, 0::double precision);
        ELSIF upper(p_expression) = 'SUM' THEN
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, '[rast1.val] + [rast2.val]'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
        ELSIF upper(p_expression) = 'RANGE' THEN
            t_raster = ST_MapAlgebraExpr(rast1, 2, rast2, 1, 'LEAST([rast1.val], [rast2.val])'::text, NULL::text, 'UNION'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision);
            p_raster := MapAlgebra4Union(rast1, rast2, 'MAX'::text, NULL::text, NULL::text, NULL::double precision, NULL::text, NULL::text, NULL::text, NULL::double precision);
            RETURN ST_AddBand(p_raster, t_raster, 1, 2);
        ELSIF upper(p_expression) = 'MEAN' THEN
            t_raster = ST_MapAlgebraExpr(rast1, 2, rast2, 1, '[rast1.val] + 1'::text, NULL::text, 'UNION'::text, '1'::text, '[rast1.val]'::text, 0::double precision);
            p_raster := MapAlgebra4Union(rast1, rast2, 'SUM'::text, NULL::text, NULL::text, NULL::double precision, NULL::text, NULL::text, NULL::text, NULL::double precision);
            RETURN ST_AddBand(p_raster, t_raster, 1, 2);
        ELSE
            IF t_expression NOTNULL AND t_expression != '' THEN
                t_raster = ST_MapAlgebraExpr(rast1, 2, rast2, 1, t_expression, NULL::text, 'UNION'::text, t_nodata1expr, t_nodata2expr, t_nodatanodataval::double precision);
                p_raster = ST_MapAlgebraExpr(rast1, 1, rast2, 1, p_expression, NULL::text, 'UNION'::text, p_nodata1expr, p_nodata2expr, p_nodatanodataval::double precision);
                RETURN ST_AddBand(p_raster, t_raster, 1, 2);
            END IF;     
            RETURN ST_MapAlgebraExpr(rast1, 1, rast2, 1, p_expression, NULL, 'UNION'::text, NULL::text, NULL::text, NULL::double precision);
        END IF;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION MapAlgebra4UnionFinal3(rast rastexpr)
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_MapAlgebraExpr(rast.rast, 1, rast.rast, 2, rast.f_expression, NULL::text, 'UNION'::text, rast.f_nodata1expr, rast.f_nodata2expr, rast.f_nodatanodataval);
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION MapAlgebra4UnionFinal1(rast rastexpr)
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        IF upper(rast.f_expression) = 'RANGE' THEN
            RETURN ST_MapAlgebraExpr(rast.rast, 1, rast.rast, 2, '[rast1.val] - [rast2.val]'::text, NULL::text, 'UNION'::text, NULL::text, NULL::text, NULL::double precision);
        ELSEIF upper(rast.f_expression) = 'MEAN' THEN
            RETURN ST_MapAlgebraExpr(rast.rast, 1, rast.rast, 2, 'CASE WHEN [rast2.val] > 0 THEN [rast1.val] / [rast2.val]::float8 ELSE NULL END'::text, NULL::text, 'UNION'::text, NULL::text, NULL::text, NULL::double precision);
        ELSE
            RETURN rast.rast;
        END IF;
    END;
    $$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text, 
                        p_nodata1expr text, 
                        p_nodata2expr text, 
                        p_nodatanodataval double precision, 
                        t_expression text, 
                        t_nodata1expr text, 
                        t_nodata2expr text, 
                        t_nodatanodataval double precision, 
                        f_expression text,
                        f_nodata1expr text, 
                        f_nodata2expr text, 
                        f_nodatanodataval double precision)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, $4, $5, $6, $7, $8, $9, $10), $11, $12, $13, $14)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text, 
                        t_expression text, 
                        f_expression text)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, NULL, NULL, NULL, $4, NULL, NULL, NULL), $5, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text, 
                        p_nodata1expr text, 
                        p_nodata2expr text, 
                        p_nodatanodataval double precision, 
                        t_expression text, 
                        t_nodata1expr text, 
                        t_nodata2expr text, 
                        t_nodatanodataval double precision)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, $4, $5, $6, $7, $8, $9, $10), NULL, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text, 
                        t_expression text)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, NULL, NULL, NULL, $4, NULL, NULL, NULL), NULL, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text, 
                        p_nodata1expr text, 
                        p_nodata2expr text, 
                        p_nodatanodataval double precision)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, $4, $5, $6, NULL, NULL, NULL, NULL), NULL, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster, 
                        p_expression text)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, $3, NULL, NULL, NULL, NULL, NULL, NULL, NULL), $3, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

CREATE OR REPLACE FUNCTION MapAlgebra4Union(rast1 rastexpr, 
                        rast2 raster)
    RETURNS rastexpr 
    AS $$
        SELECT (MapAlgebra4Union(($1).rast, $2, 'LAST', NULL, NULL, NULL, NULL, NULL, NULL, NULL), NULL, NULL, NULL, NULL)::rastexpr
    $$ LANGUAGE 'SQL';

--DROP AGGREGATE ST_Union(raster, text, text, text, double precision, text, text, text, double precision, text, text, text, double precision);
CREATE AGGREGATE ST_Union(raster, text, text, text, double precision, text, text, text, double precision, text, text, text, double precision) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr,
    FINALFUNC = MapAlgebra4UnionFinal3
);

--DROP AGGREGATE ST_Union(raster, text, text, text);
CREATE AGGREGATE ST_Union(raster, text, text, text) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr,
    FINALFUNC = MapAlgebra4UnionFinal3
);

--DROP AGGREGATE ST_Union(raster, text, text, text, double precision, text, text, text, double precision);
CREATE AGGREGATE ST_Union(raster, text, text, text, double precision, text, text, text, double precision) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr
);

--DROP AGGREGATE ST_Union(raster, text, text);
CREATE AGGREGATE ST_Union(raster, text, text) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr
);

--DROP AGGREGATE ST_Union(raster, text, text, text, double precision);
CREATE AGGREGATE ST_Union(raster, text, text, text, double precision) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr,
    FINALFUNC = MapAlgebra4UnionFinal1
);

--DROP AGGREGATE ST_Union(raster, text);
CREATE AGGREGATE ST_Union(raster, text) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr,
    FINALFUNC = MapAlgebra4UnionFinal1
);

--DROP AGGREGATE ST_Union(raster);
CREATE AGGREGATE ST_Union(raster) (
    SFUNC = MapAlgebra4Union,
    STYPE = rastexpr
);

-- Test ST_TestRaster
SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(0, 0, 2)) rast) foo;

-- Test St_Union
SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast, 'mean'), 1) rast 
      FROM (SELECT ST_TestRaster(1, 0, 6) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(1, 1, 4) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(-1, 0, 6) AS rast
            UNION ALL 
            SELECT ST_TestRaster(0, 0, 2) AS rast 
            ) foi) foo

-- Test St_Union merging a blending merge of disjoint raster
SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast, 'last'), 1) rast 
      FROM (SELECT ST_TestRaster(0, 0, 1) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(3, 0, 2) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(3, 3, 4) AS rast
            UNION ALL 
            SELECT ST_TestRaster(0, 3, 3) AS rast 
            ) foi) foo

      
-- Explicit implementation of 'MEAN' to make sure directly passing expressions works properly
SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast, '[rast1.val] + [rast2.val]'::text, '[rast2.val]'::text, '[rast1.val]'::text, NULL::double precision,
                                               '[rast1.val] + 1'::text, '1'::text, '[rast1.val]'::text, 0::double precision,
                                               'CASE WHEN [rast2.val] > 0 THEN [rast1.val] / [rast2.val]::float8 ELSE NULL END'::text, NULL::text, NULL::text, NULL::double precision), 1) rast 
      FROM (SELECT ST_TestRaster(0, 0, 2) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(1, 1, 4) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(1, 0, 6) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(-1, 0, 6) AS rast
            ) foi) foo

-- Pseudo-explicit implementation of 'MEAN' using other predefined functions. Do not work yet...
SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast, 'SUM'::text, 
                                               'COUNT'::text, 
                                               'CASE WHEN [rast2.val] > 0 THEN [rast1.val] / [rast2.val]::float8 ELSE NULL END'::text), 1) rast 
      FROM (SELECT ST_TestRaster(0, 0, 2) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(1, 1, 4) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(1, 0, 6) AS rast 
            UNION ALL 
            SELECT ST_TestRaster(-1, 0, 6) AS rast
            ) foi) foo


SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast), 1) AS rast 
      FROM (SELECT ST_TestRaster(0, 0, 1) AS rast UNION ALL SELECT ST_TestRaster(2, 0, 2)
           ) foi
     ) foo


SELECT ST_AsBinary((rast).geom), (rast).val 
FROM (SELECT ST_PixelAsPolygons(ST_Union(rast, 'mean'), 1) AS rast 
      FROM (SELECT ST_TestRaster(0, 0, 1) AS rast UNION ALL SELECT ST_TestRaster(1, 0, 2) UNION ALL SELECT ST_TestRaster(0, 1, 6)
           ) foi
     ) foo
