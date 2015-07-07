----------------------------------------------------------------------
--
-- Copyright (c) 2011 David Zwarg <dzwarg@azavea.com>
--
----------------------------------------------------------------------

--
-- Helper method to get the smallest value in a raster, based on the pixeltype.
--
CREATE OR REPLACE FUNCTION ST_MinPossibleValue(pixeltype text)
    RETURNS float8 AS
    $$
    DECLARE
        newval int := 0;
    BEGIN
        newval := CASE 
            WHEN pixeltype = '1BB' THEN 0
            WHEN pixeltype = '2BUI' THEN 0
            WHEN pixeltype = '4BUI' THEN 0
            WHEN pixeltype = '8BUI' THEN 0
            WHEN pixeltype = '8BSI' THEN -128
            WHEN pixeltype = '16BUI' THEN 0
            WHEN pixeltype = '16BSI' THEN -32768
            WHEN pixeltype = '32BUI' THEN 0
            WHEN pixeltype = '32BSI' THEN -2147483648
            WHEN pixeltype = '32BF' THEN -2147483648 -- Could not find a function returning the smallest real yet
            WHEN pixeltype = '64BF' THEN -2147483648 -- Could not find a function returning the smallest float8 yet
        END;
        RETURN newval;
    END;
    $$
    LANGUAGE 'plpgsql';

--
--Test rasters
--
CREATE OR REPLACE FUNCTION ST_TestRaster(h integer, w integer, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(h, w, 0, 0, 1, 1, 0, 0, -1), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

--------------------------------------------------------------------
-- ST_MapAlgebraFctNgb - (one raster version) Return a raster which values 
--                 are the result of a PLPGSQL user function involving a 
--                 neighborhood of values from the input raster band.
-- Arguments 
-- rast raster -  Raster on which the user function is evaluated.
-- band integer - Band number of the raster to be evaluated. Default to 1.
-- pixeltype text - Pixeltype assigned to the resulting raster. User function
--                  results are truncated to this type. Default to the 
--                  pixeltype of the first raster.
-- ngbwidth integer - The width of the neighborhood, in cells.
-- ngbheight integer - The heigh of the neighborhood, in cells.
-- userfunction regprocedure - PLPGSQL user function to apply to with neighborhod pixels.
-- args variadic text[] - Arguments to pass into the user function.
--------------------------------------------------------------------
DROP FUNCTION IF EXISTS ST_MapAlgebraFctNgb(rast raster, band integer, pixeltype text, ngbwidth integer, ngbheight integer, userfunction text, nodatamode text, variadic args text[]);
CREATE OR REPLACE FUNCTION ST_MapAlgebraFctNgb(rast raster, band integer, pixeltype text, ngbwidth integer, ngbheight integer, userfunction text, nodatamode text, variadic args text[]) 
    RETURNS raster AS 
    $$
    DECLARE
        width integer;
        height integer;
        x integer;
        y integer;
        r float;
        newrast raster;
        newval float8;
        newpixeltype text;
        newnodatavalue float;
        newinitialvalue float;
        neighborhood float[][];
        nrow float[];
        valuesub boolean;
    BEGIN
        -- Check if raster is NULL
        IF rast IS NULL THEN
            RAISE NOTICE 'ST_MapAlgebraFctNgb: Raster is NULL. Returning NULL';
            RETURN NULL;
        END IF;

        width := ST_Width(rast);
        height := ST_Height(rast);

        -- Create a new empty raster having the same georeference as the provided raster
        newrast := ST_MakeEmptyRaster(width, height, ST_UpperLeftX(rast), ST_UpperLeftY(rast), ST_ScaleX(rast), ST_ScaleY(rast), ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast));

        -- If this new raster is empty (width = 0 OR height = 0) then there is nothing to compute and we return it right now
        IF ST_IsEmpty(newrast) THEN 
            RAISE NOTICE 'ST_MapAlgebraFctNgb: Raster is empty. Returning an empty raster';
            RETURN newrast;
        END IF;
        
        -- Check if rast has the required band. Otherwise return a raster without band
        IF ST_HasNoBand(rast, band) THEN 
            RAISE NOTICE 'ST_MapAlgebraFctNgb: Raster does not have the required band. Returning a raster without a band';
            RETURN newrast;
        END IF;
        
        -- Set the new pixeltype
        newpixeltype := pixeltype;
        IF newpixeltype IS NULL THEN
            newpixeltype := ST_BandPixelType(rast, band);
        ELSIF newpixeltype != '1BB' AND newpixeltype != '2BUI' AND newpixeltype != '4BUI' AND newpixeltype != '8BSI' AND newpixeltype != '8BUI' AND 
               newpixeltype != '16BSI' AND newpixeltype != '16BUI' AND newpixeltype != '32BSI' AND newpixeltype != '32BUI' AND newpixeltype != '32BF' AND newpixeltype != '64BF' THEN
            RAISE EXCEPTION 'ST_MapAlgebraFctNgb: Invalid pixeltype "%". Aborting.', newpixeltype;
        END IF;

        -- Check for notada value
        newnodatavalue := ST_BandNodataValue(rast, band);
        IF newnodatavalue IS NULL OR newnodatavalue < ST_MinPossibleValue(newpixeltype) OR newnodatavalue > (-ST_MinPossibleValue(newpixeltype) - 1) THEN
            RAISE NOTICE 'ST_MapAlgebraFctNgb: Source raster does not have a nodata value or is out of range for the new raster pixeltype, nodata value for new raster set to the min value possible';
            newnodatavalue := ST_MinPossibleValue(newpixeltype);
        END IF;

        -- We set the initial value of the future band to nodata value. 
        -- If nodatavalue is null then the raster will be initialise to ST_MinPossibleValue 
        -- but all the values should be recomputed anyway.
        newinitialvalue := newnodatavalue;

        -- Optimization: If the raster is only filled with nodata value return right now a raster filled with the nodatavalueexpr
        IF ST_BandIsNoData(rast, band) THEN
            RETURN ST_AddBand(newrast, newpixeltype, newinitialvalue, newnodatavalue);
        END IF;
        
        --Create the raster receiving all the computed values. Initialize it to the new initial value.
        newrast := ST_AddBand(newrast, newpixeltype, newinitialvalue, newnodatavalue);

        IF nodatamode = 'value' THEN
            valuesub := TRUE;
        ELSIF nodatamode != 'ignore' AND nodatamode != 'NULL' AND NOT nodatamode ~ E'^[\-+]?[0-9]*(\.[0-9]+(e[\-+]?[0-9]+)?)?$' THEN
            RAISE NOTICE 'ST_MapAlgebraFctNgb: ''nodatamode'' parameter must be one of: value, ignore, NULL, or a numerical value.';
            RETURN NULL;
        END IF;

        -- Computation loop
        FOR x IN (1+ngbwidth)..(width-ngbwidth) LOOP
            FOR y IN (1+ngbheight)..(height-ngbheight) LOOP

                -- create a matrix of the pixel values in the neighborhood
                neighborhood := ARRAY[[]]::float[];
                FOR u IN (x-ngbwidth)..(x+ngbwidth) LOOP
                    nrow := ARRAY[]::float[];
                    FOR v in (y-ngbheight)..(y+ngbheight) LOOP
                        nrow := nrow || ARRAY[ ST_Value(rast, band, u, v)::float ];
                    END LOOP;
                    neighborhood := neighborhood || ARRAY[ nrow ];
                END LOOP;

                -- RAISE NOTICE 'Neighborhood: %', neighborhood;

                IF valuesub IS TRUE THEN
                    nodatamode := ST_Value(rast, band, x, y);
                    -- special case where NULL needs to be quoted
                    IF nodatamode IS NULL THEN
                        nodatamode := 'NULL';
                    END IF;
                END IF;

                EXECUTE 'SELECT ' ||userfunction || '(' || quote_literal(neighborhood) || ', ' || quote_literal(nodatamode) || ', NULL)' INTO r;

                newrast = ST_SetValue(newrast, band, x, y, r);
            END LOOP;
        END LOOP;
        RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';

--
-- A simple 'callback' user function that sums up all the values in a neighborhood.
--
CREATE OR REPLACE FUNCTION ST_Sum(matrix float[][], nodatamode text, variadic args text[])
    RETURNS float AS
    $$
    DECLARE
        x1 integer;
        x2 integer;
        y1 integer;
        y2 integer;
        sum float;
    BEGIN
        sum := 0;
        raise notice 'in callback: %', nodatamode;
        FOR x in array_lower(matrix, 1)..array_upper(matrix, 1) LOOP
            FOR y in array_lower(matrix, 2)..array_upper(matrix, 2) LOOP
                IF matrix[x][y] IS NULL THEN
                    raise notice 'matrix[%][%] is NULL', x, y;
                    IF nodatamode = 'ignore' THEN
                        matrix[x][y] := 0;
                    ELSE
                        matrix[x][y] := nodatamode::float;
                    END IF;
                END IF;
                sum := sum + matrix[x][y];
            END LOOP;
        END LOOP;
        RETURN sum;
    END;
    $$
    LANGUAGE 'plpgsql';

-- Tests
-- Test NULL Raster. Should be true.
--SELECT ST_MapAlgebraFctNgb(NULL, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL) IS NULL FROM ST_TestRaster(0, 0, -1) rast;

-- Test empty Raster. Should be true.
--SELECT ST_IsEmpty(ST_MapAlgebraFctNgb(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, -1), 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL));

-- Test has no band raster. Should be true
--SELECT ST_HasNoBand(ST_MapAlgebraFctNgb(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, -1), 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL));

-- Test has no nodata value. Should return null and 7.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(
--      ST_SetBandNoDataValue(rast, NULL), 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL
--    ), 2, 2) = 7
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 2, 2, NULL) AS rast;
--
-- Test NULL nodatamode. Should return null and null.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  ) IS NULL
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 2, 2, NULL) AS rast;
--
-- Test ignore nodatamode. Should return null and 8.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'ignore', NULL), 2, 2
--  ) = 8
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 2, 2, NULL) AS rast;
--
-- Test value nodatamode. Should return null and null.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'value', NULL), 2, 2
--  ) IS NULL 
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 2, 2, NULL) AS rast;
--
-- Test value nodatamode. Should return null and 9.
--SELECT 
--  ST_Value(rast, 1, 1) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'value', NULL), 2, 2
--  ) = 9
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 1, 1, NULL) AS rast;
--
-- Test value nodatamode. Should return null and 0.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', '-8', NULL), 2, 2
--  ) = 0
-- FROM ST_SetValue(ST_TestRaster(3, 3, 1), 2, 2, NULL) AS rast;
--
-- Test ST_Sum user function. Should be 1 and 9.
--SELECT 
--  ST_Value(rast, 2, 2) = 1, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  ) = 9
-- FROM ST_TestRaster(3, 3, 1) AS rast;
--
-- Test ST_Sum user function on a no nodata value raster. Should be null and -1.
--SELECT 
--  ST_Value(rast, 2, 2) IS NULL, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(ST_SetBandNoDataValue(rast, NULL), 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  ) = -1
-- FROM ST_SetValue(ST_TestRaster(3, 3, 0), 2, 2, NULL) AS rast;
--
-- Test pixeltype 1. Should return 2 and 15.
--SELECT
--  ST_Value(rast, 2, 2) = 2,
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, '4BUI', 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  ) = 15
-- FROM ST_SetBandNoDataValue(ST_TestRaster(3, 3, 2), 1, NULL) AS rast;
--
-- Test pixeltype 1. Should return an error.
--SELECT 
--  ST_Value(rast, 2, 2), 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, '4BUId', 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  )
-- FROM ST_TestRaster(3, 3, 2) AS rast;
--
-- Test pixeltype 1. Should return 1 and 3.
--SELECT 
--  ST_Value(rast, 2, 2) = 1, 
--  ST_Value(
--    ST_MapAlgebraFctNgb(rast, 1, '2BUI', 1, 1, 'ST_Sum', 'NULL', NULL), 2, 2
--  ) = 3
-- FROM ST_TestRaster(3, 3, 1) AS rast;
--
-- Test that the neighborhood function leaves a border of NODATA
--SELECT
--  COUNT(*) = 1
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL)
--    )).*
--   FROM ST_TestRaster(5, 5, 1) AS rast) AS foo
-- WHERE ST_Area(geom) = 9;
--
-- Test that the neighborhood function leaves a border of NODATA
--SELECT
--  ST_Area(geom) = 8, val = 9
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL)
--    )).*
--   FROM ST_SetValue(ST_TestRaster(5, 5, 1), 1, 1, NULL) AS rast) AS foo;
--
-- Test that the neighborhood function leaves a border of NODATA
-- plus a corner where one cell has a value of 8.
--SELECT
--  (ST_Area(geom) = 1 AND val = 8) OR (ST_Area(geom) = 8 AND val = 9)
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'ignore', NULL)
--    )).*
--   FROM ST_SetValue(ST_TestRaster(5, 5, 1), 1, 1, NULL) AS rast) AS foo;
--
-- Test that the neighborhood function leaves a border of NODATA
-- plus a hole where 9 cells have NODATA
-- This results in a donut: a polygon with a hole. The polygon has
-- an area of 16, with a hole that has an area of 9
--SELECT
--  ST_NRings(geom) = 2,
--  ST_NumInteriorRings(geom) = 1,
--  ST_Area(geom) = 16, 
--  val = 9, 
--  ST_Area(ST_BuildArea(ST_InteriorRingN(geom, 1))) = 9
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL)
--    )).*
--   FROM ST_SetValue(ST_TestRaster(7, 7, 1), 4, 4, NULL) AS rast) AS foo;
--
-- Test that the neighborhood function leaves a border of NODATA,
-- and the center pyramids when summed twice, ignoring NODATA values
--SELECT
--  COUNT(*) = 9, SUM(ST_Area(geom)) = 9, SUM(val) = ((36+54+36) + (54+81+54) + (36+54+36)) 
--  --ST_AsText(geom), ST_Area(geom), val
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(
--        ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'ignore', NULL), 1, NULL, 1, 1, 'ST_Sum', 'ignore', NULL
--      )
--    )).*
--   FROM ST_SetBandNoDataValue(ST_TestRaster(5, 5, 1), NULL) AS rast) AS foo;
--
-- Test that the neighborhood function leaves a border of NODATA,
-- and the center contains one cel when summed twice, replacing NULL with NODATA values
--SELECT
--  COUNT(*) = 1, SUM(ST_Area(geom)) = 1, SUM(val) = 81
--  --ST_AsText(geom), ST_Area(geom), val
-- FROM (SELECT
--    (ST_DumpAsPolygons(
--      ST_MapAlgebraFctNgb(
--        ST_MapAlgebraFctNgb(rast, 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL), 1, NULL, 1, 1, 'ST_Sum', 'NULL', NULL
--      )
--    )).*
--   FROM ST_SetBandNoDataValue(ST_TestRaster(5, 5, 1), NULL) AS rast) AS foo;
--
