----------------------------------------------------------------------
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------
-- NOTE: The ST_MapAlgebra() function is already implemented in C. This plpgsql script is provided only as an example. 
-- Defining the plpgsql function below might overwrite the current C implementation and brake other functions dependent on it.
-- Use with caution.
----------------------------------------------------------------------

--------------------------------------------------------------------
-- ST_MapAlgebra - (one raster version) Return a raster which values 
--                 are the result of an SQL expression involving pixel 
--                 value from the input raster band.
-- Arguments 
-- rast raster -  Raster on which the expression is evaluated. (Referred 
--                by "rast1" in the expression. 
-- band integer - Band number of the raster to be evaluated. Default to 1.
-- expression text - SQL expression to apply to with value pixels. Ex.: "rast + 2"
-- nodatavalueexpr text - SQL expression to apply to nodata value pixels. Ex.: "2"
-- pixeltype text - Pixeltype assigned to the resulting raster. Expression
--                  results are truncated to this type. Default to the 
--                  pixeltype of the first raster.
--
-- NOTE that this function now exist as a C implementation and is provided 
-- here solely as a plpgsql example.
--------------------------------------------------------------------
DROP FUNCTION IF EXISTS ST_MapAlgebra(rast raster, band integer, expression text, nodatavalueexpr text, pixeltype text);
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast raster, band integer, expression text, nodatavalueexpr text, pixeltype text) 
    RETURNS raster AS 
    $$
    DECLARE
        width integer;
        height integer;
        x integer;
        y integer;
        r float;
        newnodatavalue float8;
        newinitialvalue float8;
        newrast raster;
        newval float8;
        newexpr text;
        initexpr text;
        initndvexpr text;
        newpixeltype text;
        skipcomputation int := 0;
        newhasnodatavalue boolean := FALSE;
    BEGIN
        -- Check if raster is NULL
        IF rast IS NULL THEN
            RAISE NOTICE 'ST_MapAlgebra: Raster is NULL. Returning NULL';
            RETURN NULL;
        END IF;

        width := ST_Width(rast);
        height := ST_Height(rast);

        -- Create a new empty raster having the same georeference as the provided raster
        newrast := ST_MakeEmptyRaster(width, height, ST_UpperLeftX(rast), ST_UpperLeftY(rast), ST_ScaleX(rast), ST_ScaleY(rast), ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast));

        -- If this new raster is empty (width = 0 OR height = 0) then there is nothing to compute and we return it right now
        IF ST_IsEmpty(newrast) THEN 
            RAISE NOTICE 'ST_MapAlgebra: Raster is empty. Returning an empty raster';
            RETURN newrast;
        END IF;
        
        -- Check if rast has the required band. Otherwise return a raster without band
        IF ST_HasNoBand(rast, band) THEN 
            RAISE NOTICE 'ST_MapAlgebra: Raster do not have the required band. Returning a raster without band';
            RETURN newrast;
        END IF;
        
        -- Set the new pixeltype
        newpixeltype := pixeltype;
        IF newpixeltype IS NULL THEN
            newpixeltype := ST_BandPixelType(rast, band);
        ELSIF newpixeltype != '1BB' AND newpixeltype != '2BUI' AND newpixeltype != '4BUI' AND newpixeltype != '8BSI' AND newpixeltype != '8BUI' AND 
               newpixeltype != '16BSI' AND newpixeltype != '16BUI' AND newpixeltype != '32BSI' AND newpixeltype != '32BUI' AND newpixeltype != '32BF' AND newpixeltype != '64BF' THEN
            RAISE EXCEPTION 'ST_MapAlgebra: Invalid pixeltype "%". Aborting.', newpixeltype;
        END IF;

        -- Check for notada value
        newnodatavalue := ST_BandNodataValue(rast, band);
        IF newnodatavalue IS NULL OR newnodatavalue < ST_MinPossibleValue(newpixeltype) OR newnodatavalue > (-ST_MinPossibleValue(newpixeltype) - 1) THEN
            RAISE NOTICE 'ST_MapAlgebra: Source raster do not have a nodata value or is out of range for the new raster pixeltype, nodata value for new raster set to the min value possible';
            newnodatavalue := ST_MinPossibleValue(newpixeltype);
        END IF;
        -- We set the initial value of the future band to nodata value. 
        -- If nodatavalue is null then the raster will be initialise to ST_MinPossibleValue 
        -- but all the values should be recomputed anyway.
        newinitialvalue := newnodatavalue;

        initexpr := 'SELECT ' || trim(upper(expression));
        initndvexpr := 'SELECT ' || trim(upper(nodatavalueexpr));

--RAISE NOTICE '111 initexpr=%, newnodatavalue=%', initexpr,newnodatavalue;

        -- Optimization: If a nodatavalueexpr is provided, recompute the initial value 
        -- so we can then initialise the raster with this value and skip the computation
        -- of nodata values one by one in the main computing loop
        IF NOT nodatavalueexpr IS NULL THEN
            newexpr := replace(initndvexpr, 'RAST', newnodatavalue::text);
--RAISE NOTICE '222 newexpr=%', newexpr;
            EXECUTE newexpr INTO newinitialvalue;
        END IF;

--RAISE NOTICE '333';

        -- Optimization: If the raster is only filled with nodata value return right now a raster filled with the nodatavalueexpr
        IF ST_BandIsNoData(rast, band) THEN
            RETURN ST_AddBand(newrast, newpixeltype, newinitialvalue, newnodatavalue);
        END IF;
        
        -- Optimization: If expression resume to 'RAST' and nodatavalueexpr is NULL or also equal to 'RAST', 
        -- we can just return the band from the original raster
        IF initexpr = 'SELECT RAST' AND (nodatavalueexpr IS NULL OR initndvexpr = 'SELECT RAST') THEN
            RETURN ST_AddBand(newrast, rast, band, 1);   -- To be implemented in C       
        END IF;
        
        -- Optimization: If expression resume to a constant (it does not contain RAST) 
        IF position('RAST' in initexpr) = 0 THEN
--RAISE NOTICE '444';
            EXECUTE initexpr INTO newval;
--RAISE NOTICE '555';
            skipcomputation := 1;
            
            IF nodatavalueexpr IS NULL THEN
                -- Compute the new value, set it and we will return after creating the new raster
                newinitialvalue := newval;
                skipcomputation := 2;
            ELSEIF newval = newinitialvalue THEN
                -- Return the new raster as it will be before computing pixel by pixel
                skipcomputation := 2;
            END IF;
        END IF;
        
        --Create the raster receiving all the computed values. Initialize it to the new initial value.
        newrast := ST_AddBand(newrast, newpixeltype, newinitialvalue, newnodatavalue);


        -- Optimization: If expression is NULL, or all the pixels could be set in a one step, return the initialised raster now
        IF expression IS NULL OR skipcomputation = 2 THEN
            RETURN newrast;
        END IF;   
        FOR x IN 1..width LOOP
            FOR y IN 1..height LOOP
                r := ST_Value(rast, band, x, y);
                -- We compute a value only for the withdata value pixel since the nodata value have already been set by the first optimization
                IF NOT r IS NULL AND (newnodatavalue IS NULL OR r != newnodatavalue) THEN -- The second part of the test can be removed once ST_Value return NULL for a nodata value
                    IF skipcomputation = 0 THEN
                        newexpr := replace(initexpr, 'RAST', r::text);
--RAISE NOTICE '666 newexpr=%', newexpr;
                        EXECUTE newexpr INTO newval;
                        IF newval IS NULL THEN
                            newval := newnodatavalue;
                        END IF;
                    END IF;
                    newrast = ST_SetValue(newrast, band, x, y, newval);
                ELSE
                    newhasnodatavalue := TRUE;
                END IF;
            END LOOP;
        END LOOP;
        IF newhasnodatavalue THEN
            newrast := ST_SetBandNodataValue(newrast, 1, newnodatavalue);
        END IF;
        RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';

--Test rasters
CREATE OR REPLACE FUNCTION ST_TestRaster(val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_SetValue(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 1, 1, 0, 0, -1), '32BF', val, -1), 1, 1, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

-- Tests
-- Test NULL Raster. Should be true.
--SELECT ST_MapAlgebra(NULL, 1, 'rast + 20', '2', NULL) IS NULL FROM ST_TestRaster(0, 0, -1) rast;

-- Test empty Raster. Should be true.
--SELECT ST_IsEmpty(ST_MapAlgebra(ST_MakeEmptyRaster(0, 10, 0, 0, 1, 1, 1, 1, -1), 1, 'rast + 20', '2', NULL))

-- Test has no band raster. Should be true.
--SELECT ST_HasNoBand(ST_MapAlgebra(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, 1, -1), 1, 'rast + 20', '2', NULL))

-- Test has no nodata value. Should return null and 19.
--SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(ST_SetBandNoDataValue(ST_TestRaster(0, 0, 1), NULL), 1, 'rast + 20', '2', NULL), 1, 1) 
--FROM ST_TestRaster(0, 0, 1) rast;

-- Test has nodata value. Should return null and 2.
--SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, 'rast + 20', 'rast + 3', NULL), 1, 1) 
--FROM ST_TestRaster(0, 0, 1) rast;

-- Test has nodata value. Should return null and null.
--SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(rast, 1, 'rast + 20', NULL, NULL), 1, 1) 
--FROM ST_TestRaster(0, 0, 1) rast;

-- Test 'rast' expression. Should be 1 and 1.
--SELECT ST_Value(rast, 1, 2), ST_Value(ST_MapAlgebra(rast, 1, 'rast', 'rast', NULL), 1, 2) 
--FROM ST_TestRaster(0, 0, 0) rast;

-- Test 'rast' expression on a no nodata value raster. Should be null and -1.
--SELECT ST_Value(rast, 1, 1), ST_Value(ST_MapAlgebra(ST_SetBandNoDataValue(rast, NULL), 1, 'rast', NULL, NULL), 1, 1) 
--FROM ST_TestRaster(0, 0, -1) rast;

-- Test pixeltype 1. Should return 100 and 15.
--SELECT ST_Value(rast, 1, 2), ST_Value(ST_MapAlgebra(rast, 1, 'rast + 20', 'rast + 2', '4BUI'), 1, 2) 
--FROM ST_TestRaster(0, 0, 100) rast;

-- Test pixeltype 1. Should return an error.
--SELECT ST_Value(rast, 1, 2), ST_Value(ST_MapAlgebra(rast, 1, 'rast + 20', 'rast + 2', '4BUId'), 1, 2) 
--FROM ST_TestRaster(0, 0, 100) rast;

-- Test pixeltype 1. Should return 101 and 3.
--SELECT ST_Value(rast, 1, 2), ST_Value(ST_MapAlgebra(rast, 1, 'rast + 20', 'rast + 2', '2BUI'), 1, 2) 
--FROM ST_TestRaster(0, 0, 101) rast;


--------------------------------------------------------------------
-- ST_SameAlignment
-- Determine if the raster coordinates are aligned. 
-- Scale must be the same and pixels corners must fall on the same grid.
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_SameAlignment(rast1ulx float8, 
                                            rast1uly float8, 
                                            rast1scalex float8, 
                                            rast1scaley float8, 
                                            rast1skewx float8, 
                                            rast1skewy float8, 
                                            rast2ulx float8, 
                                            rast2uly float8, 
                                            rast2scalex float8, 
                                            rast2scaley float8,
                                            rast2skewx float8, 
                                            rast2skewy float8) 
    RETURNS boolean AS 
    $$
    DECLARE
        emptyraster2 raster;
        r2x integer;
        r2y integer;
    BEGIN
        IF rast1scalex != rast2scalex THEN
            RAISE NOTICE 'ST_SameAlignment: Pixelsize in x are different';
            RETURN FALSE;
            END IF;
        IF rast1scaley != rast2scaley THEN
            RAISE NOTICE 'ST_SameAlignment: Pixelsize in y are different';
            RETURN FALSE;
        END IF;
         IF rast1skewx != rast2skewx THEN
            RAISE NOTICE 'ST_SameAlignment: Skews in x are different';
            RETURN FALSE;
        END IF;
        IF rast1skewy != rast2skewy THEN
            RAISE NOTICE 'ST_SameAlignment: Skews in y are different';
            RETURN FALSE;
        END IF;
        -- For checking for the pixel corner alignment, we create a fake raster aligned on the second raster...
        emptyraster2 := ST_MakeEmptyRaster(1, 1, rast2ulx, rast2uly, rast2scalex, rast2scaley, rast2skewx, rast2skewy, -1);
        -- We get the raster coordinates of the upper left corner of the first raster in this new raster..
        r2x := ST_World2RasterCoordX(emptyraster2, rast1ulx, rast1uly);
        r2y := ST_World2RasterCoordY(emptyraster2, rast1ulx, rast1uly);
        -- And we make sure the world coordinates the upperleft corner of this pixel are the same as the first raster.
        IF ST_Raster2WorldCoordX(emptyraster2, r2x, r2y) != rast1ulx OR ST_Raster2WorldCoordY(emptyraster2, r2x, r2y) != rast1uly THEN
            RAISE NOTICE 'ST_SameAlignment: alignments are different';
            RETURN FALSE;
        END IF;       
        RETURN TRUE;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION ST_SameAlignment(rast1 raster, rast2 raster)
    RETURNS boolean
    AS 'select ST_SameAlignment(ST_UpperLeftX($1), 
                                ST_UpperLeftY($1), 
                                ST_ScaleX($1), 
                                ST_ScaleY($1), 
                                ST_SkewX($1),
                                ST_SkewY($1),
                                ST_UpperLeftX($2), 
                                ST_UpperLeftY($2), 
                                ST_ScaleX($2), 
                                ST_ScaleY($2), 
                                ST_SkewX($2),
                                ST_SkewY($2))'
    LANGUAGE 'SQL' IMMUTABLE STRICT;

--Test

--SELECT ST_SameAlignment(0, 0, 1, 1, 0, 0, 0, 0, 1, 1, 0, 0) -- FALSE
--SELECT ST_SameAlignment(0.0001, 0, 1, 1, 0, 0, 1.00001, 0, 1, 1, 0, 0) -- FALSE
--SELECT ST_SameAlignment(10.0001, 2, 1, 1, 0, 0, 1.0001, 1110, 1, 1, 0, 0) --TRUE
--SELECT ST_SameAlignment(10.0001, 2, 1, 1, 0, 0,  1.001, 1110, 1, 1, 0, 0) -- FALSE
--SELECT ST_SameAlignment(10, 2, 1, 1, 1, -1, sqrt(2.0), sqrt(2.0),  1,  1, 1, -1) -- FALSE
--SELECT ST_SameAlignment(ST_AddBand(ST_MakeEmptyRaster(10, 10, 2, 0, 1, 1, 1, -1, -1), '8BSI'), ST_AddBand(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, 1, -1, -1), '8BSI'))
--SELECT ST_SameAlignment(ST_AddBand(ST_MakeEmptyRaster(10, 10, 2, 0, 1, 1, 1, -1, -1), '8BSI'), ST_AddBand(ST_MakeEmptyRaster(10, 10, 2, 6, 1, 1, 1, -1, -1), '8BSI'))

--SELECT ST_SameAlignment(ST_AddBand(ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, -0.2, -0.2, -1), '8BSI'), ST_AddBand(ST_MakeEmptyRaster(5, 5, 0.8, -1.2, 1, -1, -0.2, -0.2, -1), '8BSI'))
--SELECT ST_SameAlignment(ST_AddBand(ST_MakeEmptyRaster(10, 10, 0, 0, 1, -1, 0.2, 0.2, -1), '8BSI'), ST_AddBand(ST_MakeEmptyRaster(10, 10, 1.2, -0.8, 1, -1, 0.2, 0.2, -1), '8BSI'))

--SELECT generate_series(1, 100) AS ID, AsBinary((ST_PixelAsPolygons(ST_AddBand(ST_MakeEmptyRaster(10, 10, 0, 0, 1, 1, -0.4361, 0.4361, -1), '8BSI'), 1)).geom)

--------------------------------------------------------------------
-- ST_IsEmpty
-- Return TRUE if the raster is empty. i.e. is NULL, width = 0 or height = 0
--------------------------------------------------------------------
--CREATE OR REPLACE FUNCTION ST_IsEmpty(raster)
--    RETURNS boolean
--    AS 'SELECT $1 IS NULL OR ST_Width($1) = 0 OR ST_Height($1) = 0'
--    LANGUAGE 'SQL' IMMUTABLE;

--------------------------------------------------------------------
-- ST_HasNoBand
-- Return TRUE if the number of band is 0
--------------------------------------------------------------------
--CREATE OR REPLACE FUNCTION ST_HasNoBand(raster)
--    RETURNS boolean
--    AS 'SELECT $1 IS NULL OR ST_NumBands($1) = 0'
--    LANGUAGE 'SQL' IMMUTABLE;

--------------------------------------------------------------------
-- ST_HasNoBand
-- Return TRUE if the raster do not have a band of this number.
--------------------------------------------------------------------
--CREATE OR REPLACE FUNCTION ST_HasNoBand(raster, int)
--    RETURNS boolean
--    AS 'SELECT $1 IS NULL OR ST_NumBands($1) < $2'
--    LANGUAGE 'SQL' IMMUTABLE;

-- Test
--SELECT ST_HasNoband(NULL);

--------------------------------------------------------------------
-- ST_BandIsNoData
-- NOT YET IMPLEMENTED
-- Return TRUE if the raster band contains only nodata values.
--------------------------------------------------------------------
--CREATE OR REPLACE FUNCTION ST_BandIsNoData(raster, int)
--    RETURNS boolean
--    AS 'SELECT ST_HasNoBand($1, $2) AND ST_BandHasNodataValue($1, $2) AND false'
--    LANGUAGE 'SQL' IMMUTABLE;

--CREATE OR REPLACE FUNCTION toto()
--    RETURNS int
--    AS 'SELECT 10'
--    LANGUAGE 'SQL' IMMUTABLE STRICT;

DROP FUNCTION IF EXISTS ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text, 
                                         nodata1expr text, 
                                         nodata2expr text,
                                         nodatanodataaexpr text);
                                         
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text, 
                                         nodata1expr text, 
                                         nodata2expr text,
                                         nodatanodataexpr text) 
    RETURNS raster AS 
    $$
    DECLARE
        x integer;
        y integer;
        r1 float8;
        r2 float8;
        rast1ulx float8;
        rast1uly float8;
        rast1width int;
        rast1height int;
        rast1scalex float8;
        rast1scaley float8;
        rast1skewx float8;
        rast1skewy float8;
        rast1nodataval float8;
        rast1srid int;

        rast1offsetx int;
        rast1offsety int;

        rast2ulx float8;
        rast2uly float8;
        rast2width int;
        rast2height int;
        rast2scalex float8;
        rast2scaley float8;
        rast2skewx float8;
        rast2skewy float8;
        rast2nodataval float8;
        rast2srid int;
        
        rast2offsetx1 int;
        rast2offsety1 int;
        rast2offsetx2 int;
        rast2offsety2 int;
        
        newrast raster;
        newsrid int;
        
        newscalex float8;
        newscaley float8;
        newskewx float8;
        newskewy float8;
        newnodatavalue float8;
        newpixeltype text;
        newulx float8;
        newuly float8;
        newwidth int;
        newheight int;
        newoffsetx1 int;
        newoffsety1 int;
        newoffsetx2 int;
        newoffsety2 int;
        
        newval float8;
        newexpr text;
        
    BEGIN
        -- We have to deal with NULL, empty, hasnoband and hasnodatavalue band rasters... 
        -- These are respectively tested by "IS NULL", "ST_IsEmpty()", "ST_HasNoBand()" and "ST_BandIsNodata()"
        
        -- If both raster are null, we return NULL. ok
        -- If both raster do not have extent (are empty), we return an empty raster. ok
        -- If both raster do not have the specified band, 
        --     we return a no band raster with the correct extent (according to the extent expression). ok
        -- If both raster bands are nodatavalue and there is no replacement value, we return a nodata value band. ok
        
        -- If only one raster is null or empty or has no band or hasnodata band we treat it as a nodata band raster.
        -- If there is a replacement value we replace the missing raster values with this replacement value. ok
        -- If there is no replacement value, we return a nodata value band. ok
        
        -- What to do when only one raster is NULL or empty
        -- If the extent expression is FIRST and the first raster is null we return NULL. ok
        -- If the extent expression is FIRST and the first raster do not have extent (is empty), we return an empty raster. ok
        -- If the extent expression is SECOND and the second raster is null we return NULL. ok
        -- If the extent expression is SECOND and the second raster do not have extent (is empty), we return an empty raster. ok
        -- If the extent expression is INTERSECTION and one raster is null or do not have extent (is empty), we return an empty raster. ok
        -- If the extent expression is UNION and one raster is null or do not have extent (is empty), 
        --     we return a raster having the extent and the band characteristics of the other raster. ok

        -- What to do when only one raster do not have the required band.
        -- If the extent expression is FIRST and the first raster do not have the specified band, 
        --     we return a no band raster with the correct extent (according to the extent expression). ok
        -- If the extent expression is SECOND and the second raster do not have the specified band, 
        --     we return a no band raster with the correct extent (according to the extent expression). ok
        -- If the extent expression is INTERSECTION and one raster do not have the specified band, 
        --     we treat it as a nodata raster band. ok
        -- If the extent expression is UNION and one raster do not have the specified band, 
        --     we treat it as a nodata raster band. ok

        -- In all those cases, we make a warning.

        -- Check if both rasters are NULL
        IF rast1 IS NULL AND rast2 IS NULL THEN
            RAISE NOTICE 'ST_MapAlgebra: Both raster are NULL. Returning NULL';
            RETURN NULL;
        END IF;

        -- Check if both rasters are empty (width or height = 0)
        IF ST_IsEmpty(rast1) AND ST_IsEmpty(rast2) THEN
            RAISE NOTICE 'ST_MapAlgebra: Both raster are empty. Returning an empty raster';
            RETURN ST_MakeEmptyRaster(0, 0, 0, 0, 0, 0, 0, 0, -1);
        END IF;

        rast1ulx := ST_UpperLeftX(rast1);
        rast1uly := ST_UpperLeftY(rast1);
        rast1width := ST_Width(rast1);
        rast1height := ST_Height(rast1);
        rast1scalex := ST_ScaleX(rast1);
        rast1scaley := ST_ScaleY(rast1);
        rast1skewx := ST_SkewX(rast1);
        rast1skewy := ST_SkewY(rast1);
        rast1srid := ST_SRID(rast1);

        rast2ulx := ST_UpperLeftX(rast2);
        rast2uly := ST_UpperLeftY(rast2);
        rast2width := ST_Width(rast2);
        rast2height := ST_Height(rast2);
        rast2scalex := ST_ScaleX(rast2);
        rast2scaley := ST_ScaleY(rast2);
        rast2skewx := ST_SkewX(rast2);
        rast2skewy := ST_SkewY(rast2);
        rast2srid := ST_SRID(rast2);

        -- Check if the first raster is NULL or empty
        IF rast1 IS NULL OR ST_IsEmpty(rast1)  THEN
            rast1ulx := rast2ulx;
            rast1uly := rast2uly;
            rast1width := rast2width;
            rast1height := rast2height;
            rast1scalex := rast2scalex;
            rast1scaley := rast2scaley;
            rast1skewx := rast2skewx;
            rast1skewy := rast2skewy;
            rast1srid := rast2srid;
        END IF;
        -- Check if the second raster is NULL or empty
        IF rast2 IS NULL OR ST_IsEmpty(rast2)  THEN
            rast2ulx := rast1ulx;
            rast2uly := rast1uly;
            rast2width := rast1width;
            rast2height := rast1height;
            rast2scalex := rast1scalex;
            rast2scaley := rast1scaley;
            rast2skewx := rast1skewx;
            rast2skewy := rast1skewy;
            rast2srid := rast1srid;
        END IF;

        -- Check for SRID
        IF rast1srid != rast2srid THEN
            RAISE EXCEPTION 'ST_MapAlgebra: Provided raster with different SRID. Aborting';
        END IF;
        newsrid := rast1srid;

        -- Check for alignment. (Rotation problem here)
        IF NOT ST_SameAlignment(rast1ulx, rast1uly, rast1scalex, rast1scaley, rast1skewx, rast1skewy, rast2ulx, rast2uly, rast2scalex, rast2scaley, rast2skewx, rast2skewy) THEN
            -- For now print an error message, but a more robust implementation should resample the second raster to the alignment of the first raster.
            RAISE EXCEPTION 'ST_MapAlgebra: Provided raster do not have the same alignment. Aborting';
        END IF;

        -- Set new pixel size and skew. We set it to the rast1 scale and skew 
        -- since both rasters are aligned and thus have the same scale and skew
        newscalex := rast1scalex; 
        newscaley := rast1scaley;
        newskewx := rast1skewx;
        newskewy := rast1skewy;

        --rastoffset is the offset of a raster in relatively to the first raster
        rast2offsetx1 := -st_world2rastercoordx(rast2, rast1ulx, rast1uly) + 1;
        rast2offsety1 := -st_world2rastercoordy(rast2, rast1ulx, rast1uly) + 1;
        rast2offsetx2 := rast2offsetx1 + rast2width - 1;
        rast2offsety2 := rast2offsety1 + rast2height - 1;

        -- Compute x and y relative index of master and slave according to the extent expression (FIRST, SECOND, INTERSECTION or UNION)
        IF extentexpr IS NULL OR upper(extentexpr) = 'FIRST' THEN

            -- Check if rast1 is NULL
            IF rast1 IS NULL  THEN 
                RAISE NOTICE 'ST_MapAlgebra: FIRST raster is NULL. Returning NULL';
                RETURN NULL;
            END IF;
            
            -- Check if rast1 is empty
            IF ST_IsEmpty(rast1) THEN 
                RAISE NOTICE 'ST_MapAlgebra: FIRST raster is empty. Returning an empty raster';
                RETURN ST_MakeEmptyRaster(0, 0, 0, 0, 0, 0, 0, 0, newsrid);
            END IF;
                        
            -- Check if rast1 has the required band
            IF ST_HasNoBand(rast1, band1) THEN 
                RAISE NOTICE 'ST_MapAlgebra: FIRST raster has no band. Returning a raster without band';
                RETURN ST_MakeEmptyRaster(rast1width, rast1height, rast1ulx, rast1uly, rast1scalex, rast1scaley, rast1skewx, rast1skewy, rast1srid);
            END IF;
            
            newulx := rast1ulx;
            newuly := rast1uly;
            newwidth := rast1width;
            newheight := rast1height;
            rast1offsetx := 0;
            rast1offsety := 0;

        ELSIF upper(extentexpr) = 'SECOND' THEN

            -- Check if rast2 is NULL
            IF rast2 IS NULL  THEN 
                RAISE NOTICE 'ST_MapAlgebra: SECOND raster is NULL. Returning NULL';
                RETURN NULL;
            END IF;
            
            -- Check if rast2 is empty
            IF ST_IsEmpty(rast2) THEN 
                RAISE NOTICE 'ST_MapAlgebra: SECOND raster is empty. Returning an empty raster';
                RETURN ST_MakeEmptyRaster(0, 0, 0, 0, 0, 0, 0, 0, newsrid);
            END IF;
            
            -- Check if rast2 has the required band
            IF ST_HasNoBand(rast2, band2) THEN 
                RAISE NOTICE 'ST_MapAlgebra: SECOND raster has no band. Returning an empty raster';
                RETURN ST_MakeEmptyRaster(rast2width, rast2height, rast2ulx, rast2uly, rast2scalex, rast2scaley, rast2skewx, rast2skewy, rast2srid);
            END IF;

            newulx := rast2ulx;
            newuly := rast2uly;
            newwidth := rast2width;
            newheight := rast2height;
            rast1offsetx := -rast2offsetx1;
            rast1offsety := -rast2offsety1;
            rast2offsetx1 := 0;
            rast2offsety1 := 0;

        ELSIF upper(extentexpr) = 'INTERSECTION' THEN

            -- Check if the intersection is empty.
            IF rast2offsetx2 < 0 OR rast2offsetx1 > (rast1width - 1) OR 
               rast2offsety2 < 0 OR rast2offsety1 > (rast1height - 1) OR
               rast1 IS NULL OR ST_IsEmpty(rast1) OR
               rast2 IS NULL OR ST_IsEmpty(rast2) THEN 
                RAISE NOTICE 'ST_MapAlgebra: INTERSECTION of provided rasters is empty. Returning an empty raster';
                RETURN ST_MakeEmptyRaster(0, 0, 0, 0, 0, 0, 0, 0, newsrid);
            END IF;
            
            --Determine the new raster upper left x offset
            newoffsetx1 := 0;
            IF rast2offsetx1 > 0 THEN
                newoffsetx1 := rast2offsetx1;
            END IF;
            --Determine the new raster upper left y offset
            newoffsety1 := 0;
            IF rast2offsety1 > 0 THEN
                newoffsety1 := rast2offsety1;
            END IF;
            --Determine the new raster lower right x offset
            newoffsetx2 := rast1width - 1;
            IF rast2offsetx2 < rast1width THEN
                newoffsetx2 := rast2offsetx2;
            END IF;
            --Determine the new raster lower right y offset
            newoffsety2 := rast1height - 1;
            IF rast2offsety2 < rast1height THEN
                newoffsety2 := rast2offsety2;
            END IF;

            -- Compute the new ulx and uly
            newulx := st_raster2worldcoordx(rast1, newoffsetx1 + 1, newoffsety1 + 1);
            newuly := st_raster2worldcoordy(rast1, newoffsetx1 + 1, newoffsety1 + 1);
            newwidth := newoffsetx2 - newoffsetx1 + 1;
            newheight := newoffsety2 - newoffsety1 + 1;

            -- Compute the master offsets
            rast1offsetx := -st_world2rastercoordx(rast1, newulx, newuly) + 1;
            rast1offsety := -st_world2rastercoordy(rast1, newulx, newuly) + 1;

            -- Recompute the slave offsets
            rast2offsetx1 := -st_world2rastercoordx(rast2, newulx, newuly) + 1;
            rast2offsety1 := -st_world2rastercoordy(rast2, newulx, newuly) + 1;

        ELSIF upper(extentexpr) = 'UNION' THEN

            IF rast1 IS NULL OR ST_IsEmpty(rast1) THEN
                newulx := rast2ulx;
                newuly := rast2uly;
                newwidth := rast2width;
                newheight := rast2height;
                rast1offsetx := 0;
                rast1offsety := 0;
                rast2offsetx1 := 0;
                rast2offsety1 := 0;
            ELSIF rast2 IS NULL OR ST_IsEmpty(rast2) THEN
                newulx := rast1ulx;
                newuly := rast1uly;
                newwidth := rast1width;
                newheight := rast1height;
                rast1offsetx := 0;
                rast1offsety := 0;
                rast2offsetx1 := 0;
                rast2offsety1 := 0;
            ELSE
                --Determine the new raster upper left x offset
                newoffsetx1 := 0;
                IF rast2offsetx1 < 0 THEN
                    newoffsetx1 := rast2offsetx1;
                END IF;
                --Determine the new raster upper left y offset
                newoffsety1 := 0;
                IF rast2offsety1 < 0 THEN
                    newoffsety1 := rast2offsety1;
                END IF;
                --Determine the new raster lower right x offset
                newoffsetx2 := rast1width - 1;
                IF rast2offsetx2 >= rast1width THEN
                    newoffsetx2 := rast2offsetx2;
                END IF;
                --Determine the new raster lower right y offset
                newoffsety2 := rast1height - 1;
                IF rast2offsety2 >= rast1height THEN
                    newoffsety2 := rast2offsety2;
                END IF;

                -- Compute the new ulx and uly
                newulx := st_raster2worldcoordx(rast1, newoffsetx1 + 1, newoffsety1 + 1);
                newuly := st_raster2worldcoordy(rast1, newoffsetx1 + 1, newoffsety1 + 1);
                newwidth := newoffsetx2 - newoffsetx1 + 1;
                newheight := newoffsety2 - newoffsety1 + 1;

                -- Compute the first raster offsets
                rast1offsetx := -st_world2rastercoordx(rast1, newulx, newuly) + 1;
                rast1offsety := -st_world2rastercoordy(rast1, newulx, newuly) + 1;

                -- Recompute the second raster offsets
                rast2offsetx1 := -st_world2rastercoordx(rast2, newulx, newuly) + 1;
                rast2offsety1 := -st_world2rastercoordy(rast2, newulx, newuly) + 1;
            END IF;
        ELSE
            RAISE EXCEPTION 'ST_MapAlgebra: Unhandled extent expression "%". Only MASTER, INTERSECTION and UNION are accepted. Aborting.', upper(extentexpr);
        END IF;

        -- Check if both rasters do not have the specified band.
        IF ST_HasNoband(rast1, band1) AND ST_HasNoband(rast2, band2) THEN
            RAISE NOTICE 'ST_MapAlgebra: Both raster do not have the specified band. Returning a no band raster with the correct extent';
            RETURN ST_MakeEmptyRaster(newwidth, newheight, newulx, newuly, newscalex, newscaley, newskewx, newskewy, newsrid);
        END IF;
        
        -- Check newpixeltype
        newpixeltype := pixeltype;
        IF newpixeltype NOTNULL AND newpixeltype != '1BB' AND newpixeltype != '2BUI' AND newpixeltype != '4BUI' AND newpixeltype != '8BSI' AND newpixeltype != '8BUI' AND 
               newpixeltype != '16BSI' AND newpixeltype != '16BUI' AND newpixeltype != '32BSI' AND newpixeltype != '32BUI' AND newpixeltype != '32BF' AND newpixeltype != '64BF' THEN
            RAISE EXCEPTION 'ST_MapAlgebra: Invalid pixeltype "%". Aborting.', newpixeltype;
        END IF;
        
        -- If no newpixeltype was provided, get it from the provided rasters.
        IF newpixeltype IS NULL THEN
            IF (upper(extentexpr) = 'SECOND' AND NOT ST_HasNoBand(rast2, band2)) OR ST_HasNoBand(rast1, band1) THEN
                newpixeltype := ST_BandPixelType(rast2, band2);
            ELSE
                newpixeltype := ST_BandPixelType(rast1, band1);
            END IF;
        END IF;
               
         -- Get the nodata value for first raster
        IF NOT ST_HasNoBand(rast1, band1) AND NOT ST_BandNodataValue(rast1, band1) IS NULL THEN
            rast1nodataval := ST_BandNodatavalue(rast1, band1);
        ELSE
            rast1nodataval := NULL;
        END IF;
         -- Get the nodata value for second raster
        IF NOT ST_HasNoBand(rast2, band2) AND NOT ST_BandNodatavalue(rast2, band2) IS NULL THEN
            rast2nodataval := ST_BandNodatavalue(rast2, band2);
        ELSE
            rast2nodataval := NULL;
        END IF;
        
        -- Determine new notadavalue
        IF (upper(extentexpr) = 'SECOND' AND NOT rast2nodataval IS NULL) THEN
            newnodatavalue := rast2nodataval;
        ELSEIF NOT rast1nodataval IS NULL THEN
            newnodatavalue := rast1nodataval;
        ELSEIF NOT rast2nodataval IS NULL THEN
            newnodatavalue := rast2nodataval;
        ELSE
            RAISE NOTICE 'ST_MapAlgebra: Both source rasters do not have a nodata value, nodata value for new raster set to the minimum value possible';
            newnodatavalue := ST_MinPossibleValue(newrast);
        END IF;
        
        -------------------------------------------------------------------
        --Create the raster receiving all the computed values. Initialize it to the new nodatavalue.
        newrast := ST_AddBand(ST_MakeEmptyRaster(newwidth, newheight, newulx, newuly, newscalex, newscaley, newskewx, newskewy, newsrid), newpixeltype, newnodatavalue, newnodatavalue);
        -------------------------------------------------------------------

        -- If one of the two raster is NULL, empty, do not have the requested band or is only nodata values 
        -- and there is no replacement value for those missing values 
        -- and this raster IS involved in the expression 
        -- return NOW with the nodata band raster.
        --IF (rast1 IS NULL OR ST_IsEmpty(rast1) OR ST_HasNoBand(rast1, band1) OR ST_BandIsNoData(rast1, band1)) AND nodatavalrepl IS NULL AND position('RAST1' in upper(expression)) != 0 THEN
        --    RETURN newrast;
        --END IF;
        --IF (rast2 IS NULL OR ST_IsEmpty(rast2) OR ST_HasNoBand(rast2, band2) OR ST_BandIsNoData(rast2, band2)) AND nodatavalrepl IS NULL AND position('RAST2' in upper(expression)) != 0 THEN
        --    RETURN newrast;
        --END IF;
                
        -- There is place for optimization here when doing a UNION we don't want to iterate over the empty space.
        FOR x IN 1..newwidth LOOP
            FOR y IN 1..newheight LOOP
                r1 := ST_Value(rast1, band1, x - rast1offsetx, y - rast1offsety);
                r2 := ST_Value(rast2, band2, x - rast2offsetx1, y - rast2offsety1);
                
                -- Check if both values are outside the extent or nodata values
                IF (r1 IS NULL OR r1 = rast1nodataval) AND (r2 IS NULL OR r2 = rast1nodataval) THEN
                    IF nodatanodataexpr IS NULL THEN
                        newval := newnodatavalue;
                    ELSE
                        EXECUTE 'SELECT ' || nodatanodataexpr INTO newval;
                    END IF;
                ELSIF r1 IS NULL OR r1 = rast1nodataval THEN
                    IF nodata1expr IS NULL THEN
                        newval := newnodatavalue;
                    ELSE
                        newexpr := replace('SELECT ' || upper(nodata1expr), 'RAST2'::text, r2::text);
                        EXECUTE newexpr INTO newval;
                    END IF;

                ELSIF r2 IS NULL OR r2 = rast2nodataval THEN
                    IF nodata2expr IS NULL THEN
                        newval := newnodatavalue;
                    ELSE
                        newexpr := replace('SELECT ' || upper(nodata2expr), 'RAST1'::text, r1::text);
                        EXECUTE newexpr INTO newval;
                    END IF;
                ELSE
                    newexpr := replace('SELECT ' || upper(expression), 'RAST1'::text, r1::text);
                    newexpr := replace(newexpr, 'RAST2'::text, r2::text);
--RAISE NOTICE '222 - %',  newexpr;
                    EXECUTE newexpr INTO newval;
--RAISE NOTICE '333 - %',  newval;
                END IF; 
                IF newval IS NULL THEN
                    newval := newnodatavalue;
                END IF;
                newrast = ST_SetValue(newrast, 1, x, y, newval);
            END LOOP;
        END LOOP;
        RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';


--------------------------------------------------------------------
-- ST_MapAlgebra (two raster version) variants 
--------------------------------------------------------------------

-- Variant 5
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         rast2 raster, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text, 
                                         nodata1expr text,
                                         nodata2expr text,
                                         nodatanodataexpr text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, 1, $2, 1, $3, $4, $5, $6, $7, $8)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant 6
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         rast2 raster, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, 1, $2, 1, $3, $4, $5, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant 7
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         rast2 raster, 
                                         expression text, 
                                         pixeltype text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, 1, $2, 1, $3, $4, NULL, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE;

    
-- Variant 8
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         rast2 raster, 
                                         expression text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, 1, $2, 1, $3, NULL, NULL, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE STRICT;
    
-- Variant 10
--DROP FUNCTION ST_MapAlgebra(rast1 raster, band1 integer, rast2 raster, band2 integer, expression text, pixeltype text, extentexpr text, rastnodatavalrepl float8);                                        
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text, 
                                         nodataexpr text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, $2, $3, $4, $5, $6, $7, $8, $8, $8)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant 11
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text, 
                                         pixeltype text, 
                                         extentexpr text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, $2, $3, $4, $5, $6, $7, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant 12
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text, 
                                         pixeltype text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant 13
CREATE OR REPLACE FUNCTION ST_MapAlgebra(rast1 raster, 
                                         band1 integer, 
                                         rast2 raster, 
                                         band2 integer, 
                                         expression text)
    RETURNS raster
    AS 'SELECT ST_MapAlgebra($1, $2, $3, $4, $5, NULL, NULL, NULL, NULL, NULL)'
    LANGUAGE 'SQL' IMMUTABLE;
    
--test MapAlgebra with NULL
--SELECT ST_MapAlgebra(NULL, 1, ST_TestRaster(2, 0), 1, 'rast2', NULL, 'UNION', 0);
--SELECT AsBinary((rast).geom), (rast).val FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(NULL, 1, ST_TestRaster(2, 0), 1, 'rast2', NULL, 'UNION', 0), 1) rast) foo

--SELECT ST_MapAlgebra(NULL, 1, NULL, 1, 'rast2', NULL, 'UNION', 0);


--Test rasters
CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(3, 3, ulx, uly, 1, 1, 0, 0, -1), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION ST_TestRotatedRaster(ulx float8, uly float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(20, 20, ulx, uly, 1, -1, 0.2, 0.2, -1), '32BSI', 1, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

-- Overlay operations on two raster implemented with the two raster version of ST_MapAlgebra
-- Note that every time an expression resume to 'rast1' or 'rast2' you can optimize the value setting memcpy the pixel values.

-- First display the two test rasters used in the following examples in OpenJump
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(0, 0, 1)) gv) foo

--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(1, 1, 3)) gv) foo


-- 1) ST_Intersection and ST_Clip
--SELECT ST_MapAlgebra(rast1, rast2, 'rast1', '32BF', 'INTERSECTION')
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'rast1', '32BF', 'INTERSECTION')) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 2) ST_Intersection returning a two band raster
--SELECT ST_AddBand(ST_MapAlgebra(rast1, rast2, 'rast1', '32BF', 'INTERSECTION'), ST_MapAlgebra(rast1, rast2, 'rast2', '32BF', 'INTERSECTION'))
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display band 1 in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_AddBand(ST_MapAlgebra(rast1, rast2, 'rast1', '32BF', 'INTERSECTION'), ST_MapAlgebra(rast1, rast2, 'rast2', '32BF', 'INTERSECTION'))) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2

-- Display band 2 in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_AddBand(ST_MapAlgebra(rast1, rast2, 'rast1', '32BF', 'INTERSECTION'), ST_MapAlgebra(rast1, rast2, 'rast2', '32BF', 'INTERSECTION')), 2) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 3) ST_Union
--SELECT ST_MapAlgebra(rast1, rast2, '(rast1 + rast2)/2::numeric', '32BF', 'UNION', 'rast2', 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, '(rast1 + rast2)/2::numeric', '32BF', 'UNION', 'rast2', 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 4) ST_Collect
--SELECT ST_MapAlgebra(rast1, rast2, 'rast2', '32BF', 'UNION', 'rast2', 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'rast2', '32BF', 'UNION', 'rast2', 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 5) ST_Difference making a mere geometric difference
--SELECT ST_MapAlgebra(rast1, rast2, 'CASE WHEN NOT rast2 IS NULL THEN NULL ELSE rast1 END', '32BF', 'FIRST', NULL, 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'CASE WHEN NOT rast2 IS NULL THEN NULL ELSE rast1 END', '32BF', 'FIRST', NULL, 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 6) ST_Difference making an arithmetic difference
--SELECT ST_MapAlgebra(rast1, rast2, 'rast1 - rast2', '32BF', 'FIRST', NULL, 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'rast1 - rast2', '32BF', 'FIRST', NULL, 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2


-- 7) ST_SymDifference making a mere geometric difference (commutative)
--SELECT ST_MapAlgebra(rast1, rast2, 'NULL', '32BF', 'UNION', 'rast2', 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo

-- Display first commutation in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'NULL', '32BF', 'UNION', 'rast2', 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 3) rast2) foo) foo2

-- Display second commutation in OpenJump.
--SELECT (gv).val, ST_AsBinary((gv).geom)
--FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(rast1, rast2, 'NULL', '32BF', 'UNION', 'rast2', 'rast1', NULL)) gv
--      FROM (SELECT ST_TestRaster(1, 1, 3) rast1, ST_TestRaster(0, 0, 1) rast2) foo) foo2


-- 8) ST_SymDifference making an arithmetic difference (not commutative)
--Commutation 1
--SELECT ST_MapAlgebra(rast1, rast2, 'rast1 - rast2', '32BF', 'UNION', 'rast2', 'rast1', NULL)
--FROM (SELECT ST_TestRaster(0, 0, 1) rast1, ST_TestRaster(1, 1, 2) rast2) foo

--Commutation 2
--SELECT ST_MapAlgebra(rast1, rast2, 'rast1 - rast2', '32BF', 'UNION', 'rast2', 'rast1', NULL)
--FROM (SELECT ST_TestRaster(1, 1, 2) rast1, ST_TestRaster(0, 0, 1) rast2) foo


-- Other tests

-- UNION
-- SELECT ST_Value(rast, 1, 1) AS "1, 1", 
--        ST_Value(rast, 2, 1) AS "2, 1", 
--        ST_Value(rast, 3, 1) AS "3, 1", 
--        ST_Value(rast, 1, 2) AS "1, 2", 
--        ST_Value(rast, 2, 2) AS "2, 2", 
--        ST_Value(rast, 3, 2) AS "3, 2",
--        ST_BandPixelType(rast, 1), 
--        ST_Width(rast), 
--        ST_Height(rast)
-- FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0, 1), 1, ST_TestRaster(1, 0, 1), 1, 'rast1 + rast2 + 2*rast2'::text, '8BSI'::text, 'Union'::text, '0'::text) AS rast) foo

-- INTERSECTION
--SELECT ST_IsEmpty(rast),
--       ST_Value(rast, 1, 1) AS "1, 1", 
--       ST_Value(rast, 1, 2) AS "1, 2", 
--       ST_Value(rast, 2, 1) AS "2, 1", 
--      ST_Value(rast, 2, 2) AS "2, 2", 
--       ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0, 1), 1, ST_TestRaster(1, 0, 1), 1, '(rast1 + rast2)/3::float8', '64BF', 'INTERSECTION', '0'::text) AS rast) foo


-- FIRST -- Doesn't work...
--SELECT ST_Value(rast, 1, 1), 
--       ST_Value(rast, 1, 2), 
--       ST_Value(rast, 2, 1), 
--       ST_Value(rast, 2, 2), 
--       ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0, 1), 1, ST_TestRaster(1, 1, 1), 1, 'rast1 + rast2 + 2*rast2 + toto()', '8BSI', 'FIRST', NULL) AS rast) foo

-- SECOND -- Doesn't work...
--SELECT ST_Value(rast, 1, 1), 
--       ST_Value(rast, 1, 2), 
--       ST_Value(rast, 2, 1), 
--       ST_Value(rast, 2, 2), 
--       ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0, 1), 1, ST_TestRaster(1, 1, 1), 1, 'rast1 + rast2 + 2*rast2 + toto()', '8BSI', 'SECOND', NULL) AS rast) foo


-- INTERSECTION with rotated. -- Doesn't work...
--SELECT ST_IsEmpty(rast),
--       ST_Value(rast, 1, 1) AS "1, 1", 
--       ST_Value(rast, 1, 2) AS "1, 2", 
--       ST_Value(rast, 2, 1) AS "2, 1", 
--       ST_Value(rast, 2, 2) AS "2, 2", 
--       ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRotatedRaster(0, 0), 1, ST_TestRotatedRaster(1.2, -0.8), 1, '(rast1 + rast2)/3::float8', '64BF', 'Union', 0) AS rast) foo

--SELECT AsBinary((rast).geom), (rast).val FROM (SELECT ST_PixelAsPolygons(ST_TestRotatedRaster(0, 0), 1) rast) foo
--SELECT AsBinary((rast).geom), (rast).val FROM (SELECT ST_PixelAsPolygons(ST_TestRotatedRaster(1.2, -0.8), 1) rast) foo
--SELECT AsBinary((rast).geom), (rast).val FROM (SELECT ST_PixelAsPolygons(ST_MapAlgebra(ST_TestRotatedRaster(0, 0), 1, ST_TestRotatedRaster(1.2, -0.8), 1, '(rast1 + rast2)/3::float8', '64BF', 'Union', 0), 1) rast) foo

--SELECT ST_IsEmpty(rast),
--       ST_Value(rast, 1, 1) AS "1, 1", 
--       ST_Value(rast, 1, 2) AS "1, 2", 
--       ST_Value(rast, 2, 1) AS "2, 1", 
--       ST_Value(rast, 2, 2) AS "2, 2", 
 --      ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0), 1, ST_TestRaster(1, 1), 1, '(rast1 + rast2)/2', '64BF', 'Union', 0) AS rast) foo

--SELECT ST_IsEmpty(rast),
--       ST_Value(rast, 1, 1) AS "1, 1", 
--       ST_Value(rast, 1, 2) AS "1, 2", 
--       ST_Value(rast, 2, 1) AS "2, 1", 
--       ST_Value(rast, 2, 2) AS "2, 2", 
--       ST_BandPixelType(rast, 1), 
--       ST_Width(rast), 
--       ST_Height(rast)
--FROM (SELECT ST_MapAlgebra(ST_TestRaster(0, 0), 1, ST_TestRaster(1, 1), 1, 'CASE WHEN rast1 IS NULL THEN rast2 WHEN rast2 IS NULL THEN rast1 ELSE (rast1 + rast2)/2 END', '64BF', 'Union', 0) AS rast) foo
