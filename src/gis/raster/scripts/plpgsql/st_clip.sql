----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- NOTE: The ST_Clip function found in this file still many improvements before being implemented in C.

CREATE OR REPLACE FUNCTION ST_Clip(rast raster, x int, y int, width int, height int) 
    RETURNS raster AS 
    $$
    DECLARE
        newrast raster := ST_MakeEmptyRaster(width, height, ST_UpperLeftX(rast), ST_UpperLeftY(rast), 
                          ST_ScaleX(rast), ST_ScaleY(rast), ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast));
        numband int := ST_Numbands(rast);
    band int;
    cx int;
    cy int;
    newwidth int := CASE WHEN x + width > ST_Width(rast) THEN ST_Width(rast) - x ELSE width END;
    newheight int := CASE WHEN y + height > ST_Height(rast) THEN ST_Height(rast) - y ELSE height END;
    BEGIN
        FOR b IN 1..numband LOOP
            newrast := ST_AddBand(newrast, ST_PixelType(rast, band), ST_NodataValue(rast, band), ST_NodataValue(rast, band));
            FOR cx IN 1..newwidth LOOP
                FOR cy IN 1..newheight LOOP
                    newrast := ST_SetValue(newrast, band, cx, cy, ST_Value(rast, band, cx + x - 1, cy + y - 1));
                END LOOP;
            END LOOP;
        END LOOP;
    RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';
-------------------------------------------------------------------
-- ST_Clip
-- Clip the values of a raster to the shape of a polygon.
-- 
-- rast   - raster to be clipped
-- band   - limit the result to only one band
-- geom   - geometry defining the she to clip the raster
-- nodata - define (if there is none defined) or replace the raster nodata value with this value
-- trimraster - limit the extent of the result to the extent of the geometry
-- Todo:
-- test point
-- test line
-- test polygon smaller than pixel
-- test and optimize raster totally included in polygon 
CREATE OR REPLACE FUNCTION ST_Clip(rast raster, band int, geom geometry, nodata float8 DEFAULT null, trimraster boolean DEFAULT false) 
    RETURNS raster AS 
    $$
    DECLARE
        sourceraster raster := rast;
        newrast raster;
        geomrast raster;
        numband int := ST_Numbands(rast);
        bandstart int;
        bandend int;
        newextent text;
        newnodata float8;
        newpixtype text;
        bandi int;
    BEGIN
        IF rast IS NULL THEN
            RETURN null;
        END IF;
        IF geom IS NULL THEN
            RETURN rast;
        END IF;
        IF band IS NULL THEN
            bandstart := 1;
            bandend := numband;
        ELSEIF ST_HasNoBand(rast, band) THEN
            RAISE NOTICE 'Raster do not have band %. Returning null', band;
            RETURN null;
        ELSE
            bandstart := band;
            bandend := band;
        END IF;
        newpixtype := ST_BandPixelType(rast, bandstart);
        newnodata := coalesce(nodata, ST_BandNodataValue(rast, bandstart), ST_MinPossibleValue(newpixtype));
        newextent := CASE WHEN trimraster THEN 'INTERSECTION' ELSE 'FIRST' END;
        
--RAISE NOTICE 'newextent=%', newextent;
        -- Convert the geometry to a raster
        geomrast := ST_AsRaster(geom, rast, ST_BandPixelType(rast, band), 1, newnodata);  

        -- Set the newnodata
        sourceraster := ST_SetBandNodataValue(sourceraster, bandstart, newnodata);

        -- Compute the first raster band
        newrast := ST_MapAlgebraExpr(sourceraster, bandstart, geomrast, 1, '[rast1.val]', newpixtype, newextent);
        
        FOR bandi IN bandstart+1..bandend LOOP
--RAISE NOTICE 'bandi=%', bandi;
            -- for each band we must determine the nodata value
            newpixtype := ST_BandPixelType(rast, bandi);
            newnodata := coalesce(nodata, ST_BandNodataValue(sourceraster, bandi), ST_MinPossibleValue(newpixtype));
            sourceraster := ST_SetBandNodataValue(sourceraster, bandi, newnodata);
            newrast := ST_AddBand(newrast, ST_MapAlgebraExpr(sourceraster, bandi, geomrast, 1, '[rast1.val]', newpixtype, newextent));
        END LOOP;
        RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';

-- Variant defaulting to band 1
CREATE OR REPLACE FUNCTION ST_Clip(rast raster, geom geometry, nodata float8 DEFAULT null, trimraster boolean DEFAULT false)
    RETURNS raster 
    AS $$
        SELECT ST_Clip($1, 1, $2, $3, $4);
    $$ LANGUAGE 'SQL';

-- Variant defaulting nodata to the one of the raster or the min possible value
CREATE OR REPLACE FUNCTION ST_Clip(rast raster, band int, geom geometry, trimraster boolean)
    RETURNS raster 
    AS $$
        SELECT ST_Clip($1, $2, $3, null, $4);
    $$ LANGUAGE 'SQL';

-- Variant defaulting nodata to the one of the raster or the min possible value
CREATE OR REPLACE FUNCTION ST_Clip(rast raster, geom geometry, trimraster boolean)
    RETURNS raster 
    AS $$
        SELECT ST_Clip($1, 1, $2, null, $3);
    $$ LANGUAGE 'SQL';

-- Test

CREATE OR REPLACE FUNCTION ST_TestRaster(h integer, w integer, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(h, w, 0, 0, 1, 1, 0, 0, 0), '32BF', val, 0);
    END;
    $$
    LANGUAGE 'plpgsql';

SELECT ST_Clip(ST_TestRaster(10, 10, 2), 1, ST_Buffer(ST_MakePoint(8, 5), 4)) rast

-- Test one band raster
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_TestRaster(10, 10, 2), 1, ST_Buffer(ST_MakePoint(8, 5), 4))) gv

-- Test two bands raster
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_AddBand(ST_TestRaster(10, 10, 2), '16BUI'::text, 4, 0), null, ST_Buffer(ST_MakePoint(8, 5), 4)), 2) gv

-- Test one band raster with trimraster set to true
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_TestRaster(10, 10, 2), 1, ST_Buffer(ST_MakePoint(8, 5), 4), null, true)) gv

-- Test two bands raster with trimraster set to true
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_AddBand(ST_TestRaster(10, 10, 2), '16BUI'::text, 4, 0), null, ST_Buffer(ST_MakePoint(8, 5), 4), null, true), 2) gv

-- Test nodatavalue set by the first raster
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_SetBandNoDataValue(ST_Clip(ST_TestRaster(10, 10, 2), 1, ST_Buffer(ST_MakePoint(8, 5), 4)), null)) gv

-- Test nodatavalue set by the parameter
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_SetBandNoDataValue(ST_Clip(ST_TestRaster(10, 10, 2), 1, ST_Buffer(ST_MakePoint(8, 5), 4), -10), null)) gv

-- Test nodatavalue set by the min possible value
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_SetBandNoDataValue(ST_Clip(ST_SetBandNoDataValue(ST_TestRaster(10, 10, 2), null), 1, ST_Buffer(ST_MakePoint(8, 5), 4)), null)) gv

-- Test the variant defaulting to band 1
SELECT ST_Numbands(ST_Clip(ST_AddBand(ST_TestRaster(10, 10, 2), '16BUI'::text, 4, 0), ST_Buffer(ST_MakePoint(8, 5), 4))) gv

SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_AddBand(ST_TestRaster(10, 10, 2), '16BUI'::text, 4, 0), ST_Buffer(ST_MakePoint(8, 5), 4)), 1) gv

-- Test defaulting to min possible value and band 1
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_SetBandNoDataValue(ST_Clip(ST_SetBandNoDataValue(ST_TestRaster(10, 10, 2), null), ST_Buffer(ST_MakePoint(8, 5), 4)), null)) gv

-- Test defaulting to nodatavalue set by the first raster and band 1
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_SetBandNoDataValue(ST_Clip(ST_TestRaster(10, 10, 2), ST_Buffer(ST_MakePoint(8, 5), 4)), null)) gv

-- Test when band number does not exist
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_TestRaster(10, 10, 2), 2, ST_Buffer(ST_MakePoint(8, 5), 4))) gv

-- Test point -- bug. The produced raster does not have the same alignment
SELECT ST_AsBinary((gv).geom), (gv).val 
FROM ST_PixelAsPolygons(ST_Clip(ST_TestRaster(10, 10, 2), ST_MakePoint(8.5, 5.5))) gv
