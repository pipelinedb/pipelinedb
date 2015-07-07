----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

--NOTE: Both ST_SetValues functions found in this file are ready to be being implemented in C

--------------------------------------------------------------------
-- ST_SetValues   - Set a range of raster pixels to a value.
-- 
-- Arguments
--
-- rast raster    - Raster to be edited.
-- band integer   - Band number of the raster to be edited. Default to 1.
-- x, y           - Raster coordinates of the upper left corner of the range 
--                  of pixel to be edited.
-- width, height  - Width and height of the range of pixel to be edited.
-- val            - Value to set the range. If NULL, pixels are set to nodata.
-- keepdestnodata - Flag indicating not to change pixels set to nodata value.
--                  Default to FALSE.
--
-- When x, y, width or height are out of the raster range, only the part 
-- of the range intersecting with the raster is set.
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_SetValues(rast raster, band int, x int, y int, width int, height int, val float8, keepdestnodata boolean)
    RETURNS raster AS 
    $$
    DECLARE
        newraster raster := rast;
        cx int;
        cy int;
        newwidth int := width;
        newheight int := height;
        newband int := band;
        oldx int := x;
        oldy int := y;
        newx int;
        newy int;
        newval float8 := val;
        newkeepdestnodata boolean := keepdestnodata;
        rastnodataval int := 0;
    BEGIN
        IF rast IS NULL THEN
            RAISE NOTICE 'ST_SetValues: No raster provided. Returns NULL';
            RETURN NULL;
        END IF;
        
        IF ST_IsEmpty(rast) OR ST_HasNoBand(rast, band) THEN
            RAISE NOTICE 'ST_SetValues: Empty or no band raster provided. Returns rast';
            RETURN rast;
        END IF;
        
        IF newband IS NULL THEN
            newband := 1;
        END IF;
        
        IF newband < 1 THEN
            RAISE NOTICE 'ST_SetValues: band out of range. Returns rast';
            RETURN rast;
        END IF;
        
        IF width IS NULL OR width < 1 OR height IS NULL OR height < 1 THEN
            RAISE NOTICE 'ST_SetValues: invalid width or height. Returns rast';
            RETURN rast;
        END IF;

        IF x IS NULL THEN
            oldx := 1;
        END IF;

        newx := 1 + LEAST(GREATEST(0, oldx - 1), ST_Width(rast));
        newwidth := GREATEST(LEAST(1 + ST_Width(rast), oldx + newwidth), 1) - newx;        
                
        IF y IS NULL THEN
            oldy := 1;
        END IF;
        
        newy := 1 + LEAST(GREATEST(0, oldy - 1), ST_Height(rast));
        newheight := GREATEST(LEAST(1 + ST_Height(rast), oldy + newheight), 1) - newy;

        IF newwidth < 1 OR newheight < 1 THEN
            RETURN rast;
        END IF;
        
        IF newkeepdestnodata IS NULL THEN
            newkeepdestnodata := FALSE;
        END IF;
        
        IF newkeepdestnodata THEN
            IF NOT ST_BandNodataValue(rast, newband) IS NULL THEN
                rastnodataval := ST_BandNoDataValue(rast, newband);
            ELSE
                RAISE NOTICE 'ST_SetValues: keepdestnodata was set to TRUE but rast1 does not have a nodata value. keepdestnodata reset to FALSE';
                newkeepdestnodata := FALSE;
            END IF;
            IF ST_BandIsNodata(rast, band) THEN
                RETURN rast;
            END IF;
        END IF;

        IF newval IS NULL THEN
            newval := ST_BandNoDataValue(rast, band);
        END IF;

        IF newval IS NULL THEN
            RAISE NOTICE 'ST_SetValues: val is NULL and no nodata exist for rast. Returns rast';
            RETURN rast;
        END IF;

        IF newkeepdestnodata THEN
            FOR cx IN newx..newx + newwidth - 1 LOOP
                FOR cy IN newy..newy + newheight - 1 LOOP
                    IF ST_Value(newraster, newband, cx, cy) != rastnodataval THEN
                        newraster := ST_SetValue(newraster, newband, cx, cy, newval);
                    END IF;
                END LOOP;
            END LOOP;
        ELSE
            FOR cx IN newx..newx + newwidth - 1 LOOP
                FOR cy IN newy..newy + newheight - 1 LOOP
                    newraster := ST_SetValue(newraster, newband, cx, cy, newval);
                END LOOP;
            END LOOP;
        END IF;
        
        RETURN newraster;
    END;
    $$
    LANGUAGE 'plpgsql';

-- Variant with band = 1
CREATE OR REPLACE FUNCTION ST_SetValues(rast raster, x int, y int, width int, height int, val float8, keepdestnodata boolean)
    RETURNS raster
    AS 'SELECT ST_SetValues($1, 1, $2, $3, $4, $5, $6, $7)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant with band = 1 & keepdestnodata = FALSE
CREATE OR REPLACE FUNCTION ST_SetValues(rast raster, x int, y int, width int, height int, val float8)
    RETURNS raster
    AS 'SELECT ST_SetValues($1, 1, $2, $3, $4, $5, $6, FALSE)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant with keepdestnodata = FALSE
CREATE OR REPLACE FUNCTION ST_SetValues(rast raster, band int, x int, y int, width int, height int, val float8)
    RETURNS raster
    AS 'SELECT ST_SetValues($1, $2, $3, $4, $5, $6, $7, FALSE)'
    LANGUAGE 'SQL' IMMUTABLE;

--Test rasters
CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(10, 10, ulx, uly, 1, 1, 0, 0, -1), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';


-- Test
SELECT ST_SetValues(ST_TestRaster(0, 0, 1), 2, 2, 1, 1, 0)

SELECT (pix).geom, (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(0, 0, 1)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, 1), 2, 1, 1, 1, 0)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, 1), 2, 1, 1, 10, 0)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, 1), 1, 1, 4, 4, 0)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, 1), 0, 3, 4, 4, 0)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, -1), 2, 2, 2, 2, 0, TRUE)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_SetBandNoDataValue(ST_TestRaster(0, 0, -1), NULL), 2, 2, 2, 2, 0, TRUE)) as pix) foo

--------------------------------------------------------------------
-- ST_SetValues   - Set a range of raster pixels to values copied from 
--                  the corresponding pixels in another raster.
-- Arguments
--
-- rast1 raster   - Raster to be edited.
-- band1 integer  - Band number of the raster to be edited. Default to 1.
-- x, y           - Raster coordinates of the upper left corner of the 
--                  range of pixel to be edited.
-- width, height  - Width and height of the range of pixel to be edited.
-- rast2          - Raster values are copied from.
-- band2          - Band number of the raster values are copied from. 
-- keepdestnodata - Flag indicating not to change pixels (in the edited 
--                  raster) set to nodata value. Default to FALSE.
-- keepsourcetnodata - Flag indicating not to copy pixels (from the source 
--                  raster) set to nodata value. Default to FALSE.
--
-- When x, y, width or height are out of the raster range, only the part 
-- of the range intersecting with the raster is set.
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_SetValues(rast1 raster, band1 int, x int, y int, width int, height int, rast2 raster, band2 int, keepdestnodata boolean, keepsourcenodata boolean)
    RETURNS raster AS 
    $$
    DECLARE
        newraster raster := rast1;
        newwidth int := width;
        newheight int := height;
        oldx int := x;
        oldy int := y;
        newx int;
        newy int;
        newband1 int := band1;
        newband2 int := band2;
        newkeepdestnodata boolean := keepdestnodata;
        newkeepsourcenodata boolean := keepsourcenodata;
        r2val float;
        rast1nodataval float;
        rast2nodataval float;

        x2 int;
        y2 int;
        cx int;
        cy int;
    BEGIN
        IF rast1 IS NULL THEN
            RAISE NOTICE 'ST_SetValues: No raster provided. Return NULL';
            RETURN NULL;
        END IF;
        
        IF ST_IsEmpty(rast1) OR ST_HasNoBand(rast1, band1) THEN
            RAISE NOTICE 'ST_SetValues: Empty or no band destination raster provided. Returns rast1';
            RETURN rast1;
        END IF;
        
        IF  rast2 IS NULL OR ST_IsEmpty(rast2) OR ST_HasNoBand(rast2, band2) THEN
            RAISE NOTICE 'ST_SetValues: Empty or no band source raster provided. Returns rast1';
            RETURN rast1;
        END IF;

        IF newband1 IS NULL THEN
            newband1 := 1;
        END IF;
        
        IF newband1 < 1 THEN
            RAISE NOTICE 'ST_SetValues: band1 out of range. Returns rast';
            RETURN rast1;
        END IF;

        IF newband2 IS NULL THEN
            newband2 := 1;
        END IF;
        
        IF newband2 < 1 THEN
            RAISE NOTICE 'ST_SetValues: band2 out of range. Returns rast';
            RETURN rast1;
        END IF;
        
        IF x IS NULL THEN
            oldx := 1;
        END IF;

        newx := 1 + LEAST(GREATEST(0, oldx - 1), ST_Width(rast1));
        newwidth := GREATEST(LEAST(1 + ST_Width(rast1), oldx + newwidth), 1) - newx;
        oldx := newx;      
                
        IF y IS NULL THEN
            oldy := 1;
        END IF;
        
--RAISE NOTICE 'aaa oldy=%, newheight=%', oldy, newheight;

        newy := 1 + LEAST(GREATEST(0, oldy - 1), ST_Height(rast1));
        newheight := GREATEST(LEAST(1 + ST_Height(rast1), oldy + newheight), 1) - newy;
        oldy := newy;      

--RAISE NOTICE 'bbb newx=%, newy=%', newx, newy;
--RAISE NOTICE 'ccc newwidth=%, newheight=%', newwidth, newheight;
        IF newwidth < 1 OR newheight < 1 THEN
            RETURN rast1;
        END IF;

        x2 := ST_World2RasterCoordX(rast1, ST_Raster2WorldCoordX(rast2, 1, 1), ST_Raster2WorldCoordY(rast2, 1, 1));
        y2 := ST_World2RasterCoordY(rast1, ST_Raster2WorldCoordY(rast2, 1, 1), ST_Raster2WorldCoordY(rast2, 1, 1));
    
--RAISE NOTICE '111 x2=%, y2=%', x2, y2;

        newx := x2 + LEAST(GREATEST(0, oldx - x2), ST_Width(rast2));
        newwidth := GREATEST(LEAST(x2 + ST_Width(rast2), oldx + newwidth), x2) - newx; 

        newy := y2 + LEAST(GREATEST(0, oldy - y2), ST_Height(rast2));
        newheight := GREATEST(LEAST(y2 + ST_Height(rast2), oldy + newheight), y2) - newy; 

        IF newwidth < 1 OR newheight < 1 THEN
            RETURN rast1;
        END IF;

--RAISE NOTICE '222 newx=%, newy=%', newx, newy;
--RAISE NOTICE '333 newwidth=%, newheight=%', newwidth, newheight;

        IF newkeepdestnodata IS NULL THEN
            newkeepdestnodata := FALSE;
        END IF;
        
        IF newkeepdestnodata THEN
            IF NOT ST_BandNodataValue(rast1, newband1) IS NULL THEN
                rast1nodataval := ST_BandNoDataValue(rast1, newband1);
            ELSE
                RAISE NOTICE 'ST_SetValues: keepdestnodata was set to TRUE but rast1 does not have a nodata value. keepdestnodata reset to FALSE';
                newkeepdestnodata := FALSE;
            END IF;
            IF ST_BandIsNodata(rast1, newband1) THEN
                RETURN rast1;
            END IF;
        END IF;

        IF newkeepsourcenodata IS NULL THEN
            newkeepsourcenodata := FALSE;
        END IF;
        
        IF newkeepsourcenodata THEN
            IF NOT ST_BandNodataValue(rast2, newband2) IS NULL THEN
                rast2nodataval := ST_BandNoDataValue(rast2, newband2);
            ELSE
                RAISE NOTICE 'ST_SetValues: keepsourcenodata was set to true but rast2 does not have a nodata value. keepsourcenodata reset to false';
                newkeepsourcenodata := FALSE;
            END IF;
            IF ST_BandIsNodata(rast2, newband2) THEN
                RETURN rast1;
            END IF;
        END IF;

        IF newkeepdestnodata THEN
            FOR cx IN newx..newx + newwidth - 1 LOOP
                FOR cy IN newy..newy + newheight - 1 LOOP
                    r2val := ST_Value(rast2, newband2, cx - x2 + 1, cy - y2 + 1);
--RAISE NOTICE '444 x=%, y=%', cx, cy;
                    IF (ST_Value(newraster, newband1, cx, cy) != rast1nodataval) AND (NOT newkeepsourcenodata OR r2val != rast2nodataval) THEN
                        newraster := ST_SetValue(newraster, newband1, cx, cy, r2val);
                    END IF;
                END LOOP;
            END LOOP;
        ELSE
--RAISE NOTICE '555 newx + newwidth - 1=%, newy + newheight - 1=%', newx + newwidth - 1, newy + newheight - 1;
           FOR cx IN newx..newx + newwidth - 1 LOOP
                FOR cy IN newy..newy + newheight - 1 LOOP
                    r2val := ST_Value(rast2, newband2, cx - x2 + 1, cy - y2 + 1);
--RAISE NOTICE '666 x=%, y=%', cx, cy;
                    IF NOT newkeepsourcenodata OR r2val != rast2nodataval THEN
                        newraster := ST_SetValue(newraster, newband1, cx, cy, r2val);
                    END IF;
                END LOOP;
            END LOOP;
        END IF;
        RETURN newraster;
    END;
    $$
    LANGUAGE 'plpgsql';

-- Variant with both band = 1
CREATE OR REPLACE FUNCTION ST_SetValues(rast1 raster, x int, y int, width int, height int, rast2 raster, keepdestnodata boolean, keepsourcenodata boolean)
    RETURNS raster
    AS 'SELECT ST_SetValues($1, 1, $2, $3, $4, $5, $6, 1, $7, $8)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Variant with both band = 1 & both keepnodata = FALSE
CREATE OR REPLACE FUNCTION ST_SetValues(rast1 raster, x int, y int, width int, height int, rast2 raster)
    RETURNS raster
    AS 'SELECT ST_SetValues($1, 1, $2, $3, $4, $5, $6, 1, FALSE, FALSE)'
    LANGUAGE 'SQL' IMMUTABLE;

-- Test
SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(0, 0, 1)) as pix) foo

SELECT ST_AsBinary((pix).geom), (pix).val
FROM (SELECT ST_PixelAsPolygons(ST_SetValues(ST_TestRaster(0, 0, 1), 2, 1, 3, 1, ST_TestRaster(3, 0, 3))) as pix) foo



