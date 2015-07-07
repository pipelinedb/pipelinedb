----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- Note: The functions provided in this script are in developement. Do not use.

-- Note: this script is dependent on 
--   _MapAlgebraParts(r1x int, r1y int, r1w int, r1h int, r2x int, r2y int, r2w int, r2h int) 
--   ST_SameAlignment(rast1ulx float8, rast1uly float8, rast1scalex float8, rast1scaley float8, rast1skewx float8, rast1skewy float8, rast2ulx float8, rast2uly float8, rast2scalex float8, rast2scaley float8, rast2skewx float8, rast2skewy float8)
--   ST_IsEmpty(raster)
--   ST_HasNoBand(raster, int)
--   ST_BandIsNoData(raster, int)
-- to be found in the script/plpgsql folder

--------------------------------------------------------------------
-- ST_MapAlgebra - (two rasters version) Return a raster which 
--                 values are the result of an SQL expression involving 
--                 pixel values from input rasters bands.
-- Arguments 
-- rast1 raster -  First raster referred by rast1 in the expression. 
-- band1 integer - Band number of the first raster. Default to 1.
-- rast2 raster -  Second raster referred by rast2 in the expression.
-- band2 integer - Band number of the second raster. Default to 1.
-- expression text - SQL expression. Ex.: "rast1 + 2 * rast2"
-- pixeltype text - Pixeltype assigned to the resulting raster. Expression
--                  results are truncated to this type. Default to the 
--                  pixeltype of the first raster.
-- extentexpr text - Raster extent of the result. Can be: 
--                     -FIRST: Same extent as the first raster. Default.
--                     -SECOND: Same extent as the second) raster. Default.
--                     -INTERSECTION: Intersection of extent of the two rasters.
--                     -UNION: Union oof extent of the two rasters.
-- nodata1expr text - Expression used when only rast1 pixel value are nodata or absent, i.e. rast2 pixel value is with data.
-- nodata2expr text - Expression used when only rast2 pixel value are nodata or absent, i.e. rast1 pixel value is with data.
-- nodatanodataexpr text - Expression used when both pixel values are nodata values or absent.

-- Further enhancements:
-- -Move the expression parameter in seventh position just before other expression parameters.
-- -Optimization for UNION & FIRST. We might not have to iterate over all the new raster. See st_mapalgebra_optimized.sql
-- -Add the possibility to assign the x or y coordinate of the pixel to the pixel (do the same to the one raster version).
-- -Resample the second raster when necessary (Require ST_Resample)
-- -More test with rotated images
--------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_MapAlgebra2(rast1 raster, 
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
        r1 float;
        r2 float;
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
        
        r1x int;
        r1y int;
        r1w int;
        r1h int;
        r2x int;
        r2y int;
        r2w int;
        r2h int;
        
        newrx int;
        newry int;
        
        newrast raster;
        tmprast raster;
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
        
        newval float;
        newexpr text;
        upnodatanodataexpr text;
        upnodata1expr text;
        upnodata2expr text;
        upexpression text;
        nodatanodataval float;
        skipcomputation int;
        
        zones int[];
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
RAISE NOTICE 'ST_MapAlgebra2 000';

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
        IF rast1 IS NULL OR ST_IsEmpty(rast1) THEN
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
        IF rast2 IS NULL OR ST_IsEmpty(rast2) THEN
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

        --r1x & r2x are the offset of each rasters relatively to global extent
        r1x := 0;
        r2x := -st_world2rastercoordx(rast2, rast1ulx, rast1uly) + 1;
        IF r2x < 0 THEN
            r1x := -r2x;
            r2x := 0;
        END IF;
        r1y := 0;
        r2y := -st_world2rastercoordy(rast2, rast1ulx, rast1uly) + 1;
        IF r2y < 0 THEN
            r1y := -r2y;
            r2y := 0;
        END IF;
        
        r1w := rast1width;
        r1h := rast1height;
        r2w := rast2width;
        r2h := rast2height;
        
        zones := _MapAlgebraParts(r1x + 1, r1y + 1, r1w, r1h, r2x + 1, r2y + 1, r2w, r2h);
        z11x := zones[1];
        z11y := zones[2];
        z11w := zones[3];
        z11h := zones[4];
        z12x := zones[5];
        z12y := zones[6];
        z12w := zones[7];
        z12h := zones[8];
        z13x := zones[9];
        z13y := zones[10];
        z13w := zones[11];
        z13h := zones[12];
        z14x := zones[13];
        z14y := zones[14];
        z14w := zones[15];
        z14h := zones[16];
        z21x := zones[17];
        z21y := zones[18];
        z21w := zones[19];
        z21h := zones[20];
        z22x := zones[21];
        z22y := zones[22];
        z22w := zones[23];
        z22h := zones[24];
        z23x := zones[25];
        z23y := zones[26];
        z23w := zones[27];
        z23h := zones[28];
        z24x := zones[29];
        z24y := zones[30];
        z24w := zones[31];
        z24h := zones[32];
        zcx := zones[33];
        zcy := zones[34];
        zcw := zones[35];
        zch := zones[36];
        
        -- Compute x and y relative index of master and slave according to the extent expression (FIRST, SECOND, INTERSECTION or UNION)
        IF extentexpr IS NULL OR upper(extentexpr) = 'FIRST' THEN

            -- Check if rast1 is NULL
            IF rast1 IS NULL THEN 
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

            newrx := r1x;
            newry := r1y;
            z21w := 0;
            z22w := 0;
            z23w := 0;
            z24w := 0;

        ELSIF upper(extentexpr) = 'SECOND' THEN

            -- Check if rast2 is NULL
            IF rast2 IS NULL THEN 
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

            newrx := r2x;
            newry := r2y;
            z11w := 0;
            z12w := 0;
            z13w := 0;
            z14w := 0;

        ELSIF upper(extentexpr) = 'INTERSECTION' THEN

            -- Check if the intersection is empty.
            IF zcw = 0 OR zch = 0 OR
               rast1 IS NULL OR ST_IsEmpty(rast1) OR
               rast2 IS NULL OR ST_IsEmpty(rast2) THEN 
                RAISE NOTICE 'ST_MapAlgebra: INTERSECTION of provided rasters is empty. Returning an empty raster';
                RETURN ST_MakeEmptyRaster(0, 0, 0, 0, 0, 0, 0, 0, newsrid);
            END IF;
            

            -- Compute the new ulx and uly
            newulx := st_raster2worldcoordx(rast1, zcx - r1x + 1, zcy - r1y + 1);
            newuly := st_raster2worldcoordy(rast1, zcx - r1x + 1, zcy - r1y + 1);
            newwidth := zcw;
            newheight := zch;

            newrx := zcx;
            newry := zcy;
            z11w := 0;
            z12w := 0;
            z13w := 0;
            z14w := 0;
            z21w := 0;
            z22w := 0;
            z23w := 0;
            z24w := 0;

        ELSIF upper(extentexpr) = 'UNION' THEN

            IF rast1 IS NULL OR ST_IsEmpty(rast1) THEN
                newulx := rast2ulx;
                newuly := rast2uly;
                newwidth := rast2width;
                newheight := rast2height;

                newrx := r2x;
                newry := r2y;
                z21w := 0;
                z22w := 0;
                z23w := 0;
                z24w := 0;
            ELSIF rast2 IS NULL OR ST_IsEmpty(rast2) THEN
                newulx := rast1ulx;
                newuly := rast1uly;
                newwidth := rast1width;
                newheight := rast1height;

                newrx := r1x;
                newry := r1y;
                z11w := 0;
                z12w := 0;
                z13w := 0;
                z14w := 0;
            ELSE
                newrx := 0;
                newry := 0;

                newulx := st_raster2worldcoordx(rast1, r1x + 1, r1y + 1);
                newuly := st_raster2worldcoordy(rast1, r1x + 1, r1y + 1);
                newwidth := max(r1x + r1w, r2x + r2w);
                newheight := max(r1y + r1h, r2y + r2h);

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
        IF NOT ST_HasNoBand(rast1, band1) AND ST_BandHasNodataValue(rast1, band1) THEN
            rast1nodataval := ST_BandNodatavalue(rast1, band1);
        ELSE
            rast1nodataval := NULL;
        END IF;
         -- Get the nodata value for second raster
        IF NOT ST_HasNoBand(rast2, band2) AND ST_BandHasNodatavalue(rast2, band2) THEN
            rast2nodataval := ST_BandNodatavalue(rast2, band2);
        ELSE
            rast2nodataval := NULL;
        END IF;
        
        -- Determine new notadavalue
        IF (upper(extentexpr) = 'SECOND' AND NOT rast2nodataval IS NULL) THEN
            newnodatavalue := rast2nodataval;
        ELSEIF NOT rast1nodataval IS NULL THEN
            newnodatavalue := rast1nodataval;
        ELSE
            RAISE NOTICE 'ST_MapAlgebra: Both source rasters do not have a nodata value, nodata value for new raster set to the minimum value possible';
            newnodatavalue := ST_MinPossibleValue(newrast);
        END IF;
         
        upnodatanodataexpr := upper(nodatanodataexpr);
        upnodata1expr := upper(nodata1expr);
        upnodata2expr := upper(nodata2expr);
        upexpression := upper(expression);

        -- Initialise newrast with nodata-nodata values. Then we don't have anymore to set values for nodata-nodata pixels.
        IF upnodatanodataexpr IS NULL THEN
            nodatanodataval := newnodatavalue;
        ELSE
            EXECUTE 'SELECT ' || upnodatanodataexpr INTO nodatanodataval;
            IF nodatanodataval IS NULL THEN
                nodatanodataval := newnodatavalue;
            ELSE
                newnodatavalue := nodatanodataval;
            END IF;
        END IF;

        -------------------------------------------------------------------
        --Create the raster receiving all the computed values. Initialize it to the new nodatavalue.
        newrast := ST_AddBand(ST_MakeEmptyRaster(newwidth, newheight, newulx, newuly, newscalex, newscaley, newskewx, newskewy, newsrid), newpixeltype, newnodatavalue, newnodatavalue);
        -------------------------------------------------------------------

RAISE NOTICE 'ST_MapAlgebra2 111 z11x=%, z11y=%, z11w=%, z11h=%', z11x, z11y, z11w, z11h;
        -- First zone with only rast1 (z11)
        IF z11w > 0 AND z11h > 0 AND NOT ST_BandIsNodata(rast1, band1) AND NOT nodata2expr IS NULL THEN
            IF upnodata2expr = 'RAST' THEN

                
                -- IF rast1nodataval != nodatanodataval THEN
RAISE NOTICE 'ST_MapAlgebra2 222';
                --     newrast := ST_SetValues(newrast, 1, z11x, z11y, z11w, z11h, nodatanodataval);
                -- END IF;
RAISE NOTICE 'ST_MapAlgebra2 333 z11x=%, z11y=%, z11w=%, z11h=%', z11x, z11y, z11w, z11h;
                newrast := ST_SetValues(newrast, 1, z11x, z11y, z11w, z11h, rast1, band1, NULL, TRUE);

            ELSE
RAISE NOTICE 'ST_MapAlgebra2 444';

                tmprast := ST_Clip(rast1, z11x - r1x + 1, z11y - r1y + 1, z11w, z11h);
                newrast := ST_Mapalgebra2(newrast, 1, tmprast, 1, replace(upnodata2expr, 'RAST', 'RAST2'), NULL, 'FIRST', NULL, 'RAST', NULL);

            END IF;
        END IF;

RAISE NOTICE 'ST_MapAlgebra2 555';

        -- Common zone (zc)
        skipcomputation = 0;
        IF zcw > 0 AND zch > 0 AND (NOT ST_BandIsNodata(rast1, band1) OR NOT ST_BandIsNodata(rast2, band2)) THEN
            
RAISE NOTICE 'ST_MapAlgebra2 666';
            -- Initialize the zone with nodatavalue. We will not further compute nodata nodata pixels
            -- newrast := ST_SetValues(newrast, 1, zcx + 1, zcy + 1, zcw, zch, newnodatavalue);

            -- If rast1 is only nodata values, apply nodata1expr
            IF ST_BandIsNodata(rast1, band1) THEN

                IF nodata1expr IS NULL THEN

                    -- Do nothing
                    skipcomputation = 0;
                    
                ELSEIF upnodata1expr = 'RAST' THEN

                    -- Copy rast2 into newrast
                    newrast := ST_SetValues(newrast, 1, zcx, zcy, zcw, zch, , rast2, band2, NULL, 'KEEP');

                ELSE
                    -- If nodata1expr resume to a constant
                    IF position('RAST' in upnodata1expr) = 0 THEN

                        EXECUTE 'SELECT ' || upnodata1expr INTO newval;
                        IF newval IS NULL OR newval = newnodatavalue THEN
                            -- The constant is equal to nodata. We have nothing to compute since newrast was already initialized to nodata
                            skipcomputation := 2;                    
                        ELSEIF newnodatavalue IS NULL THEN 
                            -- We can globally initialize to the constant only if there was no newnodatavalue.
                            newrast := ST_SetValues(newrast, 1, zcx, zcy, zcw, zch, newval);
                            skipcomputation := 2;
                        ELSE
                            -- We will assign the constant pixel by pixel.
                            skipcomputation := 1;
                        END IF;
                    END IF;
                    IF skipcomputation < 2 THEN
                        FOR x IN 1..zcw LOOP
                            FOR y IN 1..zch LOOP
                                r2 := ST_Value(rast2, band2, x + r2x, y + r2y);
                                IF (r2 IS NULL OR r2 = rast2nodataval) THEN
                                    -- Do nothing since the raster have already been all set to this value
                                ELSE
                                    IF skipcomputation < 1 THEN
                                        newexpr := replace('SELECT ' || upnodata1expr, 'RAST', r2);
                                        EXECUTE newexpr INTO newval;
                                        IF newval IS NULL THEN
                                            newval := newnodatavalue;
                                        END IF;
                                    END IF;
                                    newrast = ST_SetValue(newrast, 1, x + zcx, y + zcy, newval);
                                END IF;
                            END LOOP;
                        END LOOP;
                    END IF;
                END IF;
            ELSEIF ST_BandIsNodata(rast2, band2) THEN
            ELSE
            END IF;
        END IF;

        RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(5, 5, ulx, uly, 1, -1, 0, 0, -1), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';


SELECT asbinary((gv).geom), (gv).val
FROM st_pixelaspolygons(ST_TestRaster(-10, 2, 1)) gv;

SELECT asbinary(_MapAlgebraAllPartsGeom(0, 0, 2, 1, 1, 0, 2, 1))

SELECT asbinary(pix.geom) as geom, pix.val
FROM st_pixelaspolygons(ST_MapAlgebra2(ST_TestRaster(0, 1, 1), 1, ST_TestRaster(1, 0, 1), 1, '(rast1 + rast2) / 2', NULL, 'union', '2*rast', 'rast', NULL), 1) as pix




