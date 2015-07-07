----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------
-- ST_Tile
-- Split a raster into a set of raster tiles, one tile per row returned. 
-- Works on multiband rasters. There is no way to specify the upper left 
-- corner of the new tiled grid. The grid start at the upperleft corner 
-- of the provided raster.
--
-- rast   - Raster to be tiled.
-- width  - Width of the tiles.
-- height - Height of the tiles
-- padwithnodata - If TRUE, the produced tiles are strictly width x heigth pixels. 
--                 Pixels outside the extent of the passed raster are filled with 
--                 nodata value. When FALSE out of bound tiles are clipped to the 
--                 extent of the raster. Default to FALSE.
-- nodatavalue   - nodata value to use to pad the outbound tiles when the provided 
--                 raster do not have a nodata value defined. If not provided and 
--                 the raster do not have a nodata value defined 
--                 ST_MinPossibleValue(ST_BandPixelType(rast, band)) is used for each band.
--
-- Example producing 120 x 120 pixel tiles
--
-- CREATE TABLE srtm_22_03_tiled_120x120 AS
-- SELECT ST_Tile(rast, 120, 120, true), generate_series(1, 3600) rid
-- FROM srtm_22_03;
----------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS ST_Tile(rast raster, width integer, height integer, padwithnodata boolean, nodatavalue double precision);
CREATE OR REPLACE FUNCTION ST_Tile(rast raster, width integer, height integer, padwithnodata boolean DEFAULT FALSE, nodatavalue double precision DEFAULT NULL) 
    RETURNS SETOF raster AS 
    $$
    DECLARE
        gridrast raster;
        rastwidth integer;
        rastheight integer;
        gridwidth integer;
        gridheight integer;
        newnodata double precision;
        newpixtype text;
        nbband integer;
        bandi integer;
        newrast raster;
        initvalue double precision;
        grid record;
        geomrast raster;
    BEGIN
        IF rast IS NULL THEN
            RETURN;
        END IF;
    
        nbband := ST_Numbands(rast);
        IF nbband < 1 THEN
                RAISE NOTICE 'Raster do not have band %. Returning null', band;
                RETURN;
        END IF;

        rastwidth := ST_Width(rast);
        IF width IS NULL THEN
            width := rastwidth;
        END IF;

        rastheight := ST_Height(rast);
        IF height IS NULL THEN
            height := rastheight;
        END IF;

        gridwidth := (rastwidth / width) + CASE WHEN rastwidth % width > 0 THEN 1 ELSE 0 END;
        gridheight := (rastheight / height) + CASE WHEN rastheight % height > 0 THEN 1 ELSE 0 END;

        gridrast := ST_AddBand(ST_MakeEmptyRaster(gridwidth, gridheight, ST_UpperLeftX(rast), ST_UpperLeftY(rast), ST_ScaleX(rast) * width, ST_ScaleY(rast) * height, ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast)), '8BUI'::text, 1, 0);
        IF padwithnodata THEN
            FOR grid IN SELECT (ST_PixelAsPolygons(gridrast)).geom LOOP
                FOR bandi IN 1..nbband LOOP
                    -- for each band we must determine the nodata value
                    newpixtype := ST_BandPixelType(rast, bandi);
                    newnodata := ST_BandNodataValue(rast, bandi);
                    IF newnodata IS NULL THEN
                        newnodata := coalesce(nodatavalue, ST_MinPossibleVal(newpixtype));
                        rast := ST_SetBandNodataValue(rast, newnodata);
                    END IF;
--RAISE NOTICE 'newnodata1 %', ST_BandNodataValue(rast);

                    geomrast := ST_AsRaster(grid.geom, rast);
                    IF bandi = 1 THEN
                        newrast := ST_SetBandNodataValue(ST_MapAlgebraExpr(rast, 1, geomrast, 1, 'RAST1', newpixtype, 'SECOND', newnodata::text, newnodata::text, newnodata), newnodata);
                    ELSE
                        newrast := ST_AddBand(newrast, ST_SetBandNodataValue(ST_MapAlgebraExpr(rast, bandi, geomrast, 1, 'RAST1', newpixtype, 'SECOND', newnodata::text, newnodata::text, newnodata), newnodata));
                    END IF;
--RAISE NOTICE 'newnodata2 = %, newnodata = %, numbands = %, type = %, newset = %', ST_BandNodataValue(newrast), newnodata, ST_NumBands(newrast), ST_BandPixelType(newrast), ST_BandNodataValue(ST_SetBandNodataValue(newrast, 1, -4));
                END LOOP;
                RETURN NEXT newrast;
            END LOOP;
            RETURN;
        ELSE
            RETURN QUERY SELECT ST_Clip(rast, (ST_PixelAsPolygons(gridrast)).geom, NULL, TRUE) rast;
        END IF;
        RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';
    
----------------------------------------------------------------------
-- ST_TileAsGeom
-- Split a raster into a set of raster tiles, returning only the geometry 
-- corresponding to each tile. 
-- There is no way to specify the upper left corner of the new tiled grid. 
-- The grid start at the upperleft corner of the provided raster.
--
-- rast   - Raster to be tiled.
-- width  - Width of the tiles.
-- height - Height of the tiles
-- padwithnodata - If TRUE, the produced tiles are strictly width x heigth pixels. 
--                 Pixels outside the extent of the passed raster are filled with 
--                 nodata value. When FALSE out of bound tiles are clipped to the 
--                 extent of the raster. Default to FALSE.
--
-- Example producing 120 x 120 pixel tiles
--
-- SELECT ST_TileAsGeom(rast, 130, 130, true)
-- FROM srtm_22_03;
----------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS ST_TileAsGeom(rast raster, width integer, height integer, padwithnodata boolean);
CREATE OR REPLACE FUNCTION ST_TileAsGeom(rast raster, width integer, height integer, padwithnodata boolean DEFAULT FALSE) 
    RETURNS SETOF geometry AS 
    $$
    DECLARE
        gridrast raster;
        rastwidth integer;
        rastheight integer;
        gridwidth integer;
        gridheight integer;
        nbband integer;
        rastextent geometry;
    BEGIN
        IF rast IS NULL THEN
            RETURN;
        END IF;
    
        nbband := ST_Numbands(rast);
        IF nbband < 1 THEN
                RAISE NOTICE 'Raster do not have band %. Returning null', band;
                RETURN;
        END IF;

        rastwidth := ST_Width(rast);
        IF width IS NULL THEN
            width := rastwidth;
        END IF;

        rastheight := ST_Height(rast);
        IF height IS NULL THEN
            height := rastheight;
        END IF;

        gridwidth := (rastwidth / width) + CASE WHEN rastwidth % width > 0 THEN 1 ELSE 0 END;
        gridheight := (rastheight / height) + CASE WHEN rastheight % height > 0 THEN 1 ELSE 0 END;

        gridrast := ST_AddBand(ST_MakeEmptyRaster(gridwidth, gridheight, ST_UpperLeftX(rast), ST_UpperLeftY(rast), ST_ScaleX(rast) * width, ST_ScaleY(rast) * height, ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast)), '8BUI'::text, 1, 0);
        IF padwithnodata THEN
            RETURN QUERY SELECT (ST_PixelAsPolygons(gridrast)).geom geom;
        ELSE
            rastextent := rast::geometry;
            RETURN QUERY SELECT ST_Intersection(rastextent, (ST_PixelAsPolygons(gridrast)).geom) geom;
        END IF;
        RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';


-- Redefine ST_TestRaster()
CREATE OR REPLACE FUNCTION ST_TestRaster(ulx float8, uly float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(48, 63, ulx, uly, 0.001, -0.001, 0, 0, 4269), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

-----------------------------------------------
-- Display the test raster
SELECT ST_AsBinary((gv).geom) geom,  (gv).val
FROM (SELECT ST_PixelAsPolygons(ST_TestRaster(0, 0, 1)) gv) foo;

-- Tile it to 10x10 tiles
SELECT ST_Tile(ST_TestRaster(0, 0, 1), 10, 10)

-- Display the result of the tile function
SELECT ST_AsBinary((gv).geom) geom,  (gv).val
FROM (SELECT ST_PixelAsPolygons(ST_Tile(ST_TestRaster(0, 0, 1), 3, 4)) gv) foo;

-- Display each tile as a geometry
SELECT ST_Asbinary(ST_Tile(ST_TestRaster(0, 0, 1), 10, 10)::geometry);

-- Test the padwithnodata parameter
SELECT ST_Asbinary(ST_Tile(ST_TestRaster(0, 0, 1), 10, 10, true)::geometry);

-- Display the result
SELECT ST_AsBinary((gv).geom) geom,  (gv).val
FROM (SELECT ST_PixelAsPolygons(ST_Tile(ST_TestRaster(0, 0, 1), 10, 10, true)) gv) foo;

-- Add a rid
SELECT ST_Asbinary(ST_Tile(ST_TestRaster(0, 0, 1), 10, 10)::geometry), generate_series(1, 35) rid;

SELECT ST_AsBinary((gv).geom) geom,  (gv).val, rid
FROM (SELECT ST_PixelAsPolygons(ST_Tile(ST_TestRaster(0, 0, 1), 10, 10, true)) gv, generate_series(1, 35) rid) foo;

SELECT ST_AsBinary((gv).geom) geom,  (gv).val, rid
FROM (SELECT ST_PixelAsPolygons(rast) gv, rid
      FROM (SELECT ST_Tile(ST_TestRaster(0, 0, 1), 10, 10, true) rast, generate_series(1, 35) rid) foo WHERE rid = 2) foo2;

-- Test ST_TileAsGeom
SELECT ST_TileAsGeom(ST_TestRaster(0, 0, 1), 10, 10);


-- Other tests
SELECT ST_Tile(ST_AddBand(ST_MakeEmptyRaster(48, 63, 0, 0, 0.001, -0.001, 0, 0, 4269), '8BSI'::text, 0, -2), 10, 10, true, -10);

SELECT ST_BandNodataValue(ST_Tile(ST_AddBand(ST_MakeEmptyRaster(48, 63, 0, 0, 0.001, -0.001, 0, 0, 4269), '8BSI'::text, 0, 200), 10, 10, true));

SELECT ST_BandNodataValue(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 0.001, -0.001, 0, 0, 4269), '8BSI'::text, 0, 128));

SELECT ST_BandNodataValue(ST_SetBandNodataValue(ST_AddBand(ST_MakeEmptyRaster(2, 2, 0, 0, 0.001, -0.001, 0, 0, 4269), '8BSI'::text, 0, 2), 200));
