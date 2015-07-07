----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- NOTE: The ST_AsRaster() function is already implemented in C. This plpgsql script is provided only as an example. 
-- Defining the plpgsql function below might overwrite the current C implementation and brake other functions dependent on it.
-- Use with caution.

CREATE OR REPLACE FUNCTION ST_AsRaster(geom geometry, rast raster, pixeltype text, nodatavalue float8, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    numband int := ST_NumBands(rast);
        x1w float8 := ST_XMin(geom);
        y1w float8 := ST_YMax(geom);
        x2w float8 := ST_XMax(geom);
        y2w float8 := ST_YMin(geom);
        x1r int := ST_World2RasterCoordX(rast, x1w, y1w);
        y1r int := ST_World2RasterCoordY(rast, x1w, y1w);
        x2r int := ST_World2RasterCoordX(rast, x2w, y2w);
        y2r int := ST_World2RasterCoordY(rast, x2w, y2w);
        newx1w float8 := ST_Raster2WorldCoordX(rast, x1r, y1r);
        newy1w float8 := ST_Raster2WorldCoordY(rast, x1r, y1r);
        newwidth int := abs(x2r - x1r) + 1;
        newheight int := abs(y2r - y1r) + 1;
        newrast raster := ST_AddBand(ST_MakeEmptyRaster(newwidth, newheight, newx1w, newy1w, ST_ScaleX(rast), ST_ScaleY(rast), ST_SkewX(rast), ST_SkewY(rast), ST_SRID(rast)), pixeltype, nodatavalue, nodatavalue);
    BEGIN
    FOR x IN 1..newwidth LOOP
        FOR y IN 1..newheight LOOP
            IF ST_Intersects(geom, ST_Centroid(ST_PixelAsPolygon(newrast, x, y))) THEN
                newrast := ST_SetValue(newrast, 1, x, y, val);
            END IF;
        END LOOP;
    END LOOP;
    RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';

-- Test

SELECT (gv).geom, (gv).val
FROM (SELECT (ST_PixelAsPolygons(rast, 1)) gv
FROM (SELECT ST_AsRaster(the_geom, (SELECT rast FROM srtm_tiled_100x100 LIMIT 1), '8BSI', -1, 1) rast
FROM realcaribou_buffers_wgs
LIMIT 10) foo) foi
