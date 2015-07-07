-----------------------------------------------------------------------
-- Complex type geomvalxy for returning the geometry, the value, the x coordinate and the y coordinate of a pixel
-----------------------------------------------------------------------
CREATE TYPE geomvalxy AS (
    geom geometry,
    val double precision,
    x int,
    y int
);
-----------------------------------------------------------------------
-- ST_PixelAsPoints
-- Return all the pixels of a raster as a record composed of a point geometry, a value, a x and a y raster coordinate. 
-- Should be called like this:
-- SELECT (gv).geom, (gv).val, (gv).x, (gv).y FROM (SELECT ST_PixelAsPoints(rast) gv FROM mytable) foo
-----------------------------------------------------------------------
DROP FUNCTION IF EXISTS ST_PixelAsPoints(rast raster, band integer);
CREATE OR REPLACE FUNCTION ST_PixelAsPoints(rast raster, band integer) 
    RETURNS SETOF geomvalxy AS 
    $$
    DECLARE
        rast alias for $1;
        w integer;
        h integer;
        x integer;
        y integer;
        result geomvalxy;
    BEGIN
        SELECT st_width(rast), st_height(rast)
        INTO w, h;
        FOR x IN 1..w LOOP
             FOR y IN 1..h LOOP
                 SELECT ST_Centroid(ST_PixelAsPolygon(rast, x, y)), ST_Value(rast, band, x, y), x, y INTO result;
            RETURN NEXT result;
         END LOOP;
        END LOOP;
        RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

DROP FUNCTION IF EXISTS ST_PixelAsPoints(rast raster);
CREATE FUNCTION ST_PixelAsPoints(raster) RETURNS SETOF geomvalxy AS 
$$
    SELECT ST_PixelAsPoints($1, 1);
$$
LANGUAGE SQL;