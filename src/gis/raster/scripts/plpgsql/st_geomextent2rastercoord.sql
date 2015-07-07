DROP FUNCTION ST_GeomExtent2RasterCoord(rast raster, geomin geometry);
CREATE OR REPLACE FUNCTION ST_GeomExtent2RasterCoord(rast raster, 
							geomin geometry)
    RETURNS int[] AS
    $$
    DECLARE
        geomintersect geometry;
        x1w double precision := 0.0;
        x2w double precision := 0.0;
        y1w double precision := 0.0;
        y2w double precision := 0.0;
        x1 integer := 0;
        x2 integer := 0;
        x3 integer := 0;
        x4 integer := 0;
        y1 integer := 0;
        y2 integer := 0;
        y3 integer := 0;
        y4 integer := 0;
        psize float8;
    BEGIN

        -- Get the intersection between with the geometry.
        -- We will search for withvalue pixel only in this area.
        geomintersect := st_intersection(geomin, st_convexhull(rast));
--RAISE NOTICE 'geomintersect1=%', astext(geomintersect);

        -- If the intersection is empty, return false
        IF st_isempty(geomintersect) THEN
            RETURN ARRAY[NULL, NULL, NULL, NULL];
        END IF;

        -- We create a minimalistic buffer around the intersection in order to scan every pixels
        -- that would touch the edge or intersect with the geometry
        psize := st_scalex(rast) + st_skewy(rast);
        geomintersect := st_buffer(geomintersect, psize / 1000000);

--RAISE NOTICE 'geomintersect2=%', astext(geomintersect);

        -- Find the world coordinates of the bounding box of the intersecting area
        x1w := st_xmin(geomintersect);
        y1w := st_ymin(geomintersect);
        x2w := st_xmax(geomintersect);
        y2w := st_ymax(geomintersect);

--RAISE NOTICE 'x1w=%, y1w=%, x2w=%, y2w=%', x1w, y1w, x2w, y2w;

        -- Convert world coordinates to raster coordinates
        x1 := st_world2rastercoordx(rast, x1w, y1w);
        y1 := st_world2rastercoordy(rast, x1w, y1w);
        x2 := st_world2rastercoordx(rast, x2w, y1w);
        y2 := st_world2rastercoordy(rast, x2w, y1w);
        x3 := st_world2rastercoordx(rast, x1w, y2w);
        y3 := st_world2rastercoordy(rast, x1w, y2w);
        x4 := st_world2rastercoordx(rast, x2w, y2w);
        y4 := st_world2rastercoordy(rast, x2w, y2w);

--RAISE NOTICE 'x1=%, y1=%, x2=%, y2=%, x3=%, y3=%, x4=%, y4=%', x1, y1, x2, y2, x3, y3, x4, y4;

        -- Order the raster coordinates for the upcoming FOR loop.
        x1 := int4smaller(int4smaller(int4smaller(x1, x2), x3), x4);
        y1 := int4smaller(int4smaller(int4smaller(y1, y2), y3), y4);
        x2 := int4larger(int4larger(int4larger(x1, x2), x3), x4);
        y2 := int4larger(int4larger(int4larger(y1, y2), y3), y4);

        -- Make sure the range is not lower than 1.
        -- This can happen when world coordinate are exactly on the left border
        -- of the raster and that they do not span on more than one pixel.
        x1 := int4smaller(int4larger(x1, 1), st_width(rast));
        y1 := int4smaller(int4larger(y1, 1), st_height(rast));

        -- Also make sure the range does not exceed the width and height of the raster.
        -- This can happen when world coordinate are exactly on the lower right border
        -- of the raster.
        x2 := int4smaller(x2, st_width(rast));
        y2 := int4smaller(y2, st_height(rast));
        
        RETURN ARRAY[x1, y1, x2, y2];

    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE STRICT;

--Test
SELECT (gvxy).geom, (gvxy).x::text || ',' || (gvxy).y::text
FROM (SELECT DISTINCT rid, ST_PixelAsPolygons(rast) gvxy
      FROM srtm_22_03_tiled, rect3
      WHERE st_intersects(rast, geom)
     ) foo

SELECT DISTINCT rid
FROM srtm_22_03_tiled, rect3
WHERE st_intersects(rast, geom)


SELECT rid, id, ST_GeomExtent2RasterCoord(rast, geom) 
FROM srtm_22_03_tiled, rect3
WHERE st_intersects(rast, geom)


SELECT (ext).x1, (ext).y1, (ext).x2, (ext).y2 FROM (SELECT ST_GeomExtent2RasterCoord(rast, geom) ext FROM srtm_22_03_tiled, rect3
WHERE st_intersects(rast, geom)) foo