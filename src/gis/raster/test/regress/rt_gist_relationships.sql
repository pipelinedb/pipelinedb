-----------------------------------------------------------------------
--
-- Copyright (c) 2009 Sandro Santilli <strk@keybit.net>
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software Foundation,
-- Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
-----------------------------------------------------------------------

CREATE TYPE tile AS (x int, y int, tile raster);
CREATE OR REPLACE FUNCTION makegrid (int, int, box2d, int, int)
    RETURNS SETOF tile
AS
'
DECLARE
    gridCols alias for $1;
    gridRows alias for $2;
    extent alias for $3;
    tileWidth alias for $4;
    tileHeight alias for $5;
    rec tile;
    scalex float8;
    scaley float8;
    ipx float8;
    ipy float8;
BEGIN
	
    -- compute some sizes
    -- each tile extent width is extent.width / gridRows
    scalex = ((ST_xmax(extent)-ST_xmin(extent))/gridCols)/tileWidth;
    scaley = ((ST_ymax(extent)-ST_ymin(extent))/gridRows)/tileHeight;

    FOR y IN 0..gridRows-1 LOOP
        ipy = y*scaley + ST_ymin(extent);
        FOR x IN 0..gridCols-1 LOOP
            ipx = x*scalex + ST_xmin(extent);
            rec.x = x;
            rec.y = y;
            rec.tile = st_MakeEmptyRaster(tileWidth, tileHeight, ipx, ipy,
                                          scalex, scaley, 0, 0);
            RETURN NEXT rec;
        END LOOP;
    END LOOP;

    RETURN;
END;
'
LANGUAGE 'plpgsql';

CREATE TABLE rt_gist_grid_test AS 
    SELECT * FROM makegrid(10, 10, 'BOX(-100 -100, 100 100)', 1, 1);

CREATE TABLE rt_gist_query_test AS
    SELECT * from makegrid(3, 3, 'BOX(-100 -100, 100 100)', 1, 1);

-----------------------------------------------------------------------
-- raster_above
-----------------------------------------------------------------------
SELECT 'raster_above(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_above(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test |>> above
-----------------------------------------------------------------------

SELECT 'X |>> query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile |>> b.tile;

-----------------------------------------------------------------------
-- raster_below
-----------------------------------------------------------------------

SELECT 'raster_below(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_below(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test <<| operator (below)
-----------------------------------------------------------------------

SELECT 'X <<| query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile <<| b.tile;

-----------------------------------------------------------------------
-- raster_contained
-----------------------------------------------------------------------

SELECT 'raster_contained(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_contained(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test @ operator (contained by)
-----------------------------------------------------------------------

SELECT 'X @ query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile @ b.tile;

-----------------------------------------------------------------------
-- raster_contain
-----------------------------------------------------------------------

SELECT 'raster_contain(query(1,1), X)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_contain(b.tile, a.tile);

-----------------------------------------------------------------------
-- raster_geometry_contain
-----------------------------------------------------------------------

SELECT 'raster_geometry_contain(query(1,1), X)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_geometry_contain(b.tile, a.tile::geometry);

-----------------------------------------------------------------------
-- geometry_raster_contain
-----------------------------------------------------------------------

SELECT 'geometry_raster_contain(query(1,1), X)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND geometry_raster_contain(b.tile::geometry, a.tile);

-----------------------------------------------------------------------
-- Test ~ operator (raster contains raster)
-----------------------------------------------------------------------

SELECT 'query(1,1) ~ X' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND b.tile ~ a.tile;

-----------------------------------------------------------------------
-- Test ~ operator (raster contains geometry)
-----------------------------------------------------------------------

SELECT 'query(1,1) ~ X' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND b.tile ~ a.tile::geometry;

-----------------------------------------------------------------------
-- Test ~ operator (geometry contains raster )
-----------------------------------------------------------------------

SELECT 'query(1,1) ~ X' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND b.tile::geometry ~ a.tile;

-----------------------------------------------------------------------
-- raster_left
-----------------------------------------------------------------------

SELECT 'raster_left(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_left(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test << operator (left)
-----------------------------------------------------------------------

SELECT 'X << query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile << b.tile;

-----------------------------------------------------------------------
-- raster_overabove
-----------------------------------------------------------------------

SELECT 'raster_overabove(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_overabove(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test |&> operator (overabove)
-----------------------------------------------------------------------

SELECT 'X |&> query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile |&> b.tile;

-----------------------------------------------------------------------
-- raster_overbelow
-----------------------------------------------------------------------

SELECT 'raster_overbelow(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_overbelow(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test &<| operator (overbelow)
-----------------------------------------------------------------------

SELECT 'X &<| query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile &<| b.tile;

-----------------------------------------------------------------------
-- raster_overlap
-----------------------------------------------------------------------

SELECT 'raster_overlap(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_overlap(a.tile, b.tile);

-----------------------------------------------------------------------
-- raster_geometry_overlap
-----------------------------------------------------------------------

SELECT 'raster_geometry_overlap(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_geometry_overlap(a.tile, b.tile::geometry);

-----------------------------------------------------------------------
-- geometry_raster_overlap
-----------------------------------------------------------------------

SELECT 'geometry_raster_overlap(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND geometry_raster_overlap(a.tile::geometry, b.tile);

-----------------------------------------------------------------------
-- Test && operator (overlap)
-----------------------------------------------------------------------

SELECT 'X && query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile && b.tile;

-----------------------------------------------------------------------
-- Test && operator (raster overlap geometry)
-----------------------------------------------------------------------

SELECT 'X && query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile && b.tile::geometry;

-----------------------------------------------------------------------
-- Test && operator (geometry overlap raster)
-----------------------------------------------------------------------

SELECT 'X && query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile::geometry && b.tile;

-----------------------------------------------------------------------
-- raster_overleft
-----------------------------------------------------------------------

SELECT 'raster_overleft(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_overleft(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test &< operator (overleft)
-----------------------------------------------------------------------

SELECT 'X &< query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile &< b.tile;

-----------------------------------------------------------------------
-- raster_overright
-----------------------------------------------------------------------
SELECT 'raster_overright(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_overright(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test &> operator (overright)
-----------------------------------------------------------------------

SELECT 'X &> query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile &> b.tile;

-----------------------------------------------------------------------
-- raster_right
-----------------------------------------------------------------------

SELECT 'raster_right(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_right(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test >> operator (right)
-----------------------------------------------------------------------

SELECT 'X >> query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile >> b.tile;

-----------------------------------------------------------------------
-- raster_same
-----------------------------------------------------------------------

SELECT 'raster_same(X, query(1,1))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND raster_same(a.tile, b.tile);

SELECT 'raster_same(X, query(7,7))' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_grid_test b
WHERE b.x = 7 and b.y = 7
    AND raster_same(a.tile, b.tile);

-----------------------------------------------------------------------
-- Test ~= operator (same)
-----------------------------------------------------------------------

SELECT 'X ~= query(1,1)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_query_test b
WHERE b.x = 1 and b.y = 1
    AND a.tile ~= b.tile;

SELECT 'X ~= tile(7,7)' as op,
        count(a.y),
        min(a.x) as xmin,
        max(a.x) as xmax,
        min(a.y) as ymin,
        max(a.y) as ymax,
        st_extent(a.tile::geometry)
FROM rt_gist_grid_test a, rt_gist_grid_test b
WHERE b.x = 7 and b.y = 7
    AND a.tile ~= b.tile;

DROP FUNCTION makegrid(integer,integer,box2d,integer,integer);
DROP table rt_gist_grid_test;
DROP table rt_gist_query_test;
DROP type tile;
