----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------
-- NOTE: The ST_Histogram() function is already implemented in C. This plpgsql script is provided only as an example. 
-- Defining the plpgsql function below might overwrite the current C implementation and brake other functions dependent on it.
-- Use with caution.
----------------------------------------------------------------------
-- _ST_Values(rast raster, band int)
-- Return all rast pixels values which center are in a geometry
-- Values are returned as groups of identical adjacent values (value, count) 
-- in order to reduce the number of row returned.
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ST_Values(rast raster, band int, geom geometry, OUT val float8, OUT count int) 
    RETURNS SETOF record AS 
    $$
    DECLARE
        geomintersect geometry;
        m int[];
        x integer := 0;
        y integer := 0;
        curval float8;
    BEGIN
        m := ST_GeomExtent2RasterCoord(rast, geom);
        -- Get the intersection between with the geometry.
        geomintersect := ST_Intersection(geom, ST_ConvexHull(rast));

        -- If the intersection is empty, return false
        IF m[1] IS NULL AND m[2] IS NULL AND m[3] IS NULL AND m[4] IS NULL THEN
            RETURN;
        END IF;

        count := 0;
        val := ST_Value(rast, band, m[1], m[2]);
        FOR x IN m[1]..m[3] LOOP
            FOR y IN m[2]..m[4] LOOP
                -- Check first if the pixel intersects with the geometry. Many won't.
                IF ST_Intersects(geom, ST_Centroid(ST_PixelAsPolygon(rast, x, y))) THEN
                    curval = ST_Value(rast, band, x, y);
                    IF NOT curval IS NULL THEN
                        IF curval = val THEN
                            count := count + 1;
                        ELSE
                            RETURN NEXT;
                            val := curval;
                            count := 1;
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END LOOP;
        RETURN NEXT;
        RETURN;
    END;
    $$
    LANGUAGE 'plpgsql' IMMUTABLE STRICT;
   
----------------------------------------------------------------------
-- _ST_Values(rast raster, band int)
-- Return all rast pixels values
-- Values are returned as groups of identical adjacent values (value, count) 
-- in order to reduce the number of row returned.
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ST_Values(rast raster, band int, OUT val float8, OUT count int) 
    RETURNS SETOF record AS 
    $$
    DECLARE
        x int;
        y int;
        width int := ST_Width(rast);
        height int := ST_Height(rast);
        curval float8;
    BEGIN
        count := 0;
        val := ST_Value(rast, band, 1, 1);
        FOR x IN 1..width LOOP
            FOR y IN 1..height LOOP
                curval = ST_Value(rast, band, x, y);
                IF NOT curval IS NULL THEN
                    IF curval = val THEN
                        count := count + 1;
                    ELSE
                        RETURN NEXT;
                        val := curval;
                        count := 1;
                    END IF;
                END IF;
            END LOOP;
        END LOOP;
        RETURN NEXT;
        RETURN;
    END;
    $$
    LANGUAGE 'plpgsql';

----------------------------------------------------------------------
-- ST_Histogram(rast raster, band int) group
-- Return a set of (val, count) rows forming the value histogram for a raster
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_Histogram(rast raster, band int, OUT val double precision, OUT count bigint)
RETURNS SETOF record
    AS $$
    SELECT (vc).val val, sum((vc).count)::bigint count 
    FROM (SELECT _ST_Values($1, $2) vc) foo GROUP BY (vc).val;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION ST_Histogram(rast raster, OUT val double precision, OUT count bigint)
RETURNS SETOF record
    AS $$
    SELECT (vc).val val, sum((vc).count)::bigint count 
    FROM (SELECT _ST_Values($1, 1) vc) foo GROUP BY (vc).val;
    $$
    LANGUAGE SQL;

----------------------------------------------------------------------
-- ST_Histogram(rast raster, band int, geom geometry) group
-- Return a set of (val, count) rows forming the value histogram for the area of a raster covered by a polygon geometry. 
-- Pixels are selected only when their center intersects the polygon 
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_Histogram(rast raster, band int, geom geometry, OUT val double precision, OUT count bigint)
RETURNS SETOF record
    AS $$
    SELECT (vc).val val, sum((vc).count)::bigint count 
    FROM (SELECT _ST_Values($1, $2, $3) vc) foo GROUP BY (vc).val;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION ST_Histogram(rast raster, geom geometry, OUT val double precision, OUT count bigint)
RETURNS SETOF record
    AS $$
    SELECT (vc).val val, sum((vc).count)::bigint count 
    FROM (SELECT _ST_Values($1, 1, $2) vc) foo GROUP BY (vc).val;
    $$
    LANGUAGE SQL;

----------------------------------------------------------------------
-- This variant might be faster (not using an intermediate _ST_Values function)
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_Histogram2(rast raster, band int, OUT val double precision, OUT count bigint)
RETURNS SETOF record
    AS $$
    SELECT val, count(*) count 
    FROM (SELECT ST_Value($1, $2, x, y) val FROM generate_series(1, ST_Width($1)) x , generate_series(1, ST_Height($1)) y) foo 
    GROUP BY val;
    $$
    LANGUAGE SQL IMMUTABLE;

SELECT (hist).val val, sum((hist).count) count
FROM (SELECT ST_Histogram2(rast, 1) hist FROM srtm_22_03_tiled_10x10) foo
GROUP BY val
ORDER BY count DESC

----------------------------------------------------------------------
-- Other variant (not grouping in the function) (not using an intermediate _ST_Values function)
----------------------------------------------------------------------    
CREATE OR REPLACE FUNCTION ST_Histogram3(rast raster, band int, OUT val double precision)
RETURNS SETOF double precision
    AS $$
    SELECT ST_Value($1, $2, x, y) val FROM generate_series(1, ST_Width($1)) x , generate_series(1, ST_Height($1)) y;
    $$
    LANGUAGE SQL IMMUTABLE;

SELECT val, count(*) count
FROM (SELECT ST_Histogram3(rast, 1) val FROM srtm_22_03_tiled_10x10) foo
GROUP BY val
ORDER BY count DESC

SELECT ST_Histogram3(rast, 1) val, count(*) count FROM srtm_22_03_tiled_10x10
GROUP BY val
ORDER BY count DESC

----------------------------------------------------------------------
-- This might actually be the fasters query to get the histogram
----------------------------------------------------------------------    
SELECT val, count(*) count
FROM (SELECT ST_Value(rast, 1, x, y) val
      FROM generate_series(1, 10) x, generate_series(1, 10) y, srtm_22_03_tiled_10x10
     ) foo
GROUP BY val
ORDER BY count DESC

----------------------------------------------------------------------
-- Example 1: Query returning the histogram for a raster tile (one at a time)
----------------------------------------------------------------------

-------------------------------
-- The subquery gets the histogram for each tile
-- The main query split the resulting records in their two components (val & count)
-------------------------------
SELECT rid, 
       (hist).val, 
       (hist).count 
FROM (SELECT rid, 
             ST_Histogram(rast) hist
      FROM srtm_22_03_tiled_25x25
      WHERE rid = 234
     ) foo
ORDER BY (hist).count DESC

----------------------------------------------------------------------
-- Example 2: Query returning the complete histogram for a tiled raster coverage
-  (might be very long)
----------------------------------------------------------------------

-------------------------------
-- The subquery gets the histogram for each tile
-- The main query split the resulting records in their two 
-- components (val & count) and sum the count over all the tiles
-------------------------------
SELECT (hist).val, 
       SUM((hist).count) count
FROM (SELECT rid, 
             ST_Histogram(rast) hist
      FROM srtm_22_03_tiled_25x25
     ) foo
GROUP BY (hist).val
ORDER BY count DESC

----------------------------------------------------------------------
-- Example 3: Query returning the mean pixel value for each tile of a 
-- tiled raster (might be very long)
----------------------------------------------------------------------

-------------------------------
-- The subquery gets the histogram for each tile
-- The main query split the resulting records in their two 
-- components (val & count) computing a mean value per tile at the same time
-------------------------------
SELECT rid, 
       geom, 
       round(((SUM((hist).val * (hist).count)) / SUM((hist).count))::numeric, 2) meanval
FROM (SELECT rid, 
             rast::geometry geom, 
             ST_Histogram(rast) hist
      FROM srtm_22_03_tiled_25x25
     ) foo
GROUP BY rid, geom
ORDER BY rid;

----------------------------------------------------------------------
-- Example 4: Query returning the most frequent pixel value for each tile 
-- of a tiled raster (might be very long)
-- This example requires an aggregate function tracking the value 
-- associated with the maximum count
----------------------------------------------------------------------
CREATE TYPE dblIntSet AS (
    maxval double precision,
    val int
);

CREATE OR REPLACE FUNCTION maxFromDblIntSet(dblIntSet, dblIntSet) RETURNS dblIntSet AS
$$ SELECT CASE WHEN $1.maxval>$2.maxval THEN $1 ELSE $2 END $$
LANGUAGE sql;

CREATE OR REPLACE FUNCTION valFromDblIntSet(dblIntSet) RETURNS int AS
$$ SELECT $1.val $$
LANGUAGE sql;

CREATE AGGREGATE maxFromDblIntSet (
    BASETYPE = dblIntSet,
    SFUNC = maxFromDblIntSet,
    STYPE = dblIntSet,
    INITCOND = '(0,0)',
    FINALFUNC = valFromDblIntSet
);

-------------------------------
-- Actual query
-- The subquery gets the histogram for each tile
-- The main query split the resulting records in their two 
-- components (val & count) and compute the maximum count and its associated value
-------------------------------
SELECT rid, 
       geom, 
       maxFromDblIntSet(ROW((hist).count, (hist).val::int)) mostfreqval, 
       MAX((hist).count) count
FROM (SELECT rid, 
             rast::geometry geom, 
             ST_Histogram(rast) hist 
      FROM srtm_22_03_tiled_25x25
     ) foo
GROUP BY rid, geom
ORDER BY rid

----------------------------------------------------------------------
-- Example 5: Query returning the most frequent pixel value per polygon from a raster
-- Do not use when the raster is big, in this case it should be tiled and 
-- the next example (6) should be used instead
----------------------------------------------------------------------
SELECT polyid,
       geom, 
       maxFromDblIntSet(ROW((hist).count, (hist).val::int)) mostfreqval, 
       MAX((hist).count) count
FROM (
      SELECT polyid, 
             geom, 
             ST_Histogram(rast, geom) hist 
      FROM srtm_22_03, mypolygons
      WHERE ST_Intersects(rast, geom)
     ) foo
GROUP BY polyid, geom

----------------------------------------------------------------------
-- Example 6: Query returning the most frequent pixel value per polygon on a tiled raster coverage
----------------------------------------------------------------------

-------------------------------
-- The first subquery gets the histogram for each tile
-- The second subquery split the resulting records in their two 
-- components (val & count) and sum the count for each polygon-value couple
-- The main query compute the maximum count and its associated value
-------------------------------
SELECT polyid, 
       geom, 
       maxFromDblIntSet(ROW(count, val)) mostfreqval, 
       MAX(count) count
FROM (SELECT polyid, 
             geom, (hist).val::int val, 
             SUM((hist).count) count
      FROM (SELECT polyid, 
                   geom, 
                   ST_Histogram(rast, geom) hist 
            FROM srtm_22_03_tiled_25x25, mypolygons
            WHERE ST_Intersects(rast, geom)
           ) foo
      GROUP BY polyid, geom, (hist).val
     ) bar
GROUP BY polyid, geom

----------------------------------------------------------------------
-- Example 7: Query returning the mean pixel value per polygon on a tiled raster coverage
----------------------------------------------------------------------

-------------------------------
-- The first subquery gets the histogram for each tile
-- The second subquery split the resulting records in their two 
-- components (val & count) and sum the count for each polygon-value couple
-- The main query compute the mean pixel value
-------------------------------
SELECT polyid, 
       geom, 
       round((SUM(val * count) / SUM(count))::numeric, 2) meanval
FROM (SELECT polyid, 
             geom, (hist).val::int val, 
             SUM((hist).count) count
      FROM (SELECT polyid, 
                   geom, 
                   ST_Histogram(rast, geom) hist 
            FROM srtm_22_03_tiled_25x25, mypolygons
            WHERE ST_Intersects(rast, geom)
           ) foo
      GROUP BY polyid, geom, (hist).val
     ) bar
GROUP BY polyid, geom