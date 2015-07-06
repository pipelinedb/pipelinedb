---------------------------------------------------------------------
-- ST_SummaryStatsAgg AGGREGATE
-- Compute summary statistics for an aggregation of raster.
--
-- Exemple
-- SELECT (ss).count, 
--        (ss).sum, 
--        (ss).mean, 
--        (ss).min, 
--        (ss).max
-- FROM (SELECT ST_SummaryStatsAgg(gv) ss
--       FROM (SELECT ST_Clip(rt.rast, gt.geom) gv
--             FROM rasttable rt, geomtable gt
--             WHERE ST_Intersects(rt.rast, gt.geom)
--            ) foo
--       GROUP BY gt.id
--      ) foo2
---------------------------------------------------------------------
-- raster_summarystatsstate
-- State function used by the ST_SummaryStatsAgg aggregate
CREATE OR REPLACE FUNCTION raster_summarystatsstate(ss summarystats, rast raster, nband int DEFAULT 1, exclude_nodata_value boolean DEFAULT TRUE, sample_percent double precision DEFAULT 1)
    RETURNS summarystats 
    AS $$
    DECLARE
        newstats summarystats;
        ret summarystats;
    BEGIN
        IF rast IS NULL OR ST_HasNoBand(rast) OR ST_IsEmpty(rast) THEN
            RETURN ss;
        END IF;
        newstats := _ST_SummaryStats(rast, nband, exclude_nodata_value, sample_percent);
        IF $1 IS NULL THEN
            ret := (newstats.count, 
                    newstats.sum,
                    null,
                    null,
                    newstats.min, 
                    newstats.max)::summarystats;
        ELSE
            ret := (COALESCE(ss.count,0) + COALESCE(newstats.count, 0),
                    COALESCE(ss.sum,0) + COALESCE(newstats.sum, 0),
                    null,
                    null,
                    least(ss.min, newstats.min), 
                    greatest(ss.max, newstats.max))::summarystats;
        END IF;
        RETURN ret;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION raster_summarystatsstate(ss summarystats, rast raster)
    RETURNS summarystats 
    AS $$
        SELECT raster_summarystatsstate($1, $2, 1, true, 1);
    $$ LANGUAGE 'SQL';

---------------------------------------------------------------------
-- raster_summarystatsfinal
-- Final function used by the ST_SummaryStatsAgg aggregate 
CREATE OR REPLACE FUNCTION raster_summarystatsfinal(ss summarystats)
    RETURNS summarystats 
    AS $$
    DECLARE
        ret summarystats;
    BEGIN
        ret := (($1).count,
                ($1).sum,
                CASE WHEN ($1).count = 0 THEN null ELSE ($1).sum / ($1).count END,
                null,
                ($1).min,
                ($1).max
               )::summarystats;
        RETURN ret;
    END;
    $$
    LANGUAGE 'plpgsql';

---------------------------------------------------------------------
-- ST_SummaryStatsAgg AGGREGATE
---------------------------------------------------------------------
CREATE AGGREGATE ST_SummaryStatsAgg(raster, int, boolean, double precision) (
  SFUNC=raster_summarystatsstate,
  STYPE=summarystats,
  FINALFUNC=raster_summarystatsfinal
);

CREATE AGGREGATE ST_SummaryStatsAgg(raster) (
  SFUNC=raster_summarystatsstate,
  STYPE=summarystats,
  FINALFUNC=raster_summarystatsfinal
);

-- Test
CREATE OR REPLACE FUNCTION ST_TestRaster(h integer, w integer, val float8) 
    RETURNS raster AS 
    $$
    DECLARE
    BEGIN
        RETURN ST_AddBand(ST_MakeEmptyRaster(h, w, 0, 0, 1, 1, 0, 0, -1), '32BF', val, -1);
    END;
    $$
    LANGUAGE 'plpgsql';

SELECT id,
       (ss).count, 
       (ss).sum, 
       (ss).mean, 
       (ss).stddev, 
       (ss).min, 
       (ss).max
FROM (SELECT ST_SummaryStatsAgg(rast) as ss, id
      FROM (SELECT 1 id, ST_TestRaster(2, 2, 2) rast
            UNION ALL
            SELECT 1 id, ST_TestRaster(2, 2, 4) rast
            UNION ALL
            SELECT 2 id, ST_TestRaster(2, 2, 4) rast
           ) foo
      GROUP BY id
     ) foo2

