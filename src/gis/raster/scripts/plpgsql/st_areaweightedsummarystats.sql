---------------------------------------------------------------------
-- ST_AreaWeightedSummaryStats AGGREGATE
-- Compute statistics of a value weighted by the area of the corresponding geometry.
-- Specially written to be used with ST_Intersection(raster, geometry)
--
-- Exemple
-- SELECT gt.id,
--        (aws).count, 
--        (aws).distinctcount,
--        (aws).geom, 
--        (aws).totalarea, 
--        (aws).meanarea, 
--        (aws).totalperimeter, 
--        (aws).meanperimeter, 
--        (aws).weightedsum, 
--        (aws).weightedmean, 
--        (aws).maxareavalue, 
--        (aws).minareavalue, 
--        (aws).maxcombinedareavalue, 
--        (aws).mincombinedareavalue, 
--        (aws).sum, 
--        (aws).mean, 
--        (aws).max, 
--        (aws).min
-- FROM (SELECT ST_AreaWeightedSummaryStats(gv) aws
--       FROM (SELECT ST_Intersection(rt.rast, gt.geom) gv
--             FROM rasttable rt, geomtable gt
--             WHERE ST_Intersects(rt.rast, gt.geom)
--            ) foo
--       GROUP BY gt.id
--      ) foo2
---------------------------------------------------------------------

--DROP TYPE arealweightedstats CASCADE;
CREATE TYPE arealweightedstats AS (
    count int,
    distinctcount int,
    geom geometry,
    totalarea double precision,
    meanarea double precision,
    totalperimeter double precision,
    meanperimeter double precision,
    weightedsum double precision,
    weightedmean double precision,
    maxareavalue double precision,
    minareavalue double precision,
    maxcombinedareavalue double precision, 
    mincombinedareavalue double precision, 
    sum double precision, 
    mean double precision, 
    max  double precision, 
    min double precision
);

-- DROP TYPE arealweightedstatsstate CASCADE;
CREATE TYPE arealweightedstatsstate AS (
    count int,
    distinctvalues double precision[],
    unionedgeom geometry,
    totalarea double precision,
    totalperimeter double precision,
    weightedsum double precision,
    maxareavalue double precision[],
    minareavalue double precision[],
    combinedweightedareas double precision[], 
    sum double precision, 
    max double precision, 
    min double precision
);

---------------------------------------------------------------------
-- geomval_arealweightedstate
-- State function used by the ST_AreaWeightedSummaryStats aggregate
CREATE OR REPLACE FUNCTION geomval_arealweightedstate(aws arealweightedstatsstate, gv geomval)
    RETURNS arealweightedstatsstate 
    AS $$
    DECLARE
        i int;
        ret arealweightedstatsstate;
        newcombinedweightedareas double precision[] := ($1).combinedweightedareas;
        newgeom geometry := ($2).geom;
        geomtype text := GeometryType(($2).geom);
    BEGIN
        IF geomtype = 'GEOMETRYCOLLECTION' THEN 
            newgeom := ST_CollectionExtract(newgeom, 3);
        END IF;
        IF newgeom IS NULL OR ST_IsEmpty(newgeom) OR geomtype = 'POINT' OR geomtype = 'LINESTRING' OR geomtype = 'MULTIPOINT' OR geomtype = 'MULTILINESTRING' THEN 
            ret := aws;
        ELSEIF $1 IS NULL THEN 
            ret := (1, 
                    ARRAY[($2).val], 
                    newgeom,
                    ST_Area(newgeom),
                    ST_Perimeter(newgeom),
                    ($2).val * ST_Area(newgeom), 
                    ARRAY[ST_Area(newgeom), ($2).val], 
                    ARRAY[ST_Area(newgeom), ($2).val], 
                    ARRAY[ST_Area(newgeom)], 
                    ($2).val, 
                    ($2).val, 
                    ($2).val 
                   )::arealweightedstatsstate;
        ELSE
            -- Search for the new value in the array of distinct values
            SELECT n FROM generate_series(1, array_length(($1).distinctvalues, 1)) n WHERE (($1).distinctvalues)[n] = ($2).val INTO i;
RAISE NOTICE 'i=% ',i;            
            -- If the value already exists, increment the corresponding area with the new area
            IF NOT i IS NULL THEN
                newcombinedweightedareas[i] := newcombinedweightedareas[i] + ST_Area(newgeom);
            END IF;
            ret := (($1).count + 1, 
                    CASE WHEN i IS NULL THEN array_append(($1).distinctvalues, ($2).val) ELSE ($1).distinctvalues END, 
                    ST_Union(($1).unionedgeom, newgeom),
                    ($1).totalarea + ST_Area(newgeom),
                    ($1).totalperimeter + ST_Perimeter(newgeom),
                    ($1).weightedsum + ($2).val * ST_Area(newgeom),
                    CASE WHEN ST_Area(newgeom) > (($1).maxareavalue)[1] THEN ARRAY[ST_Area(newgeom), ($2).val] ELSE ($1).maxareavalue END,
                    CASE WHEN ST_Area(newgeom) < (($1).minareavalue)[1] THEN ARRAY[ST_Area(newgeom), ($2).val] ELSE ($1).minareavalue END,
                    CASE WHEN i IS NULL THEN array_append(($1).combinedweightedareas, ST_Area(newgeom)) ELSE ($1).combinedweightedareas END,
                    ($1).sum + ($2).val,
                    greatest(($1).max, ($2).val),
                    least(($1).min, ($2).val)
                   )::arealweightedstatsstate;
        END IF;
        RETURN ret;
    END;
    $$
    LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION geomval_arealweightedstate(aws arealweightedstatsstate, geom geometry, val double precision)
    RETURNS arealweightedstatsstate 
    AS $$
        SELECT geomval_arealweightedstate($1, ($2, $3)::geomval);
    $$ LANGUAGE 'SQL';

---------------------------------------------------------------------
-- geomval_arealweightedfinal
-- Final function used by the ST_AreaWeightedSummaryStats aggregate 
CREATE OR REPLACE FUNCTION geomval_arealweightedfinal(aws arealweightedstatsstate)
    RETURNS arealweightedstats 
    AS $$
    DECLARE
        a RECORD;
        maxarea double precision = 0.0;
        minarea double precision = (($1).combinedweightedareas)[1];
        imax int := 1;
        imin int := 1;
        ret arealweightedstats;
    BEGIN
        -- Search for the max and the min areas in the array of all distinct values
        FOR a IN SELECT n, (($1).combinedweightedareas)[n] warea FROM generate_series(1, array_length(($1).combinedweightedareas, 1)) n LOOP
            IF a.warea > maxarea THEN
                imax := a.n;
                maxarea = a.warea;
            END IF;
            IF a.warea < minarea THEN
                imin := a.n;
                minarea = a.warea;
            END IF;    
        END LOOP;

        ret := (($1).count,
                array_length(($1).distinctvalues, 1),
                ($1).unionedgeom,
                ($1).totalarea,
                ($1).totalarea / ($1).count,
                ($1).totalperimeter,
                ($1).totalperimeter / ($1).count,
                ($1).weightedsum,
                ($1).weightedsum / ($1).totalarea,
                (($1).maxareavalue)[2],
                (($1).minareavalue)[2],
                (($1).distinctvalues)[imax],
                (($1).distinctvalues)[imin],
                ($1).sum,
                ($1).sum / ($1).count, 
                ($1).max,
                ($1).min
               )::arealweightedstats;
        RETURN ret;
    END;
    $$
    LANGUAGE 'plpgsql';

---------------------------------------------------------------------
-- ST_AreaWeightedSummaryStats AGGREGATE
---------------------------------------------------------------------
CREATE AGGREGATE ST_AreaWeightedSummaryStats(geomval) (
  SFUNC=geomval_arealweightedstate,
  STYPE=arealweightedstatsstate,
  FINALFUNC=geomval_arealweightedfinal
);

---------------------------------------------------------------------
-- ST_AreaWeightedSummaryStats AGGREGATE
---------------------------------------------------------------------
CREATE AGGREGATE ST_AreaWeightedSummaryStats(geometry, double precision) (
  SFUNC=geomval_arealweightedstate,
  STYPE=arealweightedstatsstate,
  FINALFUNC=geomval_arealweightedfinal
);


SELECT id,
       (aws).count, 
       (aws).distinctcount,
       (aws).geom, 
       (aws).totalarea, 
       (aws).meanarea, 
       (aws).totalperimeter, 
       (aws).meanperimeter, 
       (aws).weightedsum, 
       (aws).weightedmean, 
       (aws).maxareavalue, 
       (aws).minareavalue, 
       (aws).maxcombinedareavalue, 
       (aws).mincombinedareavalue, 
       (aws).sum, 
       (aws).mean, 
       (aws).max, 
       (aws).min
FROM (SELECT ST_AreaWeightedSummaryStats((geom, weight)::geomval) as aws, id
      FROM (SELECT ST_GeomFromEWKT('SRID=4269;POLYGON((0 0,0 10, 10 10, 10 0, 0 0))') as geom, 'a' as id, 100 as weight
            UNION ALL
            SELECT ST_GeomFromEWKT('SRID=4269;POLYGON((12 0,12 1, 13 1, 13 0, 12 0))') as geom, 'a' as id, 1 as weight
            UNION ALL
            SELECT ST_GeomFromEWKT('SRID=4269;POLYGON((10 0, 10 2, 12 2, 12 0, 10 0))') as geom, 'b' as id, 4 as weight
           ) foo
      GROUP BY id
     ) foo2

