----------------------------------------------------------------------------------------------------------------------
-- Create an index raster. Georeference is borrowed from the provided raster.
-- pixeltype  - The pixeltype of the resulting raster
-- startvalue - The first index value assigned to the raster. Default to 0.
-- incwithx   - If true, the index increments when the x position of the pixel increments. 
--              The index decrement on x otherwise. Default to true.
-- incwithy   - If true, the index increments when the y position of the pixel increments. 
--              The index decrement on y otherwise. Default to true.
-- columnfirst  - If true, columns are traversed first. 
--                Rows are traversed first otherwise. Default to true.
-- rowscanorder - If true, the raster is traversed in "row scan" order. 
--                Row prime order (Boustrophedon) is used otherwise. Default to true.
-- falsecolinc  - Overwrite the column index increment which is normally equal to ST_Height()
-- falserowinc  - Overwrite the row index increment which is normally equal to ST_Width()
----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ST_CreateIndexRaster(rast raster, 
                                                pixeltype text DEFAULT '32BUI', 
                                                startvalue int DEFAULT 0, 
                                                incwithx boolean DEFAULT true, 
                                                incwithy boolean DEFAULT true,
                                                columnfirst boolean DEFAULT true,
                                                rowscanorder boolean DEFAULT true,
                                                falsecolinc int DEFAULT NULL,
                                                falserowinc int DEFAULT NULL)
RETURNS raster AS
$BODY$
DECLARE
    newraster raster := ST_AddBand(ST_MakeEmptyRaster(rast), pixeltype);
    x int;
    y int;
    w int := ST_Width(newraster);
    h int := ST_Height(newraster);
    rowincx int := Coalesce(falserowinc, w);
    colincx int := Coalesce(falsecolinc, h);
    rowincy int := Coalesce(falserowinc, 1);
    colincy int := Coalesce(falsecolinc, 1);
    xdir int := CASE WHEN Coalesce(incwithx, true) THEN 1 ELSE w END;
    ydir int := CASE WHEN Coalesce(incwithy, true) THEN 1 ELSE h END;
    xdflag int := Coalesce(incwithx::int, 1);
    ydflag int := Coalesce(incwithy::int, 1);
    rsflag int := Coalesce(rowscanorder::int, 1);
    newstartvalue int := Coalesce(startvalue, 0);
    newcolumnfirst boolean := Coalesce(columnfirst, true);
BEGIN
    IF newcolumnfirst THEN
        IF colincx <= (h - 1) * rowincy THEN
            RAISE EXCEPTION 'Column increment (now %) must be greater than the number of index on one column (now % pixel x % = %)...', colincx, h - 1, rowincy, (h - 1) * rowincy;
        END IF;
        newraster = ST_SetBandNodataValue(ST_MapAlgebraExpr(newraster, pixeltype, 'abs([rast.x] - ' || xdir::text || ') * ' || colincx::text || ' + abs([rast.y] - (' || h::text || ' ^ ((abs([rast.x] - ' || xdir::text || ' + 1) % 2) | ' || rsflag::text || ' # ' || ydflag::text || '))::int) * ' || rowincy::text || ' + ' || newstartvalue::text), ST_BandNodataValue(newraster));
    ELSE
        IF rowincx <= (w - 1) * colincy THEN
            RAISE EXCEPTION 'Row increment (now %) must be greater than the number of index on one row (now % pixel x % = %)...', rowincx, w - 1, colincy, (w - 1) * colincy;
        END IF;
        newraster = ST_SetBandNodataValue(ST_MapAlgebraExpr(newraster, pixeltype, 'abs([rast.x] - (' || w::text || ' ^ ((abs([rast.y] - ' || ydir::text || ' + 1) % 2) | ' || rsflag::text || ' # ' || xdflag::text || '))::int) * ' || colincy::text || ' + abs([rast.y] - ' || ydir::text || ') * ' || rowincx::text || ' + ' || newstartvalue::text), ST_BandNodataValue(newraster));
    END IF;    
    RETURN newraster;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;

-- test
SELECT ST_AsBinary((gvxy).geom), (gvxy).val, (gvxy).x, (gvxy).y
FROM (SELECT ST_PixelAsPolygons(ST_CreateIndexRaster(ST_MakeEmptyRaster(2, 2, 0, 0, 1), '32BSI', null, null, null, null, null, 3, null)) gvxy) foo

SELECT Coalesce(null::int, 2);