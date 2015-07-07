----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- NOTE: This function is provided merely as an example since a C version was implemented and is now provided in rtpostgis.sql

CREATE OR REPLACE FUNCTION ST_AddBand(rast1 raster, rast2 raster, band int, index int)
    RETURNS raster AS 
    $$
    DECLARE
        newraster raster;
        newnodatavalue int;
        newindex int := index;
        newband int := band;
        x int;
        y int;
    BEGIN
        IF ST_Width(rast1) != ST_Width(rast2) OR ST_Height(rast1) != ST_Height(rast2) THEN
            RAISE EXCEPTION 'ST_AddBand: Attempting to add a band with different width or height';
        END IF;
        IF newindex < 1 THEN
            newindex := 1;
        END IF;
        IF newindex IS NULL OR newindex > ST_NumBands(rast1) THEN
            newindex := ST_NumBands(rast1) + 1;
        END IF;
        IF newband < 1 THEN
            newband := 1;
        END IF;
        IF newband IS NULL OR newband > ST_NumBands(rast2) THEN
            newband := ST_NumBands(rast2);
        END IF;

        IF newband = 0 THEN
            RETURN rast1;
        END IF;
        newnodatavalue := ST_BandNodataValue(rast2, newband);
        newraster := ST_AddBand(rast1, newindex, ST_BandPixelType(rast2, newband), newnodatavalue, newnodatavalue);
        FOR x IN 1..ST_Width(rast2) LOOP
            FOR y IN 1..ST_Height(rast2) LOOP
                newraster := ST_SetValue(newraster, newindex, x, y, ST_Value(rast2, newband, x, y));
            END LOOP;
        END LOOP;
        RETURN newraster;
    END;
    $$
    LANGUAGE 'plpgsql';

--Test
SELECT ST_NumBands(ST_AddBand(ST_TestRaster(0, 0, 1), ST_TestRaster(0, 0, 1), 1, 2))