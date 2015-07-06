----------------------------------------------------------------------
--
--
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
--
----------------------------------------------------------------------

-- NOTE: The ST_DeleteBand function found in this file still need enhancement before being implemented in C.


-- NOTE: ST_DeleteBand(rast raster, band int) is dependent on
--   ST_AddBand(rast1 raster, rast2 raster, band int, index int)
-- to be found in the script/plpgsql folder

CREATE OR REPLACE FUNCTION ST_DeleteBand(rast raster,
				         band int) 
    RETURNS raster AS 
    $$
    DECLARE
	numband int := ST_NumBands(rast);
	newrast raster := ST_MakeEmptyRaster(rast);
    BEGIN
        FOR b IN 1..numband LOOP
        	IF b != band THEN
			newrast := ST_AddBand(newrast, rast, b, NULL);
		END IF;
        END LOOP;
	RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';

--Test
SELECT ST_MakeEmptyRaster(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

SELECT ST_MakeEmptyRaster(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, NULL), ST_TestRaster(1, 1, 3), 1, NULL));

SELECT st_width(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, NULL), ST_TestRaster(1, 1, 3), 1, NULL));

SELECT st_width(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, NULL));

SELECT ST_NumBands(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, NULL), ST_TestRaster(1, 1, 3), 1, NULL));
SELECT ST_Value(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, 2), ST_TestRaster(1, 1, 3), 1, 3), 2, 1, 1);

SELECT ST_NumBands(ST_DeleteBand(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, 2), ST_TestRaster(1, 1, 3), 1, 2), 2));

SELECT ST_NumBands(ST_DeleteBand(ST_DeleteBand(ST_AddBand(ST_AddBand(ST_TestRaster(1, 1, 1), ST_TestRaster(1, 1, 2), 1, 2), ST_TestRaster(1, 1, 3), 1, 2), 2), 2));

SELECT ST_DeleteBand(ST_TestRaster(1, 1, 1), 2);
