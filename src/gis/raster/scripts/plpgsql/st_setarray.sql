----------------------------------------------------------------------
-- Copyright (c) 2009-2010 Pierre Racine <pierre.racine@sbf.ulaval.ca>
----------------------------------------------------------------------

CREATE OR REPLACE FUNCTION ST_SetArea(rast raster, band, x int, y int, width int, height int, val float8)
    RETURNS raster AS 
    $$
    DECLARE
	newraster raster := rast;
	cx int;
	cy int;
	newwidth int := CASE WHEN x + width > ST_Width(rast) THEN ST_Width(rast) - x ELSE width END;
	newheight int := CASE WHEN y + height > ST_Height(rast) THEN ST_Height(rast) - y ELSE height END;
    BEGIN
	newrast
	FOR cx IN 1..newwidth LOOP
		FOR cy IN 1..newheight LOOP
			newrast := ST_SetValue(newrast, band, cx, cy, val);
		END LOOP;
	END LOOP;
	RETURN newrast;
    END;
    $$
    LANGUAGE 'plpgsql';


