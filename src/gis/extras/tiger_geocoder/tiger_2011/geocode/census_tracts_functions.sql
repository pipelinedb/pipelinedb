 /*** 
 * 
 * Copyright (C) 2012 Regina Obe and Leo Hsu (Paragon Corporation)
 **/
-- This function given a geometry try will try to determine the tract.
-- It defaults to returning the tract name but can be changed to return track geoid id.
-- pass in 'tract_id' to get the full geoid, 'name' to get the short decimal name

CREATE OR REPLACE FUNCTION get_tract(IN loc_geom geometry, output_field text DEFAULT 'name')
  RETURNS text AS
$$
DECLARE
  var_state text := NULL;
  var_stusps text := NULL;
  var_result text := NULL;
  var_loc_geom geometry;
  var_stmt text;
  var_debug boolean = false;
BEGIN
	IF loc_geom IS NULL THEN
		RETURN null;
	ELSE
		IF ST_SRID(loc_geom) = 4269 THEN
			var_loc_geom := loc_geom;
		ELSIF ST_SRID(loc_geom) > 0 THEN
			var_loc_geom := ST_Transform(loc_geom, 4269); 
		ELSE --If srid is unknown, assume its 4269
			var_loc_geom := ST_SetSRID(loc_geom, 4269);
		END IF;
		IF GeometryType(var_loc_geom) != 'POINT' THEN
			var_loc_geom := ST_Centroid(var_loc_geom);
		END IF;
	END IF;
	-- Determine state tables to check 
	-- this is needed to take advantage of constraint exclusion
	IF var_debug THEN
		RAISE NOTICE 'Get matching states start: %', clock_timestamp();
	END IF;
	SELECT statefp, stusps INTO var_state, var_stusps FROM state WHERE ST_Intersects(the_geom, var_loc_geom) LIMIT 1;
	IF var_debug THEN
		RAISE NOTICE 'Get matching states end: % -  %', var_state, clock_timestamp();
	END IF;
	IF var_state IS NULL THEN
		-- We don't have any data for this state
		RAISE NOTICE 'No data for this state';
		RETURN NULL;
	END IF;
	-- locate county
	var_stmt := 'SELECT ' || quote_ident(output_field) || ' FROM tract WHERE statefp =  $1 AND ST_Intersects(the_geom, $2) LIMIT 1;';
	EXECUTE var_stmt INTO var_result USING var_state, var_loc_geom ;
	RETURN var_result;
END;
$$
  LANGUAGE plpgsql IMMUTABLE
  COST 500;
