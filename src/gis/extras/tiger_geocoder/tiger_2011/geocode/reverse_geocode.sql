 /*** 
 * 
 * Copyright (C) 2011-2014 Regina Obe and Leo Hsu (Paragon Corporation)
 **/
-- This function given a point try to determine the approximate street address (norm_addy form)
-- and array of cross streets, as well as interpolated points along the streets
-- Use case example an address at the intersection of 3 streets: SELECT pprint_addy(r.addy[1]) As st1, pprint_addy(r.addy[2]) As st2, pprint_addy(r.addy[3]) As st3, array_to_string(r.street, ',') FROM reverse_geocode(ST_GeomFromText('POINT(-71.057811 42.358274)',4269)) As r;
--set search_path=tiger,public;

CREATE OR REPLACE FUNCTION reverse_geocode(IN pt geometry, IN include_strnum_range boolean DEFAULT false, OUT intpt geometry[], OUT addy norm_addy[], OUT street character varying[])
  RETURNS record AS
$BODY$
DECLARE
  var_redge RECORD;
  var_state text := NULL;
  var_stusps text := NULL;
  var_countyfp text := NULL;
  var_addy NORM_ADDY;
  var_addy_alt NORM_ADDY;
  var_nstrnum numeric(10);
  var_primary_line geometry := NULL;
  var_primary_dist numeric(10,2) ;
  var_pt geometry;
  var_place varchar;
  var_county varchar;
  var_stmt text;
  var_debug boolean =  get_geocode_setting('debug_reverse_geocode')::boolean;
  var_rating_highway integer = COALESCE(get_geocode_setting('reverse_geocode_numbered_roads')::integer,0);/**0 no preference, 1 prefer highway number, 2 prefer local name **/
  var_zip varchar := NULL;
  var_primary_fullname varchar := '';
BEGIN
	IF pt IS NULL THEN
		RETURN;
	ELSE
		IF ST_SRID(pt) = 4269 THEN
			var_pt := pt;
		ELSIF ST_SRID(pt) > 0 THEN
			var_pt := ST_Transform(pt, 4269); 
		ELSE --If srid is unknown, assume its 4269
			var_pt := ST_SetSRID(pt, 4269);
		END IF;
		var_pt := ST_SnapToGrid(var_pt, 0.00005); /** Get rid of floating point junk that would prevent intersections **/
	END IF;
	-- Determine state tables to check 
	-- this is needed to take advantage of constraint exclusion
	IF var_debug THEN
		RAISE NOTICE 'Get matching states start: %', clock_timestamp();
	END IF;
	SELECT statefp, stusps INTO var_state, var_stusps FROM state WHERE ST_Intersects(the_geom, var_pt) LIMIT 1;
	IF var_debug THEN
		RAISE NOTICE 'Get matching states end: % -  %', var_state, clock_timestamp();
	END IF;
	IF var_state IS NULL THEN
		-- We don't have any data for this state
		RETURN;
	END IF;
	IF var_debug THEN
		RAISE NOTICE 'Get matching counties start: %', clock_timestamp();
	END IF;
	-- locate county
	var_stmt := 'SELECT countyfp, name  FROM  county WHERE  statefp =  $1 AND ST_Intersects(the_geom, $2) LIMIT 1;';
	EXECUTE var_stmt INTO var_countyfp, var_county USING var_state, var_pt ;

	--locate zip
	var_stmt := 'SELECT zcta5ce  FROM zcta5 WHERE statefp = $1 AND ST_Intersects(the_geom, $2)  LIMIT 1;';
	EXECUTE var_stmt INTO var_zip USING var_state, var_pt;
	-- locate city
	IF var_zip > '' THEN
	      var_addy.zip := var_zip ;
	END IF;
	
	var_stmt := 'SELECT z.name  FROM place As z WHERE  z.statefp =  $1 AND ST_Intersects(the_geom, $2) LIMIT 1;';
	EXECUTE var_stmt INTO var_place USING var_state, var_pt ;
	IF var_place > '' THEN
			var_addy.location := var_place;
	ELSE
		var_stmt := 'SELECT z.name  FROM cousub As z WHERE  z.statefp =  $1 AND ST_Intersects(the_geom, $2) LIMIT 1;';
		EXECUTE var_stmt INTO var_place USING var_state, var_pt ;
		IF var_place > '' THEN
			var_addy.location := var_place;
		-- ELSIF var_zip > '' THEN
		-- 	SELECT z.city INTO var_place FROM zip_lookup_base As z WHERE  z.statefp =  var_state AND z.county = var_county AND z.zip = var_zip LIMIT 1;
		-- 	var_addy.location := var_place;
		END IF;
	END IF;

	IF var_debug THEN
		RAISE NOTICE 'Get matching counties end: % - %',var_countyfp,  clock_timestamp();
	END IF;
	IF var_countyfp IS NULL THEN
		-- We don't have any data for this county
		RETURN;
	END IF;
	
	var_addy.stateAbbrev = var_stusps;

	-- Find the street edges that this point is closest to with tolerance of 0.005 but only consider the edge if the point is contained in the right or left face
	-- Then order addresses by proximity to road
	IF var_debug THEN
		RAISE NOTICE 'Get matching edges start: %', clock_timestamp();
	END IF;

	var_stmt := '
	    WITH ref AS (
	        SELECT ' || quote_literal(var_pt::text) || '::geometry As ref_geom ) , 
			f AS 
			( SELECT faces.* FROM faces  CROSS JOIN ref
			WHERE faces.statefp = ' || quote_literal(var_state) || ' AND faces.countyfp = ' || quote_literal(var_countyfp) || ' 
				AND ST_Intersects(faces.the_geom, ref_geom)
				    ),
			e AS 
			( SELECT edges.tlid , edges.statefp, edges.the_geom, CASE WHEN edges.tfidr = f.tfid THEN ''R'' WHEN edges.tfidl = f.tfid THEN ''L'' ELSE NULL END::varchar As eside,
                    ST_ClosestPoint(edges.the_geom,ref_geom) As center_pt, ref_geom
				FROM edges INNER JOIN f ON (f.statefp = edges.statefp AND (edges.tfidr = f.tfid OR edges.tfidl = f.tfid)) 
				    CROSS JOIN ref
			WHERE edges.statefp = ' || quote_literal(var_state) || ' AND edges.countyfp = ' || quote_literal(var_countyfp) || ' 
				AND ST_DWithin(edges.the_geom, ref.ref_geom, 0.01) AND (edges.mtfcc LIKE ''S%'') --only consider streets and roads
				  )	,
			ea AS 
			(SELECT e.statefp, e.tlid, a.fromhn, a.tohn, e.center_pt, ref_geom, a.zip, a.side, e.the_geom
				FROM e LEFT JOIN addr As a ON (a.statefp = ' || quote_literal(var_state) || '  AND e.tlid = a.tlid and e.eside = a.side) 
				)
		SELECT * 
		FROM (SELECT DISTINCT ON(tlid,side)  foo.fullname, foo.predirabrv, foo.streetname, foo.streettypeabbrev, foo.zip,  foo.center_pt,
			  side, to_number(CASE WHEN trim(fromhn) ~ ''^[0-9]+$'' THEN fromhn ELSE NULL END,''99999999'')  As fromhn, to_number(CASE WHEN trim(tohn) ~ ''^[0-9]+$'' THEN tohn ELSE NULL END,''99999999'') As tohn, 
			  ST_GeometryN(ST_Multi(line),1) As line, dist
		FROM 
		  (SELECT e.tlid, e.the_geom As line, n.fullname, COALESCE(n.prequalabr || '' '','''')  || n.name AS streetname, n.predirabrv, COALESCE(suftypabrv, pretypabrv) As streettypeabbrev,
		      n.sufdirabrv, e.zip, e.side, e.fromhn, e.tohn , e.center_pt,
		          ST_Distance_Sphere(ST_SetSRID(e.center_pt,4326),ST_SetSRID(ref_geom,4326)) As dist
				FROM ea AS e 
					LEFT JOIN (SELECT featnames.* FROM featnames 
			    WHERE featnames.statefp = ' || quote_literal(var_state) ||'   ) AS n ON (n.statefp =  e.statefp AND n.tlid = e.tlid) 
				ORDER BY dist LIMIT 50 ) As foo 
				ORDER BY foo.tlid, foo.side, ';
				
	    -- for numbered street/road use var_rating_highway to determine whether to prefer numbered or not (0 no pref, 1 prefer numbered, 2 prefer named)
		var_stmt := var_stmt || ' CASE $1 WHEN 0 THEN 0  WHEN 1 THEN CASE WHEN foo.fullname ~ ''[0-9]+'' THEN 0 ELSE 1 END ELSE CASE WHEN foo.fullname > '''' AND NOT (foo.fullname ~ ''[0-9]+'') THEN 0 ELSE 1 END END ';
		var_stmt := var_stmt || ',  foo.fullname ASC NULLS LAST, dist LIMIT 50) As f ORDER BY f.dist, CASE WHEN fullname > '''' THEN 0 ELSE 1 END '; --don't bother penalizing for distance if less than 20 meters
				
	IF var_debug = true THEN
	    RAISE NOTICE 'Statement 1: %', replace(var_stmt, '$1', var_rating_highway::text);
	END IF;

    FOR var_redge IN EXECUTE var_stmt USING var_rating_highway LOOP
        IF var_debug THEN
            RAISE NOTICE 'Start Get matching edges loop: %,%', var_primary_line, clock_timestamp();
        END IF;
        IF var_primary_line IS NULL THEN --this is the first time in the loop and our primary guess
            var_primary_line := var_redge.line;
            var_primary_dist := var_redge.dist;
        END IF;
  
        IF var_redge.fullname IS NOT NULL AND COALESCE(var_primary_fullname,'') = '' THEN -- this is the first non-blank name we are hitting grab info
            var_primary_fullname := var_redge.fullname;
            var_addy.streetname = var_redge.streetname;
            var_addy.streettypeabbrev := var_redge.streettypeabbrev;
            var_addy.predirabbrev := var_redge.predirabrv;
        END IF;
       
        IF ST_Intersects(var_redge.line, var_primary_line) THEN
            var_addy.streetname := var_redge.streetname; 
            
            var_addy.streettypeabbrev := var_redge.streettypeabbrev;
            var_addy.address := var_nstrnum;
            IF  var_redge.fromhn IS NOT NULL THEN
                --interpolate the number -- note that if fromhn > tohn we will be subtracting which is what we want
                var_nstrnum := (var_redge.fromhn + ST_LineLocatePoint(var_redge.line, var_pt)*(var_redge.tohn - var_redge.fromhn))::numeric(10);
                -- The odd even street number side of street rule
                IF (var_nstrnum  % 2)  != (var_redge.tohn % 2) THEN
                    var_nstrnum := CASE WHEN var_nstrnum + 1 NOT BETWEEN var_redge.fromhn AND var_redge.tohn THEN var_nstrnum - 1 ELSE var_nstrnum + 1 END;
                END IF;
                var_addy.address := var_nstrnum;
            END IF;
            IF var_redge.zip > ''  THEN
                var_addy.zip := var_redge.zip;
            ELSE
                var_addy.zip := var_zip;
            END IF;
            -- IF var_redge.location > '' THEN
            --     var_addy.location := var_redge.location;
            -- ELSE
            --     var_addy.location := var_place;
            -- END IF;  
            
            -- This is a cross streets - only add if not the primary adress street
            IF var_redge.fullname > '' AND var_redge.fullname <> var_primary_fullname THEN
                street := array_append(street, (CASE WHEN include_strnum_range THEN COALESCE(var_redge.fromhn::varchar, '')::varchar || COALESCE(' - ' || var_redge.tohn::varchar,'')::varchar || ' '::varchar  ELSE '' END::varchar ||  COALESCE(var_redge.fullname::varchar,''))::varchar);
            END IF;    
            
            -- consider this a potential address
            IF (var_redge.dist < var_primary_dist*1.1 OR var_redge.dist < 20)   THEN
                 -- We only consider this a possible address if it is really close to our point
                 intpt := array_append(intpt,var_redge.center_pt); 
                -- note that ramps don't have names or addresses but they connect at the edge of a range
                -- so for ramps the address of connecting is still useful
                IF var_debug THEN
                    RAISE NOTICE 'Current addresses: %, last added, %, street: %, %', addy, var_addy, var_addy.streetname, clock_timestamp();
                END IF;
                 addy := array_append(addy, var_addy);

                -- Use current values streetname for previous value if previous value has no streetname
				IF var_addy.streetname > '' AND array_upper(addy,1) > 1 AND COALESCE(addy[array_upper(addy,1) - 1].streetname, '') = ''  THEN
					-- the match is probably an offshoot of some sort
					-- replace prior entry with streetname of new if prior had no streetname
					var_addy_alt := addy[array_upper(addy,1)- 1];
					IF var_debug THEN
						RAISE NOTICE 'Replacing answer : %, %', addy[array_upper(addy,1) - 1], clock_timestamp();
					END IF;
					var_addy_alt.streetname := var_addy.streetname;
					var_addy_alt.streettypeabbrev := var_addy.streettypeabbrev;
                    var_addy_alt.predirabbrev := var_addy.predirabbrev;
					addy[array_upper(addy,1) - 1 ] := var_addy_alt; 
					IF var_debug THEN
						RAISE NOTICE 'Replaced with : %, %', var_addy_alt, clock_timestamp();
					END IF;
				END IF;
				
				IF var_debug THEN
					RAISE NOTICE 'End Get matching edges loop: %', clock_timestamp();
					RAISE NOTICE 'Final addresses: %, %', addy, clock_timestamp();
				END IF;

            END IF;
        END IF;
     
    END LOOP;
 
    -- not matching roads or streets, just return basic info
    IF NOT FOUND THEN
        addy := array_append(addy,var_addy);
        IF var_debug THEN
            RAISE NOTICE 'No address found: adding: % street: %, %', var_addy, var_addy.streetname, clock_timestamp();
        END IF;
    END IF;
    IF var_debug THEN
        RAISE NOTICE 'current array count : %, %', array_upper(addy,1), clock_timestamp();
    END IF;

    RETURN;   
END;
$BODY$
  LANGUAGE plpgsql STABLE
  COST 1000;
