 /*** 
 * 
 * Copyright (C) 2011 Regina Obe and Leo Hsu (Paragon Corporation)
 **/
-- This function given two roadways, state and optional city, zip
-- Will return addresses that are at the intersecton of those roadways
-- The address returned will be the address on the first road way
-- Use case example an address at the intersection of 2 streets: 
-- SELECT pprint_addy(addy), st_astext(geomout),rating FROM geocode_intersection('School St', 'Washington St', 'MA', 'Boston','02117');
--DROP FUNCTION tiger.geocode_intersection(text,text,text,text,text,integer);
CREATE OR REPLACE FUNCTION geocode_intersection(IN roadway1 text, IN roadway2 text, IN in_state text, IN in_city text DEFAULT '', IN in_zip text DEFAULT '', 
IN num_results integer DEFAULT 10,  OUT ADDY NORM_ADDY,
    OUT GEOMOUT GEOMETRY,
    OUT RATING INTEGER) RETURNS SETOF record AS
$$
DECLARE
    var_na_road norm_addy;
    var_na_inter1 norm_addy;
    var_sql text := '';
    var_zip varchar(5)[];
    in_statefp varchar(2) ; 
    var_debug boolean := get_geocode_setting('debug_geocode_intersection')::boolean;
    results record;
BEGIN
    IF COALESCE(roadway1,'') = '' OR COALESCE(roadway2,'') = '' THEN
        -- not enough to give a result just return
        RETURN ;
    ELSE
        var_na_road := normalize_address('0 ' || roadway1 || ', ' || COALESCE(in_city,'') || ', ' || in_state || ' ' || in_zip);
        var_na_inter1  := normalize_address('0 ' || roadway2 || ', ' || COALESCE(in_city,'') || ', ' || in_state || ' ' || in_zip);
    END IF;
    in_statefp := statefp FROM state_lookup As s WHERE s.abbrev = upper(in_state);
    IF COALESCE(in_zip,'') > '' THEN -- limit search to 2 plus or minus the input zip
        var_zip := zip_range(in_zip, -2,2);
    END IF;

    IF var_zip IS NULL AND in_city > '' THEN
        var_zip := array_agg(zip) FROM zip_lookup_base WHERE statefp = in_statefp AND lower(city) = lower(in_city);
    END IF;
    
    -- if we don't have a city or zip, don't bother doing the zip check, just keep as null
    IF var_zip IS NULL AND in_city > '' THEN
        var_zip := array_agg(zip) FROM zip_lookup_base WHERE statefp = in_statefp AND lower(city) LIKE lower(in_city) || '%'  ;
    END IF; 
    IF var_debug THEN
		RAISE NOTICE 'var_zip: %, city: %', quote_nullable(var_zip), quote_nullable(in_city);	
    END IF;
    var_sql := '
    WITH 
    	a1 AS (SELECT f.*, addr.fromhn, addr.tohn, addr.side , addr.zip
    				FROM (SELECT * FROM featnames 
    							WHERE statefp = $1 AND ( lower(name) = $2  ' ||
    							CASE WHEN length(var_na_road.streetName) > 5 THEN ' or  lower(fullname) LIKE $6 || ''%'' ' ELSE '' END || ')' 
    							|| ')  AS f LEFT JOIN (SELECT * FROM addr WHERE addr.statefp = $1) As addr ON (addr.tlid = f.tlid AND addr.statefp = f.statefp)
    					WHERE $5::text[] IS NULL OR addr.zip = ANY($5::text[]) OR addr.zip IS NULL 
    				ORDER BY CASE WHEN lower(f.fullname) = $6 THEN 0 ELSE 1 END
    				LIMIT 5000
    			  ),
        a2 AS (SELECT f.*, addr.fromhn, addr.tohn, addr.side , addr.zip
    				FROM (SELECT * FROM featnames 
    							WHERE statefp = $1 AND ( lower(name) = $4 ' || 
    							CASE WHEN length(var_na_inter1.streetName) > 5 THEN ' or lower(fullname) LIKE $7 || ''%'' ' ELSE '' END || ')' 
    							|| ' )  AS f LEFT JOIN (SELECT * FROM addr WHERE addr.statefp = $1) AS addr ON (addr.tlid = f.tlid AND addr.statefp = f.statefp)
    					WHERE $5::text[] IS NULL OR addr.zip = ANY($5::text[])  or addr.zip IS NULL 
    			ORDER BY CASE WHEN lower(f.fullname) = $7 THEN 0 ELSE 1 END
    				LIMIT 5000
    			  ),
    	 e1 AS (SELECT e.the_geom, e.tnidf, e.tnidt, a.*,
    	 			CASE WHEN a.side = ''L'' THEN e.tfidl ELSE e.tfidr END AS tfid
    	 			FROM a1 As a
    					INNER JOIN  edges AS e ON (e.statefp = a.statefp AND a.tlid = e.tlid)
    				WHERE e.statefp = $1 
    				ORDER BY CASE WHEN lower(a.name) = $4 THEN 0 ELSE 1 END + CASE WHEN lower(e.fullname) = $7 THEN 0 ELSE 1 END
    				LIMIT 1000) ,
    	e2 AS (SELECT e.the_geom, e.tnidf, e.tnidt, a.*,
    	 			CASE WHEN a.side = ''L'' THEN e.tfidl ELSE e.tfidr END AS tfid
    				FROM (SELECT * FROM edges WHERE statefp = $1) AS e INNER JOIN a2 AS a ON (e.statefp = a.statefp AND a.tlid = e.tlid)
    					INNER JOIN e1 ON (e.statefp = e1.statefp AND ST_Intersects(e.the_geom, e1.the_geom) 
    					AND ARRAY[e.tnidf, e.tnidt] && ARRAY[e1.tnidf, e1.tnidt] )
    					
    				WHERE (lower(e.fullname) = $7 or lower(a.name) LIKE $4 || ''%'')
    				ORDER BY CASE WHEN lower(a.name) = $4 THEN 0 ELSE 1 END + CASE WHEN lower(e.fullname) = $7 THEN 0 ELSE 1 END
    				LIMIT 100
    				), 
    	segs AS (SELECT DISTINCT ON(e1.tlid, e1.side)
                   CASE WHEN e1.tnidf = e2.tnidf OR e1.tnidf = e2.tnidt THEN
                                e1.fromhn
                            ELSE
                                e1.tohn END As address, e1.predirabrv As fedirp, COALESCE(e1.prequalabr || '' '','''' ) || e1.name As fename, 
                             COALESCE(e1.suftypabrv,e1.pretypabrv)  As fetype, e1.sufdirabrv AS fedirs, 
                               p.name As place, e1.zip,
                             CASE WHEN e1.tnidf = e2.tnidf OR e1.tnidf = e2.tnidt THEN
                                ST_StartPoint(ST_GeometryN(ST_Multi(e1.the_geom),1))
                             ELSE ST_EndPoint(ST_GeometryN(ST_Multi(e1.the_geom),1)) END AS geom ,   
                                CASE WHEN lower(p.name) = $3 THEN 0 ELSE 1 END  
                                + levenshtein_ignore_case(p.name, $3) 
                                + levenshtein_ignore_case(e1.name || COALESCE('' '' || e1.sufqualabr, ''''),$2) +
                                CASE WHEN e1.fullname = $6 THEN 0 ELSE levenshtein_ignore_case(e1.fullname, $6) END +
                                + levenshtein_ignore_case(e2.name || COALESCE('' '' || e2.sufqualabr, ''''),$4)
                                AS a_rating  
                    FROM e1 
                            INNER JOIN e2 ON (
                                    ST_Intersects(e1.the_geom, e2.the_geom)  ) 
                             INNER JOIN (SELECT * FROM faces WHERE statefp = $1) As fa1 ON (e1.tfid = fa1.tfid  )
                          LEFT JOIN place AS p ON (fa1.placefp = p.placefp AND p.statefp = $1 )
                       ORDER BY e1.tlid, e1.side, a_rating LIMIT $9*4 )
    SELECT address, fedirp , fename, fetype,fedirs,place, zip , geom, a_rating 
        FROM segs ORDER BY a_rating LIMIT  $9';

    IF var_debug THEN
        RAISE NOTICE 'sql: %', replace(replace(replace(
        	replace(replace(replace(
                replace(
                    replace(
                        replace(var_sql, '$1', quote_nullable(in_statefp)), 
                              '$2', quote_nullable(lower(var_na_road.streetName) ) ),
                      '$3', quote_nullable(lower(in_city)) ),
                      '$4', quote_nullable(lower(var_na_inter1.streetName) ) ),
                      '$5', quote_nullable(var_zip) ),
                      '$6', quote_nullable(lower(var_na_road.streetName || ' ' || COALESCE(var_na_road.streetTypeAbbrev,'') )) ) ,
                      '$7', quote_nullable(trim(lower(var_na_inter1.streetName || ' ' || COALESCE(var_na_inter1.streetTypeAbbrev,'') )) ) ) ,
		 '$8', quote_nullable(in_state ) ),  '$9', num_results::text );
    END IF;

    FOR results IN EXECUTE var_sql USING in_statefp, trim(lower(var_na_road.streetName)), lower(in_city), lower(var_na_inter1.streetName), var_zip,
		trim(lower(var_na_road.streetName || ' ' || COALESCE(var_na_road.streetTypeAbbrev,''))),
		trim(lower(var_na_inter1.streetName || ' ' || COALESCE(var_na_inter1.streetTypeAbbrev,''))), in_state, num_results LOOP
		ADDY.preDirAbbrev     := results.fedirp;
        ADDY.streetName       := results.fename;
        ADDY.streetTypeAbbrev := results.fetype;
        ADDY.postDirAbbrev    := results.fedirs;
        ADDY.location         := results.place;
        ADDY.stateAbbrev      := in_state;
        ADDY.zip              := results.zip;
        ADDY.parsed := TRUE;
        ADDY.address := results.address;
        
        GEOMOUT := results.geom;
        RATING := results.a_rating;
		RETURN NEXT; 
	END LOOP;
	RETURN;
END;
$$
  LANGUAGE plpgsql IMMUTABLE
  COST 1000
  ROWS 10;
ALTER FUNCTION geocode_intersection(IN text, IN text, IN text, IN text, IN text, IN integer) SET join_collapse_limit='2';
