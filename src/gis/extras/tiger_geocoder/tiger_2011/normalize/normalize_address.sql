-- normalize_address(addressString)
-- This takes an address string and parses it into address (internal/street)
-- street name, type, direction prefix and suffix, location, state and
-- zip code, depending on what can be found in the string.
--
-- The US postal address standard is used:
-- <Street Number> <Direction Prefix> <Street Name> <Street Type>
-- <Direction Suffix> <Internal Address> <Location> <State> <Zip Code>
--
-- State is assumed to be included in the string, and MUST be matchable to
-- something in the state_lookup table.  Fuzzy matching is used if no direct
-- match is found.
--
-- Two formats of zip code are acceptable: five digit, and five + 4.
--
-- The internal addressing indicators are looked up from the
-- secondary_unit_lookup table.  A following identifier is accepted
-- but it must start with a digit.
--
-- The location is parsed from the string using other indicators, such
-- as street type, direction suffix or internal address, if available.
-- If these are not, the location is extracted using comparisons against
-- the places_lookup table, then the countysub_lookup table to determine
-- what, in the original string, is intended to be the location.  In both
-- cases, an exact match is first pursued, then a word-by-word fuzzy match.
-- The result is not the name of the location from the tables, but the
-- section of the given string that corresponds to the name from the tables.
--
-- Zip codes and street names are not validated.
--
-- Direction indicators are extracted by comparison with the direction_lookup
-- table.
--
-- Street addresses are assumed to be a single word, starting with a number.
-- Address is manditory; if no address is given, and the street is numbered,
-- the resulting address will be the street name, and the street name
-- will be an empty string.
--
-- In some cases, the street type is part of the street name.
-- eg State Hwy 22a.  As long as the word following the type starts with a
-- number (this is usually the case) this will be caught.  Some street names
-- include a type name, and have a street type that differs.  This will be
-- handled properly, so long as both are given.  If the street type is
-- omitted, the street names included type will be parsed as the street type.
--
-- The output is currently a colon seperated list of values:
-- InternalAddress:StreetAddress:DirectionPrefix:StreetName:StreetType:
-- DirectionSuffix:Location:State:ZipCode
-- This returns each element as entered.  It's mainly meant for debugging.
-- There is also another option that returns:
-- StreetAddress:DirectionPrefixAbbreviation:StreetName:StreetTypeAbbreviation:
-- DirectionSuffixAbbreviation:Location:StateAbbreviation:ZipCode
-- This is more standardized and better for use with a geocoder.
CREATE OR REPLACE FUNCTION normalize_address(in_rawinput character varying)
  RETURNS norm_addy AS
$$
DECLARE
  debug_flag boolean := get_geocode_setting('debug_normalize_address')::boolean;
  use_pagc boolean := COALESCE(get_geocode_setting('use_pagc_address_parser')::boolean, false);
  result norm_addy;
  addressString VARCHAR;
  zipString VARCHAR;
  preDir VARCHAR;
  postDir VARCHAR;
  fullStreet VARCHAR;
  reducedStreet VARCHAR;
  streetType VARCHAR;
  state VARCHAR;
  tempString VARCHAR;
  tempInt INTEGER;
  rec RECORD;
  ws VARCHAR;
  rawInput VARCHAR;
  -- is this a highway 
  -- (we treat these differently since the road name often comes after the streetType)
  isHighway boolean := false; 
BEGIN
  result.parsed := FALSE;
  IF use_pagc THEN
  	result := pagc_normalize_address(in_rawinput);
  	RETURN result;
  END IF;

  rawInput := trim(in_rawInput);

  IF rawInput IS NULL THEN
    RETURN result;
  END IF;

  ws := E'[ ,.\t\n\f\r]';

  IF debug_flag THEN
    raise notice '% input: %', clock_timestamp(), rawInput;
  END IF;

  -- Assume that the address begins with a digit, and extract it from
  -- the input string.
  addressString := substring(rawInput from E'^([0-9].*?)[ ,/.]');

  IF debug_flag THEN
    raise notice '% addressString: %', clock_timestamp(), addressString;
  END IF;

  -- There are two formats for zip code, the normal 5 digit , and
  -- the nine digit zip-4.  It may also not exist.
  
  zipString := substring(rawInput from ws || E'([0-9]{5})$');
  IF zipString IS NULL THEN
    -- Check if the zip is just a partial or a one with -s
    -- or one that just has more than 5 digits
    zipString := COALESCE(substring(rawInput from ws || '([0-9]{5})-[0-9]{0,4}$'), 
                substring(rawInput from ws || '([0-9]{2,5})$'),
                substring(rawInput from ws || '([0-9]{6,14})$'));
   
     -- Check if all we got was a zipcode, of either form
    IF zipString IS NULL THEN
      zipString := substring(rawInput from '^([0-9]{5})$');
      IF zipString IS NULL THEN
        zipString := substring(rawInput from '^([0-9]{5})-[0-9]{4}$');
      END IF;
      -- If it was only a zipcode, then just return it.
      IF zipString IS NOT NULL THEN
        result.zip := zipString;
        result.parsed := TRUE;
        RETURN result;
      END IF;
    END IF;
  END IF;

  IF debug_flag THEN
    raise notice '% zipString: %', clock_timestamp(), zipString;
  END IF;

  IF zipString IS NOT NULL THEN
    fullStreet := substring(rawInput from '(.*)'
        || ws || '+' || cull_null(zipString) || '[- ]?([0-9]{4})?$');
    /** strip off any trailing  spaces or ,**/
    fullStreet :=  btrim(fullStreet, ' ,');
    
  ELSE
    fullStreet := rawInput;
  END IF;

  IF debug_flag THEN
    raise notice '% fullStreet: %', clock_timestamp(), fullStreet;
  END IF;

  -- FIXME: state_extract should probably be returning a record so we can
  -- avoid having to parse the result from it.
  tempString := state_extract(fullStreet);
  IF tempString IS NOT NULL THEN
    state := split_part(tempString, ':', 1);
    result.stateAbbrev := split_part(tempString, ':', 2);
  END IF;

  IF debug_flag THEN
    raise notice '% stateAbbrev: %', clock_timestamp(), result.stateAbbrev;
  END IF;

  -- The easiest case is if the address is comma delimited.  There are some
  -- likely cases:
  --   street level, location, state
  --   street level, location state
  --   street level, location
  --   street level, internal address, location, state
  --   street level, internal address, location state
  --   street level, internal address location state
  --   street level, internal address, location
  --   street level, internal address location
  -- The first three are useful.

  tempString := substring(fullStreet, '(?i),' || ws || '+(.*?)(,?' || ws ||
      '*' || cull_null(state) || '$)');
  IF tempString = '' THEN tempString := NULL; END IF;
  IF tempString IS NOT NULL THEN
    IF tempString LIKE '%,%' THEN -- if it has a comma probably has suite, strip it from location
        result.location := trim(split_part(tempString,',',2));
    ELSE
        result.location := tempString;
    END IF;
    IF addressString IS NOT NULL THEN
      fullStreet := substring(fullStreet, '(?i)' || addressString || ws ||
          '+(.*),' || ws || '+' || result.location);
    ELSE
      fullStreet := substring(fullStreet, '(?i)(.*),' || ws || '+' ||
          result.location);
    END IF;
  END IF;

  IF debug_flag THEN
    raise notice '% fullStreet: %',  clock_timestamp(), fullStreet;
    raise notice '% location: %', clock_timestamp(), result.location;
  END IF;

  -- Pull out the full street information, defined as everything between the
  -- address and the state.  This includes the location.
  -- This doesnt need to be done if location has already been found.
  IF result.location IS NULL THEN
    IF addressString IS NOT NULL THEN
      IF state IS NOT NULL THEN
        fullStreet := substring(fullStreet, '(?i)' || addressString ||
            ws || '+(.*?)' || ws || '+' || state);
      ELSE
        fullStreet := substring(fullStreet, '(?i)' || addressString ||
            ws || '+(.*?)');
      END IF;
    ELSE
      IF state IS NOT NULL THEN
        fullStreet := substring(fullStreet, '(?i)(.*?)' || ws ||
            '+' || state);
      ELSE
        fullStreet := substring(fullStreet, '(?i)(.*?)');
      END IF;
    END IF;

    IF debug_flag THEN
      raise notice '% fullStreet: %', clock_timestamp(),fullStreet;
    END IF;

    IF debug_flag THEN
      raise notice '% start location extract', clock_timestamp();
    END IF;
    result.location := location_extract(fullStreet, result.stateAbbrev);

    IF debug_flag THEN
      raise notice '% end location extract', clock_timestamp();
    END IF;

    -- A location can't be a street type, sorry.
    IF lower(result.location) IN (SELECT lower(name) FROM street_type_lookup) THEN
        result.location := NULL;
    END IF;

    -- If the location was found, remove it from fullStreet
    IF result.location IS NOT NULL THEN
      fullStreet := substring(fullStreet, '(?i)(.*)' || ws || '+' ||
          result.location);
    END IF;
  END IF;

  IF debug_flag THEN
    raise notice 'fullStreet: %', fullStreet;
    raise notice 'location: %', result.location;
  END IF;

  -- Determine if any internal address is included, such as apartment
  -- or suite number.
  -- this count is surprisingly slow by itself but much faster if you add an ILIKE AND clause
  SELECT INTO tempInt count(*) FROM secondary_unit_lookup
      WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name || '('
          || ws || '|$)');
  IF tempInt = 1 THEN
    result.internal := substring(fullStreet, '(?i)' || ws || '('
        || name ||  ws || '*#?' || ws
        || '*(?:[0-9][-0-9a-zA-Z]*)?' || ')(?:' || ws || '|$)')
        FROM secondary_unit_lookup
        WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name || '('
        || ws || '|$)');
    ELSIF tempInt > 1 THEN
    -- In the event of multiple matches to a secondary unit designation, we
    -- will assume that the last one is the true one.
    tempInt := 0;
    FOR rec in SELECT trim(substring(fullStreet, '(?i)' || ws || '('
        || name || '(?:' || ws || '*#?' || ws
        || '*(?:[0-9][-0-9a-zA-Z]*)?)' || ws || '?|$)')) as value
        FROM secondary_unit_lookup
        WHERE fullStreet ILIKE '%' || name || '%' AND  texticregexeq(fullStreet, '(?i)' || ws || name || '('
        || ws || '|$)') LOOP
      IF tempInt < position(rec.value in fullStreet) THEN
        tempInt := position(rec.value in fullStreet);
        result.internal := rec.value;
      END IF;
    END LOOP;
  END IF;

  IF debug_flag THEN
    raise notice 'internal: %', result.internal;
  END IF;

  IF result.location IS NULL THEN
    -- If the internal address is given, the location is everything after it.
    result.location := trim(substring(fullStreet, result.internal || ws || '+(.*)$'));
  END IF;

  IF debug_flag THEN
    raise notice 'location: %', result.location;
  END IF;

  -- Pull potential street types from the full street information
  -- this count is surprisingly slow by itself but much faster if you add an ILIKE AND clause
  -- difference of 98ms vs 16 ms for example
  -- Put a space in front to make regex easier can always count on it starting with space
  -- Reject all street types where the fullstreet name is equal to the name
  fullStreet := ' ' || trim(fullStreet);
  tempInt := count(*) FROM street_type_lookup
      WHERE fullStreet ILIKE '%' || name || '%' AND 
        trim(upper(fullStreet)) != name AND
        texticregexeq(fullStreet, '(?i)' || ws || '(' || name
      || ')(?:' || ws || '|$)');
  IF tempInt = 1 THEN
    SELECT INTO rec abbrev, substring(fullStreet, '(?i)' || ws || '('
        || name || ')(?:' || ws || '|$)') AS given, is_hw FROM street_type_lookup
        WHERE fullStreet ILIKE '%' || name || '%' AND 
             trim(upper(fullStreet)) != name AND
            texticregexeq(fullStreet, '(?i)' || ws || '(' || name
        || ')(?:' || ws || '|$)')  ;
    streetType := rec.given;
    result.streetTypeAbbrev := rec.abbrev;
    isHighway :=  rec.is_hw;
    IF debug_flag THEN
    	   RAISE NOTICE 'street Type: %, street Type abbrev: %', rec.given, rec.abbrev;
    END IF;
  ELSIF tempInt > 1 THEN
    tempInt := 0;
    -- the last matching abbrev in the string is the most likely one
    FOR rec IN SELECT * FROM 
    	(SELECT abbrev, name, substring(fullStreet, '(?i)' || ws || '?('
        || name || ')(?:' || ws || '|$)') AS given, is_hw ,
        		RANK() OVER( ORDER BY position(name IN upper(trim(fullStreet))) ) As n_start,
        		RANK() OVER( ORDER BY position(name IN upper(trim(fullStreet))) + length(name) ) As n_end,
        		COUNT(*) OVER() As nrecs, position(name IN upper(trim(fullStreet)))
        		FROM street_type_lookup
        WHERE fullStreet ILIKE '%' || name || '%'  AND 
            trim(upper(fullStreet)) != name AND 
            (texticregexeq(fullStreet, '(?i)' || ws || '(' || name 
            -- we only consider street types that are regular and not at beginning of name or are highways (since those can be at beg or end)
            -- we take the one that is the longest e.g Country Road would be more correct than Road
        || ')(?:' || ws || '|$)') OR (is_hw AND fullstreet ILIKE name || ' %') )
     AND ((NOT is_hw AND position(name IN upper(trim(fullStreet))) > 1 OR is_hw) )
        ) As foo
        -- N_start - N_end - ensure we first get the one with the most overlapping sub types 
        -- Then of those get the one that ends last and then starts first
        ORDER BY n_start - n_end, n_end DESC, n_start LIMIT 1  LOOP
      -- If we have found an internal address, make sure the type
      -- precedes it.
      /** TODO: I don't think we need a loop anymore since we are just returning one and the one in the last position
      * I'll leave for now though **/
      IF result.internal IS NOT NULL THEN
        IF position(rec.given IN fullStreet) < position(result.internal IN fullStreet) THEN
          IF tempInt < position(rec.given IN fullStreet) THEN
            streetType := rec.given;
            result.streetTypeAbbrev := rec.abbrev;
            isHighway := rec.is_hw;
            tempInt := position(rec.given IN fullStreet);
          END IF;
        END IF;
      ELSIF tempInt < position(rec.given IN fullStreet) THEN
        streetType := rec.given;
        result.streetTypeAbbrev := rec.abbrev;
        isHighway := rec.is_hw;
        tempInt := position(rec.given IN fullStreet);
        IF debug_flag THEN
        	RAISE NOTICE 'street Type: %, street Type abbrev: %', rec.given, rec.abbrev;
        END IF;
      END IF;
    END LOOP;
  END IF;

  IF debug_flag THEN
    raise notice '% streetTypeAbbrev: %', clock_timestamp(), result.streetTypeAbbrev;
  END IF;

  -- There is a little more processing required now.  If the word after the
  -- street type begins with a number, then its most likely a highway like State Route 225a.  If
  -- In Tiger 2010+ the reduced Street name just has the number
  -- the next word starts with a char, then everything after the street type
  -- will be considered location.  If there is no street type, then I'm sad.
  IF streetType IS NOT NULL THEN
    -- Check if the fullStreet contains the streetType and ends in just numbers
    -- If it does its a road number like a country road or state route or other highway
    -- Just set the number to be the name of street
    
    tempString := NULL;
    IF isHighway THEN
        tempString :=  substring(fullStreet, streetType || ws || '+' || E'([0-9a-zA-Z]+)' || ws || '*');
    END IF;    
    IF tempString > '' AND result.location IS NOT NULL THEN
        reducedStreet := tempString;
        result.streetName := reducedStreet;
        IF debug_flag THEN
        	RAISE NOTICE 'reduced Street: %', result.streetName;
        END IF;
        -- the post direction might be portion of fullStreet after reducedStreet and type
		-- reducedStreet: 24  fullStreet: Country Road 24, N or fullStreet: Country Road 24 N
		tempString := regexp_replace(fullStreet, streetType || ws || '+' || reducedStreet,'');
		IF tempString > '' THEN
			IF debug_flag THEN
				RAISE NOTICE 'remove reduced street: % + streetType: % from fullstreet: %', reducedStreet, streetType, fullStreet;
			END IF;
			tempString := abbrev FROM direction_lookup WHERE
			 tempString ILIKE '%' || name || '%'  AND texticregexeq(reducedStreet || ws || '+' || streetType, '(?i)(' || name || ')' || ws || '+|$')
			 	ORDER BY length(name) DESC LIMIT 1;
			IF tempString IS NOT NULL THEN
				result.postDirAbbrev = trim(tempString);
				IF debug_flag THEN
					RAISE NOTICE 'postDirAbbre of highway: %', result.postDirAbbrev;
				END IF;
			END IF;
		END IF;
    ELSE
        tempString := substring(fullStreet, streetType || ws ||
            E'+([0-9][^ ,.\t\r\n\f]*?)' || ws);
        IF tempString IS NOT NULL THEN
          IF result.location IS NULL THEN
            result.location := substring(fullStreet, streetType || ws || '+'
                     || tempString || ws || '+(.*)$');
          END IF;
          reducedStreet := substring(fullStreet, '(.*)' || ws || '+'
                        || result.location || '$');
          streetType := NULL;
          result.streetTypeAbbrev := NULL;
        ELSE
          IF result.location IS NULL THEN
            result.location := substring(fullStreet, streetType || ws || '+(.*)$');
          END IF;
          reducedStreet := substring(fullStreet, '^(.*)' || ws || '+'
                        || streetType);
          IF COALESCE(trim(reducedStreet),'') = '' THEN --reduced street can't be blank
            reducedStreet := fullStreet;
            streetType := NULL;
            result.streetTypeAbbrev := NULL;
          END IF;
        END IF;
		-- the post direction might be portion of fullStreet after reducedStreet
		-- reducedStreet: Main  fullStreet: Main St, N or fullStreet: Main St N
		tempString := trim(regexp_replace(fullStreet,  reducedStreet ||  ws || '+' || streetType,''));
		IF tempString > '' THEN
		  tempString := abbrev FROM direction_lookup WHERE
			 tempString ILIKE '%' || name || '%'  
			 AND texticregexeq(fullStreet || ' ', '(?i)' || reducedStreet || ws || '+' || streetType || ws || '+(' || name || ')' || ws || '+')
			ORDER BY length(name) DESC LIMIT 1;
		  IF tempString IS NOT NULL THEN
			result.postDirAbbrev = trim(tempString);
		  END IF;
		END IF;
 

		IF debug_flag THEN
			raise notice '% reduced street: %', clock_timestamp(), reducedStreet;
		END IF;
		
		-- The pre direction should be at the beginning of the fullStreet string.
		-- The post direction should be at the beginning of the location string
		-- if there is no internal address
		reducedStreet := trim(reducedStreet);
		tempString := trim(regexp_replace(fullStreet,  ws || '+' || reducedStreet ||  ws || '+',''));
		IF tempString > '' THEN
			tempString := substring(reducedStreet, '(?i)(^' || name
				|| ')' || ws) FROM direction_lookup WHERE
				 reducedStreet ILIKE '%' || name || '%'  AND texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
				ORDER BY length(name) DESC LIMIT 1;
		END IF;
		IF tempString > '' THEN
		  preDir := tempString;
		  result.preDirAbbrev := abbrev FROM direction_lookup
			  where reducedStreet ILIKE '%' || name '%' AND texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
			  ORDER BY length(name) DESC LIMIT 1;
		  result.streetName := trim(substring(reducedStreet, '^' || preDir || ws || '(.*)'));
		ELSE
		  result.streetName := trim(reducedStreet);
		END IF;
    END IF;
    IF texticregexeq(result.location, '(?i)' || result.internal || '$') THEN
      -- If the internal address is at the end of the location, then no
      -- location was given.  We still need to look for post direction.
      SELECT INTO rec abbrev,
          substring(result.location, '(?i)^(' || name || ')' || ws) as value
          FROM direction_lookup 
            WHERE result.location ILIKE '%' || name || '%' AND texticregexeq(result.location, '(?i)^'
          || name || ws) ORDER BY length(name) desc LIMIT 1;
      IF rec.value IS NOT NULL THEN
        postDir := rec.value;
        result.postDirAbbrev := rec.abbrev;
      END IF;
      result.location := null;
    ELSIF result.internal IS NULL THEN
      -- If no location is given, the location string will be the post direction
      SELECT INTO tempInt count(*) FROM direction_lookup WHERE
          upper(result.location) = upper(name);
      IF tempInt != 0 THEN
        postDir := result.location;
        SELECT INTO result.postDirAbbrev abbrev FROM direction_lookup WHERE
            upper(postDir) = upper(name);
        result.location := NULL;
        
        IF debug_flag THEN
            RAISE NOTICE '% postDir exact match: %', clock_timestamp(), result.postDirAbbrev;
        END IF;
      ELSE
        -- postDirection is not equal location, but may be contained in it
        -- It is only considered a postDirection if it is not preceded by a ,
        SELECT INTO tempString substring(result.location, '(?i)(^' || name
            || ')' || ws) FROM direction_lookup WHERE
            result.location ILIKE '%' || name || '%' AND texticregexeq(result.location, '(?i)(^' || name || ')' || ws)
            	AND NOT  texticregexeq(rawInput, '(?i)(,' || ws || '+' || result.location || ')' || ws)
            ORDER BY length(name) desc LIMIT 1;
            
        IF debug_flag THEN
            RAISE NOTICE '% location trying to extract postdir: %, tempstring: %, rawInput: %', clock_timestamp(), result.location, tempString, rawInput;
        END IF;
        IF tempString IS NOT NULL THEN
            postDir := tempString;
            SELECT INTO result.postDirAbbrev abbrev FROM direction_lookup
              WHERE result.location ILIKE '%' || name || '%' AND texticregexeq(result.location, '(?i)(^' || name || ')' || ws) ORDER BY length(name) DESC LIMIT 1;
              result.location := substring(result.location, '^' || postDir || ws || '+(.*)');
            IF debug_flag THEN
                  RAISE NOTICE '% postDir: %', clock_timestamp(), result.postDirAbbrev;
            END IF;
        END IF;
        
      END IF;
    ELSE
      -- internal is not null, but is not at the end of the location string
      -- look for post direction before the internal address
        IF debug_flag THEN
            RAISE NOTICE '%fullstreet before extract postdir: %', clock_timestamp(), fullStreet;
        END IF;
        SELECT INTO tempString substring(fullStreet, '(?i)' || streetType
          || ws || '+(' || name || ')' || ws || '+' || result.internal)
          FROM direction_lookup 
          WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)'
          || ws || name || ws || '+' || result.internal) ORDER BY length(name) desc LIMIT 1;
        IF tempString IS NOT NULL THEN
            postDir := tempString;
            SELECT INTO result.postDirAbbrev abbrev FROM direction_lookup
                WHERE texticregexeq(fullStreet, '(?i)' || ws || name || ws);
        END IF;
    END IF;
  ELSE
  -- No street type was found

    -- If an internal address was given, then the split becomes easy, and the
    -- street name is everything before it, without directions.
    IF result.internal IS NOT NULL THEN
      reducedStreet := substring(fullStreet, '(?i)^(.*?)' || ws || '+'
                    || result.internal);
      tempInt := count(*) FROM direction_lookup WHERE
          reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)' || ws || name || '$');
      IF tempInt > 0 THEN
        postDir := substring(reducedStreet, '(?i)' || ws || '('
            || name || ')' || '$') FROM direction_lookup
            WHERE reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)' || ws || name || '$');
        result.postDirAbbrev := abbrev FROM direction_lookup
            WHERE texticregexeq(reducedStreet, '(?i)' || ws || name || '$');
      END IF;
      tempString := substring(reducedStreet, '(?i)^(' || name
          || ')' || ws) FROM direction_lookup WHERE
           reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)^(' || name || ')' || ws)
          ORDER BY length(name) DESC;
      IF tempString IS NOT NULL THEN
        preDir := tempString;
        result.preDirAbbrev := abbrev FROM direction_lookup WHERE
             reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
            ORDER BY length(name) DESC;
        result.streetName := substring(reducedStreet, '(?i)^' || preDir || ws
                   || '+(.*?)(?:' || ws || '+' || cull_null(postDir) || '|$)');
      ELSE
        result.streetName := substring(reducedStreet, '(?i)^(.*?)(?:' || ws
                   || '+' || cull_null(postDir) || '|$)');
      END IF;
    ELSE

      -- If a post direction is given, then the location is everything after,
      -- the street name is everything before, less any pre direction.
      fullStreet := trim(fullStreet);
      tempInt := count(*) FROM direction_lookup
          WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name || '(?:'
              || ws || '|$)');

      IF tempInt = 1 THEN
        -- A single postDir candidate was found.  This makes it easier.
        postDir := substring(fullStreet, '(?i)' || ws || '('
            || name || ')(?:' || ws || '|$)') FROM direction_lookup WHERE
             fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name || '(?:'
            || ws || '|$)');
        result.postDirAbbrev := abbrev FROM direction_lookup
            WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name
            || '(?:' || ws || '|$)');
        IF result.location IS NULL THEN
          result.location := substring(fullStreet, '(?i)' || ws || postDir
                   || ws || '+(.*?)$');
        END IF;
        reducedStreet := substring(fullStreet, '^(.*?)' || ws || '+'
                      || postDir);
        tempString := substring(reducedStreet, '(?i)(^' || name
            || ')' || ws) FROM direction_lookup 
            WHERE
                reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
            ORDER BY length(name) DESC;
        IF tempString IS NOT NULL THEN
          preDir := tempString;
          result.preDirAbbrev := abbrev FROM direction_lookup WHERE
              reducedStreet ILIKE '%' || name || '%' AND texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
              ORDER BY length(name) DESC;
          result.streetName := trim(substring(reducedStreet, '^' || preDir || ws
                     || '+(.*)'));
        ELSE
          result.streetName := trim(reducedStreet);
        END IF;
      ELSIF tempInt > 1 THEN
        -- Multiple postDir candidates were found.  We need to find the last
        -- incident of a direction, but avoid getting the last word from
        -- a two word direction. eg extracting "East" from "North East"
        -- We do this by sorting by length, and taking the last direction
        -- in the results that is not included in an earlier one.
        -- This wont be a problem it preDir is North East and postDir is
        -- East as the regex requires a space before the direction.  Only
        -- the East will return from the preDir.
        tempInt := 0;
        FOR rec IN SELECT abbrev, substring(fullStreet, '(?i)' || ws || '('
            || name || ')(?:' || ws || '|$)') AS value
            FROM direction_lookup
            WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)' || ws || name
            || '(?:' || ws || '|$)')
            ORDER BY length(name) desc LOOP
          tempInt := 0;
          IF tempInt < position(rec.value in fullStreet) THEN
            IF postDir IS NULL THEN
              tempInt := position(rec.value in fullStreet);
              postDir := rec.value;
              result.postDirAbbrev := rec.abbrev;
            ELSIF NOT texticregexeq(postDir, '(?i)' || rec.value) THEN
              tempInt := position(rec.value in fullStreet);
              postDir := rec.value;
              result.postDirAbbrev := rec.abbrev;
             END IF;
          END IF;
        END LOOP;
        IF result.location IS NULL THEN
          result.location := substring(fullStreet, '(?i)' || ws || postDir || ws
                   || '+(.*?)$');
        END IF;
        reducedStreet := substring(fullStreet, '(?i)^(.*?)' || ws || '+'
                      || postDir);
        SELECT INTO tempString substring(reducedStreet, '(?i)(^' || name
            || ')' || ws) FROM direction_lookup WHERE
             reducedStreet ILIKE '%' || name || '%' AND  texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
            ORDER BY length(name) DESC;
        IF tempString IS NOT NULL THEN
          preDir := tempString;
          SELECT INTO result.preDirAbbrev abbrev FROM direction_lookup WHERE
              reducedStreet ILIKE '%' || name || '%' AND  texticregexeq(reducedStreet, '(?i)(^' || name || ')' || ws)
              ORDER BY length(name) DESC;
          result.streetName := substring(reducedStreet, '^' || preDir || ws
                     || '+(.*)');
        ELSE
          result.streetName := reducedStreet;
        END IF;
      ELSE

        -- There is no street type, directional suffix or internal address
        -- to allow distinction between street name and location.
        IF result.location IS NULL THEN
          IF debug_flag THEN
            raise notice 'fullStreet: %', fullStreet;
          END IF;

          result.location := location_extract(fullStreet, result.stateAbbrev);
          -- If the location was found, remove it from fullStreet
          IF result.location IS NOT NULL THEN
            fullStreet := substring(fullStreet, '(?i)(.*),' || ws || '+' ||
                result.location);
          END IF;
        END IF;

        -- Check for a direction prefix.
        SELECT INTO tempString substring(fullStreet, '(?i)(^' || name
            || ')' || ws) FROM direction_lookup WHERE
            texticregexeq(fullStreet, '(?i)(^' || name || ')' || ws)
            ORDER BY length(name);
        IF tempString IS NOT NULL THEN
          preDir := tempString;
          SELECT INTO result.preDirAbbrev abbrev FROM direction_lookup WHERE
              texticregexeq(fullStreet, '(?i)(^' || name || ')' || ws)
              ORDER BY length(name) DESC;
          IF result.location IS NOT NULL THEN
            -- The location may still be in the fullStreet, or may
            -- have been removed already
            result.streetName := substring(fullStreet, '^' || preDir || ws
                       || '+(.*?)(' || ws || '+' || result.location || '|$)');
          ELSE
            result.streetName := substring(fullStreet, '^' || preDir || ws
                       || '+(.*?)' || ws || '*');
          END IF;
        ELSE
          IF result.location IS NOT NULL THEN
            -- The location may still be in the fullStreet, or may
            -- have been removed already
            result.streetName := substring(fullStreet, '^(.*?)(' || ws
                       || '+' || result.location || '|$)');
          ELSE
            result.streetName := fullStreet;
          END IF;
        END IF;
      END IF;
    END IF;
  END IF;

 -- For address number only put numbers and stop if reach a non-number e.g. 123-456 will return 123
  result.address := to_number(substring(addressString, '[0-9]+'), '99999999999');
   --get rid of extraneous spaces before we return
  result.zip := trim(zipString);
  result.streetName := trim(result.streetName);
  result.location := trim(result.location);
  result.postDirAbbrev := trim(result.postDirAbbrev);
  result.parsed := TRUE;
  RETURN result;
END
$$
  LANGUAGE plpgsql IMMUTABLE STRICT
  COST 100;