-- location_extract_place_exact(string, stateAbbrev)
-- This function checks the place_lookup table to find a potential match to
-- the location described at the end of the given string.  If an exact match
-- fails, a fuzzy match is performed.  The location as found in the given
-- string is returned.
CREATE OR REPLACE FUNCTION location_extract_place_exact(
    fullStreet VARCHAR,
    stateAbbrev VARCHAR
) RETURNS VARCHAR
AS $_$
DECLARE
  ws VARCHAR;
  location VARCHAR;
  tempInt INTEGER;
  lstate VARCHAR;
  rec RECORD;
BEGIN
  ws := E'[ ,.\n\f\t]';

  -- Try for an exact match against places
  IF stateAbbrev IS NOT NULL THEN
    lstate := statefp FROM state WHERE stusps = stateAbbrev;
    SELECT INTO tempInt count(*) FROM place
        WHERE place.statefp = lstate AND fullStreet ILIKE '%' || name || '%'
        AND texticregexeq(fullStreet, '(?i)' || name || '$');
  ELSE
    SELECT INTO tempInt count(*) FROM place
        WHERE fullStreet ILIKE '%' || name || '%' AND
        	texticregexeq(fullStreet, '(?i)' || name || '$');
  END IF;

  IF tempInt > 0 THEN
    -- Some matches were found.  Look for the last one in the string.
    IF stateAbbrev IS NOT NULL THEN
      FOR rec IN SELECT substring(fullStreet, '(?i)('
          || name || ')$') AS value, name FROM place
          WHERE place.statefp = lstate AND fullStreet ILIKE '%' || name || '%'
          AND texticregexeq(fullStreet, '(?i)'
          || name || '$') ORDER BY length(name) DESC LOOP
        -- Since the regex is end of string, only the longest (first) result
        -- is useful.
        location := rec.value;
        EXIT;
      END LOOP;
    ELSE
      FOR rec IN SELECT substring(fullStreet, '(?i)('
          || name || ')$') AS value, name FROM place
          WHERE fullStreet ILIKE '%' || name || '%' AND texticregexeq(fullStreet, '(?i)'
          || name || '$') ORDER BY length(name) DESC LOOP
        -- Since the regex is end of string, only the longest (first) result
        -- is useful.
        location := rec.value;
        EXIT;
      END LOOP;
    END IF;
  END IF;

  RETURN location;
END;
$_$ LANGUAGE plpgsql STABLE COST 100;
