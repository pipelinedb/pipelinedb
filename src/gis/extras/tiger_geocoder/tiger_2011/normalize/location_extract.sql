-- location_extract(streetAddressString, stateAbbreviation)
-- This function extracts a location name from the end of the given string.
-- The first attempt is to find an exact match against the place_lookup
-- table.  If this fails, a word-by-word soundex match is tryed against the
-- same table.  If multiple candidates are found, the one with the smallest
-- levenshtein distance from the given string is assumed the correct one.
-- If no match is found against the place_lookup table, the same tests are
-- run against the countysub_lookup table.
--
-- The section of the given string corresponding to the location found is
-- returned, rather than the string found from the tables.  All the searching
-- is done largely to determine the length (words) of the location, to allow
-- the intended street name to be correctly identified.
CREATE OR REPLACE FUNCTION location_extract(fullStreet VARCHAR, stateAbbrev VARCHAR) RETURNS VARCHAR
AS $_$
DECLARE
  ws VARCHAR;
  location VARCHAR;
  lstate VARCHAR;
  stmt VARCHAR;
  street_array text[];
  word_count INTEGER;
  rec RECORD;
  best INTEGER := 0;
  tempString VARCHAR;
BEGIN
  IF fullStreet IS NULL THEN
    RETURN NULL;
  END IF;

  ws := E'[ ,.\n\f\t]';

  IF stateAbbrev IS NOT NULL THEN
    lstate := statefp FROM state_lookup WHERE abbrev = stateAbbrev;
  END IF;
  lstate := COALESCE(lstate,'');

  street_array := regexp_split_to_array(fullStreet,ws);
  word_count := array_upper(street_array,1);

  tempString := '';
  FOR i IN 1..word_count LOOP
    CONTINUE WHEN street_array[word_count-i+1] IS NULL OR street_array[word_count-i+1] = '';

    tempString := COALESCE(street_array[word_count-i+1],'') || tempString;

    stmt := ' SELECT'
         || '   1,'
         || '   name,'
         || '   levenshtein_ignore_case(' || quote_literal(tempString) || ',name) as rating,'
         || '   length(name) as len'
         || ' FROM place'
         || ' WHERE ' || CASE WHEN stateAbbrev IS NOT NULL THEN 'statefp = ' || quote_literal(lstate) || ' AND ' ELSE '' END
         || '   soundex(' || quote_literal(tempString) || ') = soundex(name)'
         || '   AND levenshtein_ignore_case(' || quote_literal(tempString) || ',name) <= 2 '
         || ' UNION ALL SELECT'
         || '   2,'
         || '   name,'
         || '   levenshtein_ignore_case(' || quote_literal(tempString) || ',name) as rating,'
         || '   length(name) as len'
         || ' FROM cousub'
         || ' WHERE ' || CASE WHEN stateAbbrev IS NOT NULL THEN 'statefp = ' || quote_literal(lstate) || ' AND ' ELSE '' END
         || '   soundex(' || quote_literal(tempString) || ') = soundex(name)'
         || '   AND levenshtein_ignore_case(' || quote_literal(tempString) || ',name) <= 2 '
         || ' ORDER BY '
         || '   3 ASC, 1 ASC, 4 DESC'
         || ' LIMIT 1;'
         ;

    EXECUTE stmt INTO rec;

    IF rec.rating >= best THEN
      location := tempString;
      best := rec.rating;
    END IF;

    tempString := ' ' || tempString;
  END LOOP;

  location := replace(location,' ',ws || '+');
  location := substring(fullStreet,'(?i)' || location || '$');

  RETURN location;
END;
$_$ LANGUAGE plpgsql STABLE COST 100;
