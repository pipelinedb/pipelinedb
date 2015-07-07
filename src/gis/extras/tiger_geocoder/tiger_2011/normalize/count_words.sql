-- Determine the number of words in a string.  Words are allowed to
-- be seperated only by spaces, but multiple spaces between
-- words are allowed.
CREATE OR REPLACE FUNCTION count_words(VARCHAR) RETURNS INTEGER
AS $_$
DECLARE
  tempString VARCHAR;
  tempInt INTEGER;
  count INTEGER := 1;
  lastSpace BOOLEAN := FALSE;
BEGIN
  IF $1 IS NULL THEN
    return -1;
  END IF;
  tempInt := length($1);
  IF tempInt = 0 THEN
    return 0;
  END IF;
  FOR i IN 1..tempInt LOOP
    tempString := substring($1 from i for 1);
    IF tempString = ' ' THEN
      IF NOT lastSpace THEN
        count := count + 1;
      END IF;
      lastSpace := TRUE;
    ELSE
      lastSpace := FALSE;
    END IF;
  END LOOP;
  return count;
END;
$_$ LANGUAGE plpgsql IMMUTABLE;
