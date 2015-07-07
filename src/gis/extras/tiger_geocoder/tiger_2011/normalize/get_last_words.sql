-- Returns a string consisting of the last N words.  Words are allowed
-- to be seperated only by spaces, but multiple spaces between
-- words are allowed.  Words must be alphanumberic.
-- If more words are requested than exist, the full input string is
-- returned.
CREATE OR REPLACE FUNCTION get_last_words(
    inputString VARCHAR,
    count INTEGER
) RETURNS VARCHAR
AS $_$
DECLARE
  tempString VARCHAR;
  result VARCHAR := '';
BEGIN
  FOR i IN 1..count LOOP
    tempString := substring(inputString from '((?: )+[a-zA-Z0-9_]*)' || result || '$');

    IF tempString IS NULL THEN
      RETURN inputString;
    END IF;

    result := tempString || result;
  END LOOP;

  result := trim(both from result);

  RETURN result;
END;
$_$ LANGUAGE plpgsql IMMUTABLE COST 10;
