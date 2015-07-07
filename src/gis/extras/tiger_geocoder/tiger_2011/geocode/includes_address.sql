-- This function requires the addresses to be grouped, such that the second and
-- third arguments are from one side of the street, and the fourth and fifth
-- from the other.
CREATE OR REPLACE FUNCTION includes_address(
    given_address INTEGER,
    addr1 INTEGER,
    addr2 INTEGER,
    addr3 INTEGER,
    addr4 INTEGER
) RETURNS BOOLEAN
AS $_$
DECLARE
  lmaxaddr INTEGER := -1;
  rmaxaddr INTEGER := -1;
  lminaddr INTEGER := -1;
  rminaddr INTEGER := -1;
  maxaddr INTEGER := -1;
  minaddr INTEGER := -1;
  verbose BOOLEAN := false;
BEGIN
  IF addr1 IS NOT NULL THEN
    maxaddr := addr1;
    minaddr := addr1;
    lmaxaddr := addr1;
    lminaddr := addr1;
  END IF;

  IF addr2 IS NOT NULL THEN
    IF addr2 < minaddr OR minaddr = -1 THEN
      minaddr := addr2;
    END IF;
    IF addr2 > maxaddr OR maxaddr = -1 THEN
      maxaddr := addr2;
    END IF;
    IF addr2 > lmaxaddr OR lmaxaddr = -1 THEN
      lmaxaddr := addr2;
    END IF;
    IF addr2 < lminaddr OR lminaddr = -1 THEN
      lminaddr := addr2;
    END IF;
  END IF;

  IF addr3 IS NOT NULL THEN
    IF addr3 < minaddr OR minaddr = -1 THEN
      minaddr := addr3;
    END IF;
    IF addr3 > maxaddr OR maxaddr = -1 THEN
      maxaddr := addr3;
    END IF;
    rmaxaddr := addr3;
    rminaddr := addr3;
  END IF;

  IF addr4 IS NOT NULL THEN
    IF addr4 < minaddr OR minaddr = -1 THEN
      minaddr := addr4;
    END IF;
    IF addr4 > maxaddr OR maxaddr = -1 THEN
      maxaddr := addr4;
    END IF;
    IF addr4 > rmaxaddr OR rmaxaddr = -1 THEN
      rmaxaddr := addr4;
    END IF;
    IF addr4 < rminaddr OR rminaddr = -1 THEN
      rminaddr := addr4;
    END IF;
  END IF;

  IF minaddr = -1 OR maxaddr = -1 THEN
    -- No addresses were non-null, return FALSE (arbitrary)
    RETURN FALSE;
  ELSIF given_address >= minaddr AND given_address <= maxaddr THEN
    -- The address is within the given range
    IF given_address >= lminaddr AND given_address <= lmaxaddr THEN
      -- This checks to see if the address is on this side of the
      -- road, ie if the address is even, the street range must be even
      IF (given_address % 2) = (lminaddr % 2)
          OR (given_address % 2) = (lmaxaddr % 2) THEN
        RETURN TRUE;
      END IF;
    END IF;
    IF given_address >= rminaddr AND given_address <= rmaxaddr THEN
      -- See above
      IF (given_address % 2) = (rminaddr % 2)
          OR (given_address % 2) = (rmaxaddr % 2) THEN
        RETURN TRUE;
      END IF;
    END IF;
  END IF;
  -- The address is not within the range
  RETURN FALSE;
END;
$_$ LANGUAGE plpgsql IMMUTABLE COST 100;
