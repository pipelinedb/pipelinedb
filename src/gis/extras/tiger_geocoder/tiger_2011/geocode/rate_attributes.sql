-- rate_attributes(dirpA, dirpB, streetNameA, streetNameB, streetTypeA,
-- streetTypeB, dirsA, dirsB, locationA, locationB)
-- Rates the street based on the given attributes.  The locations must be
-- non-null.  The other eight values are handled by the other rate_attributes
-- function, so it's requirements must also be met.
-- changed: 2010-10-18 Regina Obe - all references to verbose to var_verbose since causes compile errors in 9.0
-- changed: 2011-06-25 revise to use real named args and fix direction rating typo
CREATE OR REPLACE FUNCTION rate_attributes(dirpA VARCHAR, dirpB VARCHAR, streetNameA VARCHAR, streetNameB VARCHAR,
    streetTypeA VARCHAR, streetTypeB VARCHAR, dirsA VARCHAR, dirsB VARCHAR,  locationA VARCHAR, locationB VARCHAR, prequalabr VARCHAR) RETURNS INTEGER
AS $_$
DECLARE
  result INTEGER := 0;
  locationWeight INTEGER := 14;
  var_verbose BOOLEAN := FALSE;
BEGIN
  IF locationA IS NOT NULL AND locationB IS NOT NULL THEN
    result := levenshtein_ignore_case(locationA, locationB);
  ELSE
    IF var_verbose THEN
      RAISE NOTICE 'rate_attributes() - Location names cannot be null!';
    END IF;
    RETURN NULL;
  END IF;
  result := result + rate_attributes($1, $2, streetNameA, streetNameB, $5, $6, $7, $8,prequalabr);
  RETURN result;
END;
$_$ LANGUAGE plpgsql IMMUTABLE;

-- rate_attributes(dirpA, dirpB, streetNameA, streetNameB, streetTypeA,
-- streetTypeB, dirsA, dirsB)
-- Rates the street based on the given attributes.  Only streetNames are
-- required.  If any others are null (either A or B) they are treated as
-- empty strings.
CREATE OR REPLACE FUNCTION rate_attributes(dirpA VARCHAR, dirpB VARCHAR, streetNameA VARCHAR, streetNameB VARCHAR,
    streetTypeA VARCHAR, streetTypeB VARCHAR, dirsA VARCHAR, dirsB VARCHAR, prequalabr VARCHAR) RETURNS INTEGER
AS $_$
DECLARE
  result INTEGER := 0;
  directionWeight INTEGER := 2;
  nameWeight INTEGER := 10;
  typeWeight INTEGER := 5;
  var_verbose BOOLEAN := false;
BEGIN
  result := result + levenshtein_ignore_case(cull_null($1), cull_null($2)) * directionWeight;
  IF var_verbose THEN
    RAISE NOTICE 'streetNameA: %, streetNameB: %', streetNameA, streetNameB;
  END IF;
  IF streetNameA IS NOT NULL AND streetNameB IS NOT NULL THEN
    -- We want to treat numeric streets that have numerics as equal 
    -- and not penalize if they are spelled different e.g. have ND instead of TH
    IF NOT numeric_streets_equal(streetNameA, streetNameB) THEN
        IF prequalabr IS NOT NULL THEN
            -- If the reference address (streetNameB) has a prequalabr streetNameA (prequalabr) - note: streetNameB usually comes thru without prequalabr
            -- and the input street (streetNameA) is lacking the prequal -- only penalize a little
            result := (result + levenshtein_ignore_case( trim( trim( lower(streetNameA),lower(prequalabr) ) ), trim( trim( lower(streetNameB),lower(prequalabr) ) ) )*nameWeight*0.75 + levenshtein_ignore_case(trim(streetNameA),prequalabr || ' ' ||  streetNameB) * nameWeight*0.25)::integer;
        ELSE
            result := result + levenshtein_ignore_case(streetNameA, streetNameB) * nameWeight;
        END IF;
    ELSE 
    -- Penalize for numeric streets if one is completely numeric and the other is not
    -- This is to minimize on highways like 3A being matched with numbered streets since streets are usually number followed by 2 characters e.g nth ave and highways are just number with optional letter for name
        IF  (streetNameB ~ E'[a-zA-Z]{2,10}' AND NOT (streetNameA ~ E'[a-zA-Z]{2,10}') ) OR (streetNameA ~ E'[a-zA-Z]{2,10}' AND NOT (streetNameB ~ E'[a-zA-Z]{2,10}') ) THEN
            result := result + levenshtein_ignore_case(streetNameA, streetNameB) * nameWeight;
        END IF;
    END IF;
  ELSE
    IF var_verbose THEN
      RAISE NOTICE 'rate_attributes() - Street names cannot be null!';
    END IF;
    RETURN NULL;
  END IF;
  result := result + levenshtein_ignore_case(cull_null(streetTypeA), cull_null(streetTypeB)) *
      typeWeight;
  result := result + levenshtein_ignore_case(cull_null(dirsA), cull_null(dirsB)) *
      directionWeight;
  return result;
END;
$_$ LANGUAGE plpgsql IMMUTABLE;
