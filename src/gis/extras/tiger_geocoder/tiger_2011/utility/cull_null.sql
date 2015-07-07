-- Returns the value passed, or an empty string if null.
-- This is used to concatinate values that may be null.
CREATE OR REPLACE FUNCTION cull_null(VARCHAR) RETURNS VARCHAR
AS $_$
    SELECT coalesce($1,'');
$_$ LANGUAGE sql IMMUTABLE;
