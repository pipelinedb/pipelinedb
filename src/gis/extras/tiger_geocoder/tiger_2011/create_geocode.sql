--
-- PostGIS - Spatial Types for PostgreSQL
-- http://www.postgis.org
--
-- Copyright (C) 2010, 2011 Regina Obe and Leo Hsu
-- Copyright (C) 2008 Stephen Frost (et al) 
-- reintegrated back into PostGIS code base from Steven's git (http://www.snowman.net/git/tiger_geocoder/)
-- Copyright Refractions Research
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- Tiger is where we're going to create the functions, but we need
-- the PostGIS functions/types which are in public.
\i utility/set_search_path.sql;
SELECT tiger.SetSearchPathForInstall('tiger');
\i tables/lookup_tables_2011.sql
\i geocode_settings.sql
--SET search_path TO tiger,public;
BEGIN;
-- Type used to pass around a normalized address between functions
-- This is s bit dangerous since it could potentially drop peoples tables
-- TODO: put in logic to check if any tables have norm_addy and don't drop if they do
DROP TYPE IF EXISTS norm_addy CASCADE;
CREATE TYPE norm_addy AS (
    address INTEGER,
    preDirAbbrev VARCHAR,
    streetName VARCHAR,
    streetTypeAbbrev VARCHAR,
    postDirAbbrev VARCHAR,
    internal VARCHAR,
    location VARCHAR,
    stateAbbrev VARCHAR,
    zip VARCHAR,
    parsed BOOLEAN);

-- System/General helper functions
\i utility/utmzone.sql
\i utility/cull_null.sql
\i utility/nullable_levenshtein.sql
\i utility/levenshtein_ignore_case.sql

---- Address normalizer
-- General helpers
\i normalize/end_soundex.sql
\i normalize/count_words.sql
\i normalize/state_extract.sql
\i normalize/get_last_words.sql
-- Location extraction/normalization helpers
\i normalize/location_extract_countysub_exact.sql
\i normalize/location_extract_countysub_fuzzy.sql
\i normalize/location_extract_place_exact.sql
\i normalize/location_extract_place_fuzzy.sql
\i normalize/location_extract.sql
-- Normalization API, called by geocode mainly.
\i normalize/normalize_address.sql
\i normalize/pprint_addy.sql

-- PAGC normalizer drop in replacement.
\i pagc_normalize/pagc_tables.sql
\i pagc_normalize/pagc_normalize_address.sql

---- Geocoder functions
-- General helpers
\i geocode/other_helper_functions.sql
\i geocode/rate_attributes.sql
\i geocode/includes_address.sql
\i geocode/interpolate_from_address.sql
-- Actual lookups/geocoder helpers
\i geocode/geocode_address.sql
\i geocode/geocode_location.sql
-- Geocode API, called by user
\i geocode/geocode_intersection.sql
\i geocode/geocode.sql

-- Reverse Geocode API, called by user
\i geocode/reverse_geocode.sql
\i geocode/census_tracts_functions.sql
COMMIT;

-- Tiger to PostGIS Topology
-- only useable if you have topology installed
\i topology/tiger_topology_loader.sql