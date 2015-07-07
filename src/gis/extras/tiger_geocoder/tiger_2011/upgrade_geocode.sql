--
-- PostGIS - Spatial Types for PostgreSQL
-- http://www.postgis.org
--
-- Copyright (C) 2010, 2011 Regina Obe and Leo Hsu
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Regina Obe and Leo Hsu <lr@pcorp.us>
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
\i utility/set_search_path.sql;
-- Tiger is where we're going to create the functions, but we need
-- the PostGIS functions/types which may be anywhere 
-- we'll assume user has postgis functions and other contribs as part of search path
-- the below call will put tiger schema in front so all objects in this script
-- will get created in search path
SELECT tiger.SetSearchPathForInstall('tiger');
--this is used currently for debugging
\i geocode_settings.sql
--this will fail if the column already exists which is fine
ALTER TABLE state_lookup ADD COLUMN statefp char(2);
UPDATE state_lookup SET statefp = lpad(st_code::text,2,'0') WHERE statefp IS NULL;
ALTER TABLE state_lookup ADD CONSTRAINT state_lookup_statefp_key UNIQUE(statefp);
CREATE INDEX idx_tiger_edges_countyfp ON edges USING btree(countyfp);
CREATE INDEX idx_tiger_faces_countyfp ON faces USING btree(countyfp);
CREATE INDEX tiger_place_the_geom_gist ON place USING gist(the_geom);
CREATE INDEX tiger_edges_the_geom_gist ON edges USING gist(the_geom);
CREATE INDEX tiger_state_the_geom_gist ON faces USING gist(the_geom);
DROP FUNCTION IF EXISTS reverse_geocode(geometry); /** changed to use default parameters **/
DROP FUNCTION IF EXISTS geocode_location(norm_addy); /** changed to include default parameter for restrict_geom**/
DROP FUNCTION IF EXISTS geocode(varchar); /** changed to include default parameter for max_results and restrict_geom**/
DROP FUNCTION IF EXISTS geocode(norm_addy); /** changed to include default parameter for max_results and restrict_geom **/
DROP FUNCTION IF EXISTS geocode(varchar, integer); /** changed to include default parameter for max_results and restrict_geom **/
DROP FUNCTION IF EXISTS geocode(norm_addy,integer); /** changed to include default parameter for max_results and restrict_geom **/
DROP FUNCTION IF EXISTS geocode_address(norm_addy); /** changed to include default parameter for max_results **/
DROP FUNCTION IF EXISTS geocode_address(norm_addy,integer); /** changed to include default parameter for max_results and restrict_geom **/
DROP FUNCTION IF EXISTS interpolate_from_address(integer, character varying, character varying, geometry); /** changed to use default args and added offset and side **/
DROP FUNCTION IF EXISTS interpolate_from_address(integer, integer, integer, geometry); /**don't need this since got collapes into varchar version **/

-- this will fail if already exists, that is fine.  can't use IF NOT EXISTS until 9.1
SELECT tiger.SetSearchPathForInstall('tiger');
CREATE TABLE addrfeat
(
  gid serial not null primary key,
  tlid bigint,
  statefp character varying(2),
  aridl character varying(22),
  aridr character varying(22),
  linearid character varying(22),
  fullname character varying(100),
  lfromhn character varying(12),
  ltohn character varying(12),
  rfromhn character varying(12),
  rtohn character varying(12),
  zipl character varying(5),
  zipr character varying(5),
  edge_mtfcc character varying(5),
  parityl character varying(1),
  parityr character varying(1),
  plus4l character varying(4),
  plus4r character varying(4),
  lfromtyp character varying(1),
  ltotyp character varying(1),
  rfromtyp character varying(1),
  rtotyp character varying(1),
  offsetl character varying(1),
  offsetr character varying(1),
  the_geom geometry,
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'LINESTRING'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269)
);
CREATE INDEX idx_addrfeat_geom_gist ON addrfeat USING gist(geom );
CREATE INDEX idx_addrfeat_tlid ON addrfeat USING btree(tlid);
CREATE INDEX idx_addrfeat_zipl ON addrfeat USING btree(zipl);
CREATE INDEX idx_addrfeat_zipr ON addrfeat USING btree(zipr);

-- TODO: Put in logic to update lookup tables as they change.  street_type_lookup has changed since initial release --
CREATE TABLE zcta5
(
  gid serial NOT NULL,
  statefp character varying(2),
  zcta5ce character varying(5),
  classfp character varying(2),
  mtfcc character varying(5),
  funcstat character varying(1),
  aland double precision,
  awater double precision,
  intptlat character varying(11),
  intptlon character varying(12),
  partflg character varying(1),
  the_geom geometry,
  CONSTRAINT uidx_tiger_zcta5_gid UNIQUE (gid),
  CONSTRAINT enforce_dims_the_geom CHECK (st_ndims(the_geom) = 2),
  CONSTRAINT enforce_geotype_the_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
  CONSTRAINT enforce_srid_the_geom CHECK (st_srid(the_geom) = 4269),
  CONSTRAINT pk_tiger_zcta5_zcta5ce PRIMARY KEY (zcta5ce,statefp)
 );

ALTER TABLE street_type_lookup ALTER COLUMN abbrev  TYPE varchar(50);
ALTER TABLE street_type_lookup ALTER COLUMN name  TYPE varchar(50);
ALTER TABLE street_type_lookup ADD COLUMN is_hw boolean NOT NULL DEFAULT false;
DROP FUNCTION IF EXISTS rate_attributes(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying);
DROP FUNCTION IF EXISTS rate_attributes(character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying, character varying);

--ALTER TABLE tiger.addr ALTER tlid TYPE bigint;
ALTER TABLE featnames ALTER COLUMN tlid SET NOT NULL;
ALTER TABLE county ALTER COLUMN statefp SET NOT NULL;
ALTER TABLE edges ALTER COLUMN tlid SET NOT NULL;
ALTER TABLE addr ALTER COLUMN tlid SET NOT NULL;
BEGIN;
-- Type used to pass around a normalized address between functions
-- This is s bit dangerous since it could potentially drop peoples tables
-- TODO: put in logic to check if any tables have norm_addy and don't drop if they do
-- Remarking this out for now since we aren't changing norm_addy anyway
/*DROP TYPE IF EXISTS norm_addy CASCADE; 
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
    parsed BOOLEAN); */
-- prefix and suffix street names for numbered highways
CREATE TEMPORARY TABLE temp_types AS
SELECT name, abbrev, true
    FROM (VALUES
        ('CAM', 'Cam'),
        ('CAM.', 'Cam'),
        ('CAMINO', 'Cam'),
        ('CO HWY', 'Co Hwy'),
        ('COUNTY HWY', 'Co Hwy'),
        ('COUNTY HIGHWAY', 'Co Hwy'),
        ('COUNTY HIGH WAY', 'Co Hwy'),
        ('COUNTY ROAD', 'Co Rd'),
        ('COUNTY RD', 'Co Rd'),
        ('CO RD', 'Co Rd'),
        ('CORD', 'Co Rd'),
        ('CO RTE', 'Co Rte'),
        ('COUNTY ROUTE', 'Co Rte'),
        ('CO ST AID HWY', 'Co St Aid Hwy'),
        ('EXP', 'Expy'),
        ('EXPR', 'Expy'),
        ('EXPRESS', 'Expy'),
        ('EXPRESSWAY', 'Expy'),
        ('EXPW', 'Expy'),
        ('EXPY', 'Expy'),
        ('FARM RD', 'Farm Rd'),
        ('FIRE RD', 'Fire Rd'),
        ('FOREST RD', 'Forest Rd'),
        ('FOREST ROAD', 'Forest Rd'),
        ('FOREST RTE', 'Forest Rte'),
        ('FOREST ROUTE', 'Forest Rte'),
        ('FREEWAY', 'Fwy'),
        ('FREEWY', 'Fwy'),
        ('FRWAY', 'Fwy'),
        ('FRWY', 'Fwy'),
        ('FWY', 'Fwy'),
        ('HIGHWAY', 'Hwy'),
        ('HIGHWY', 'Hwy'),
        ('HIWAY', 'Hwy'),
        ('HIWY', 'Hwy'),
        ('HWAY', 'Hwy'),
        ('HWY', 'Hwy'),
        ('I', 'I-'),
        ('I-', 'I-'),
        ('INTERSTATE', 'I-'),
        ('INTERSTATE ROUTE', 'I-'),
        ('INTERSTATE RTE', 'I-'),
        ('INTERSTATE RTE.', 'I-'),
        ('INTERSTATE RT', 'I-'),
        ('LOOP', 'Loop'),
        ('ROUTE', 'Rte'),
        ('RTE', 'Rte'),
        ('RT', 'Rte'),
        ('STATE HWY', 'State Hwy'),
        ('STATE HIGHWAY', 'State Hwy'),
        ('STATE HIGH WAY', 'State Hwy'),
        ('STATE RD', 'State Rd'),
        ('STATE ROAD', 'State Rd'),
        ('STATE ROUTE', 'State Rte'),
        ('STATE RTE', 'State Rte'),
        ('TPK', 'Tpke'),
        ('TPKE', 'Tpke'),
        ('TRNPK', 'Tpke'),
        ('TRPK', 'Tpke'),
        ('TURNPIKE', 'Tpke'),
        ('TURNPK', 'Tpke'),
        ('US HWY', 'US Hwy'),
        ('US HIGHWAY', 'US Hwy'),
        ('US HIGH WAY', 'US Hwy'),
        ('U.S.', 'US Hwy'),
        ('US RTE', 'US Rte'),
        ('US ROUTE', 'US Rte'),
        ('US RT', 'US Rte'),
        ('USFS HWY', 'USFS Hwy'),
        ('USFS HIGHWAY', 'USFS Hwy'),
        ('USFS HIGH WAY', 'USFS Hwy'),
        ('USFS RD', 'USFS Rd'),
        ('USFS ROAD', 'USFS Rd')
           ) t(name, abbrev)
           WHERE t.name NOT IN(SELECT name FROM street_type_lookup);
           
DELETE FROM street_type_lookup WHERE name IN(SELECT name FROM temp_types);         
INSERT INTO street_type_lookup (name, abbrev, is_hw) 
SELECT name, abbrev, true
    FROM temp_types As t
           WHERE t.name NOT IN(SELECT name FROM street_type_lookup);
DROP TABLE temp_types;           
DELETE FROM street_type_lookup WHERE name = 'FOREST';
UPDATE street_type_lookup SET is_hw = false WHERE abbrev = 'Loop';

CREATE TEMPORARY TABLE temp_types AS
SELECT name, abbrev
    FROM (VALUES 
 ('LOOP', 'Loop'),
 ('SERVICE DRIVE', 'Svc Dr'),
 ('SERVICE DR', 'Svc Dr'),
 ('SERVICE ROAD', 'Svc Rd'),
 ('SERVICE RD', 'Svc Rd') 
    ) t(name, abbrev);
 
DELETE FROM street_type_lookup WHERE name IN(SELECT name FROM temp_types);         
INSERT INTO street_type_lookup (name, abbrev, is_hw) 
SELECT name, abbrev, false
    FROM temp_types As t
           WHERE t.name NOT IN(SELECT name FROM street_type_lookup);
 
SELECT tiger.SetSearchPathForInstall('tiger');
-- new census loader
\i census_loader.sql
--create parent tables for census
-- if they do not exist
SELECT create_census_base_tables();
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
\i geocode/geocode.sql

-- Reverse Geocode API, called by user
\i geocode/geocode_intersection.sql
\i geocode/reverse_geocode.sql
\i geocode/census_tracts_functions.sql
COMMIT;
-- Tiger to PostGIS Topology
-- only useable if you have topology installed
\i topology/tiger_topology_loader.sql

-- install missing indexes
\echo 'Installing missing indexes - this might take a while so be patient ..'
SELECT install_missing_indexes();
\a
--\o 'drop_dup_feat_create_index.sql'
--\i generate_drop_dupe_featnames.sql
\o 
--\i drop_dup_feat_create_index.sql
\echo 'Missing index Install completed'