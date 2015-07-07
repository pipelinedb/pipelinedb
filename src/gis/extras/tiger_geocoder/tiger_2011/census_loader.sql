--
-- PostGIS - Spatial Types for PostgreSQL
-- http://www.postgis.org
--
-- Copyright (C) 2010, 2011 Regina Obe and Leo Hsu 
-- Paragon Corporation
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Regina Obe and Leo Hsu <lr@pcorp.us>
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--SET search_path TO tiger,public;
SELECT tiger.SetSearchPathForInstall('tiger');
CREATE OR REPLACE FUNCTION create_census_base_tables() 
	RETURNS text AS
$$
DECLARE var_temp text; 
BEGIN
var_temp := tiger.SetSearchPathForInstall('tiger');
IF NOT EXISTS(SELECT table_name FROM information_schema.columns WHERE table_schema = 'tiger' AND column_name = 'tract_id' AND table_name = 'tract')  THEN
	-- census block group/tracts parent tables not created yet or an older version -- drop old if not in use, create new structure
	DROP TABLE IF EXISTS tiger.tract;
	CREATE TABLE tract
	(
	  gid serial NOT NULL,
	  statefp varchar(2),
	  countyfp varchar(3),
	  tractce varchar(6),
	  tract_id varchar(11) PRIMARY KEY,
	  name varchar(7),
	  namelsad varchar(20),
	  mtfcc varchar(5),
	  funcstat varchar(1),
	  aland double precision,
	  awater double precision,
	  intptlat varchar(11),
	  intptlon varchar(12),
	  the_geom geometry,
	  CONSTRAINT enforce_dims_geom CHECK (st_ndims(the_geom) = 2),
	  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
	  CONSTRAINT enforce_srid_geom CHECK (st_srid(the_geom) = 4269)
	);
	
	DROP TABLE IF EXISTS tiger.tabblock;
	CREATE TABLE tabblock
	(
	  gid serial NOT NULL,
	  statefp varchar(2),
	  countyfp varchar(3),
	  tractce varchar(6),
	  blockce varchar(4),
	  tabblock_id varchar(16) PRIMARY KEY,
	  name varchar(20),
	  mtfcc varchar(5),
	  ur varchar(1),
	  uace varchar(5),
	  funcstat varchar(1),
	  aland double precision,
	  awater double precision,
	  intptlat varchar(11),
	  intptlon varchar(12),
	  the_geom geometry,
	  CONSTRAINT enforce_dims_geom CHECK (st_ndims(the_geom) = 2),
	  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
	  CONSTRAINT enforce_srid_geom CHECK (st_srid(the_geom) = 4269)
	);

	DROP TABLE IF EXISTS tiger.bg;
	CREATE TABLE bg
	(
	  gid serial NOT NULL,
	  statefp varchar(2),
	  countyfp varchar(3),
	  tractce varchar(6),
	  blkgrpce varchar(1),
	  bg_id varchar(12) PRIMARY KEY,
	  namelsad varchar(13),
	  mtfcc varchar(5),
	  funcstat varchar(1),
	  aland double precision,
	  awater double precision,
	  intptlat varchar(11),
	  intptlon varchar(12),
	  the_geom geometry,
	  CONSTRAINT enforce_dims_geom CHECK (st_ndims(the_geom) = 2),
	  CONSTRAINT enforce_geotype_geom CHECK (geometrytype(the_geom) = 'MULTIPOLYGON'::text OR the_geom IS NULL),
	  CONSTRAINT enforce_srid_geom CHECK (st_srid(the_geom) = 4269)
	);
	COMMENT ON TABLE tiger.bg IS 'block groups';
END IF;

IF EXISTS(SELECT * FROM information_schema.columns WHERE table_schema = 'tiger' AND column_name = 'tabblock_id' AND table_name = 'tabblock' AND character_maximum_length < 16)  THEN -- size of name and tabblock_id fields need to be increased
    ALTER TABLE tiger.tabblock ALTER COLUMN name TYPE varchar(20);
    ALTER TABLE tiger.tabblock ALTER COLUMN tabblock_id TYPE varchar(16);
    RAISE NOTICE 'Size of tabblock_id and name are being incrreased';
END IF;
RETURN 'Tables already present';
END
$$
language 'plpgsql';

DROP FUNCTION IF EXISTS loader_generate_census(text[], text);
CREATE OR REPLACE FUNCTION loader_generate_census_script(param_states text[], os text)
  RETURNS SETOF text AS
$$
SELECT create_census_base_tables();
SELECT 
	loader_macro_replace(
		replace(
			loader_macro_replace(declare_sect
				, ARRAY['staging_fold', 'state_fold','website_root', 'psql', 'state_abbrev', 'data_schema', 'staging_schema', 'state_fips'], 
				ARRAY[variables.staging_fold, s.state_fold, variables.website_root, platform.psql, s.state_abbrev, variables.data_schema, variables.staging_schema, s.state_fips::text]
			), '/', platform.path_sep) || '
' ||
	-- State level files - if an override website is specified we use that instead of variable one
	array_to_string( ARRAY(SELECT 'cd ' || replace(variables.staging_fold,'/', platform.path_sep) || '
' || platform.wget || ' ' || COALESCE(lu.website_root_override,variables.website_root || '/' || upper(table_name)  ) || '/*_' || s.state_fips || '* --no-parent --relative --recursive --level=2 --accept=zip --mirror --reject=html 
'
|| 'cd ' ||  replace(variables.staging_fold,'/', platform.path_sep) || '/' || replace(replace(COALESCE(lu.website_root_override,variables.website_root || '/' || upper(table_name) ), 'http://', ''),'ftp://','')    || '
' || replace(platform.unzip_command, '*.zip', 'tl_*_' || s.state_fips || '*_' || table_name || '.zip ') || '
' ||loader_macro_replace(COALESCE(lu.pre_load_process || E'\n', '') || platform.loader || ' -' ||  lu.insert_mode || ' -s 4269 -g the_geom ' 
		|| CASE WHEN lu.single_geom_mode THEN ' -S ' ELSE ' ' END::text || ' -W "latin1" tl_' || variables.tiger_year || '_' || s.state_fips 
	|| '_' || lu.table_name || '.dbf tiger_staging.' || lower(s.state_abbrev) || '_' || lu.table_name || ' | '::text || platform.psql 
		|| COALESCE(E'\n' || 
			lu.post_load_process , '') , ARRAY['loader','table_name', 'lookup_name'], ARRAY[platform.loader, lu.table_name, lu.lookup_name ])
				FROM loader_lookuptables AS lu
				WHERE level_state = true AND load = true AND lookup_name IN('tract','bg','tabblock')
				ORDER BY process_order, lookup_name), E'\n') ::text 
	-- County Level files
	|| E'\n' ||
		array_to_string( ARRAY(SELECT 'cd ' || replace(variables.staging_fold,'/', platform.path_sep) || '
' || platform.wget || ' ' || COALESCE(lu.website_root_override,variables.website_root || '/' || upper(table_name)  ) || '/*_' || s.state_fips || '* --no-parent --relative --recursive --level=2 --accept=zip --mirror --reject=html 
'
|| 'cd ' ||  replace(variables.staging_fold,'/', platform.path_sep) || '/' || replace(replace(COALESCE(lu.website_root_override,variables.website_root || '/' || upper(table_name)  || '/'), 'http://', ''),'ftp://','')  || '
' || replace(platform.unzip_command, '*.zip', 'tl_*_' || s.state_fips || '*_' || table_name || '.zip ') || '
' || loader_macro_replace(COALESCE(lu.pre_load_process || E'\n', '') || COALESCE(county_process_command || E'\n','')
				|| COALESCE(E'\n' ||lu.post_load_process , '') , ARRAY['loader','table_name','lookup_name'], ARRAY[platform.loader  || CASE WHEN lu.single_geom_mode THEN ' -S' ELSE ' ' END::text, lu.table_name, lu.lookup_name ]) 
				FROM loader_lookuptables AS lu
				WHERE level_county = true AND load = true AND lookup_name IN('tract','bg','tabblock')
				ORDER BY process_order, lookup_name), E'\n') ::text 
	, ARRAY['psql', 'data_schema','staging_schema', 'staging_fold', 'state_fold', 'website_root', 'state_abbrev','state_fips'], 
	ARRAY[platform.psql,  variables.data_schema, variables.staging_schema, variables.staging_fold, s.state_fold,variables.website_root, s.state_abbrev, s.state_fips::text])
			AS shell_code
FROM loader_variables As variables
		CROSS JOIN (SELECT name As state, abbrev As state_abbrev, lpad(st_code::text,2,'0') As state_fips, 
			 lpad(st_code::text,2,'0') || '_' 
	|| replace(name, ' ', '_') As state_fold
FROM state_lookup) As s CROSS JOIN loader_platform As platform
WHERE $1 @> ARRAY[state_abbrev::text]      -- If state is contained in list of states input generate script for it
AND platform.os = $2  -- generate script for selected platform
;
$$
  LANGUAGE sql VOLATILE;
  
--update with census tract loading logic
DELETE FROM loader_lookuptables WHERE lookup_name IN('tract','tabblock','bg');           
INSERT INTO loader_lookuptables(process_order, lookup_name, table_name, load, level_county, level_state, single_geom_mode, insert_mode, pre_load_process, post_load_process, columns_exclude )
VALUES(10, 'tract', 'tract', true, false, true,false, 'c', 
'${psql} -c "CREATE TABLE ${data_schema}.${state_abbrev}_${lookup_name}(CONSTRAINT pk_${state_abbrev}_${lookup_name} PRIMARY KEY (tract_id) ) INHERITS(tiger.${lookup_name}); " ',
	'${psql} -c "ALTER TABLE ${staging_schema}.${state_abbrev}_${table_name} RENAME geoid TO tract_id;  SELECT loader_load_staged_data(lower(''${state_abbrev}_${table_name}''), lower(''${state_abbrev}_${lookup_name}'')); "
	${psql} -c "CREATE INDEX ${data_schema}_${state_abbrev}_${lookup_name}_the_geom_gist ON ${data_schema}.${state_abbrev}_${lookup_name} USING gist(the_geom);"
	${psql} -c "VACUUM ANALYZE ${data_schema}.${state_abbrev}_${lookup_name};"
	${psql} -c "ALTER TABLE ${data_schema}.${state_abbrev}_${lookup_name} ADD CONSTRAINT chk_statefp CHECK (statefp = ''${state_fips}'');"', ARRAY['gid']);

INSERT INTO loader_lookuptables(process_order, lookup_name, table_name, load, level_county, level_state, single_geom_mode, insert_mode, pre_load_process, post_load_process, columns_exclude )
VALUES(11, 'tabblock', 'tabblock', true, false, true,false, 'c', 
'${psql} -c "CREATE TABLE ${data_schema}.${state_abbrev}_${lookup_name}(CONSTRAINT pk_${state_abbrev}_${lookup_name} PRIMARY KEY (tabblock_id)) INHERITS(tiger.${lookup_name});" ',
'${psql} -c "ALTER TABLE ${staging_schema}.${state_abbrev}_${table_name} RENAME geoid TO tabblock_id;  SELECT loader_load_staged_data(lower(''${state_abbrev}_${table_name}''), lower(''${state_abbrev}_${lookup_name}''), ''{gid, statefp10, countyfp10, tractce10, blockce10,suffix1ce,blockce,tractce}''::text[]); "
${psql} -c "ALTER TABLE ${data_schema}.${state_abbrev}_${lookup_name} ADD CONSTRAINT chk_statefp CHECK (statefp = ''${state_fips}'');"
${psql} -c "CREATE INDEX ${data_schema}_${state_abbrev}_${lookup_name}_the_geom_gist ON ${data_schema}.${state_abbrev}_${lookup_name} USING gist(the_geom);"
${psql} -c "vacuum analyze ${data_schema}.${state_abbrev}_${lookup_name};"', ARRAY['gid']);

INSERT INTO loader_lookuptables(process_order, lookup_name, table_name, load, level_county, level_state, single_geom_mode, insert_mode, pre_load_process, post_load_process, columns_exclude )
VALUES(12, 'bg', 'bg', true,false, true,false, 'c', 
'${psql} -c "CREATE TABLE ${data_schema}.${state_abbrev}_${lookup_name}(CONSTRAINT pk_${state_abbrev}_${lookup_name} PRIMARY KEY (bg_id)) INHERITS(tiger.${lookup_name});" ',
'${psql} -c "ALTER TABLE ${staging_schema}.${state_abbrev}_${table_name} RENAME geoid TO bg_id;  SELECT loader_load_staged_data(lower(''${state_abbrev}_${table_name}''), lower(''${state_abbrev}_${lookup_name}'')); "
${psql} -c "ALTER TABLE ${data_schema}.${state_abbrev}_${lookup_name} ADD CONSTRAINT chk_statefp CHECK (statefp = ''${state_fips}'');"
${psql} -c "CREATE INDEX ${data_schema}_${state_abbrev}_${lookup_name}_the_geom_gist ON ${data_schema}.${state_abbrev}_${lookup_name} USING gist(the_geom);"
${psql} -c "vacuum analyze ${data_schema}.${state_abbrev}_${lookup_name};"', ARRAY['gid']);


SELECT create_census_base_tables();
