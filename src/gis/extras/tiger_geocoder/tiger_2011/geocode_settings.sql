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
SELECT tiger.SetSearchPathForInstall('tiger');

CREATE OR REPLACE FUNCTION install_geocode_settings() 
	RETURNS void AS
$$
DECLARE var_temp text;
BEGIN
	var_temp := tiger.SetSearchPathForInstall('tiger'); /** set set search path to have tiger in front **/
	IF NOT EXISTS(SELECT table_name FROM information_schema.columns WHERE table_schema = 'tiger' AND table_name = 'geocode_settings')  THEN
		CREATE TABLE geocode_settings(name text primary key, setting text, unit text, category text, short_desc text);
		GRANT SELECT ON geocode_settings TO public;
	END IF;
	--add missing settings
	INSERT INTO geocode_settings(name,setting,unit,category,short_desc)
		SELECT f.*
		FROM 
		(VALUES ('debug_geocode_address', 'false', 'boolean','debug', 'outputs debug information in notice log such as queries when geocode_addresss is called if true')
			, ('debug_geocode_intersection', 'false', 'boolean','debug', 'outputs debug information in notice log such as queries when geocode_intersection is called if true')
			, ('debug_normalize_address', 'false', 'boolean','debug', 'outputs debug information in notice log such as queries and intermediate expressions when normalize_address is called if true')
			, ('debug_reverse_geocode', 'false', 'boolean','debug', 'if true, outputs debug information in notice log such as queries and intermediate expressions when reverse_geocode')
			, ('reverse_geocode_numbered_roads', '0', 'integer','rating', 'For state and county highways, 0 - no preference in name, 1 - prefer the numbered highway name, 2 - prefer local state/county name')
			, ('use_pagc_address_parser', 'false', 'boolean','normalize', 'If set to true, will try to use the pagc_address normalizer instead of tiger built one')
		) f(name,setting,unit,category,short_desc)
		WHERE f.name NOT IN(SELECT name FROM geocode_settings);
END;
$$
language plpgsql;

SELECT install_geocode_settings(); /** create the table if it doesn't exist **/

CREATE OR REPLACE FUNCTION get_geocode_setting(setting_name text)
RETURNS text AS
$$
SELECT setting FROM geocode_settings WHERE name = $1;
$$
language sql STABLE;

CREATE OR REPLACE FUNCTION set_geocode_setting(setting_name text, setting_value text)
RETURNS text AS
$$
UPDATE geocode_settings SET setting = $2 WHERE name = $1
	RETURNING setting;
$$
language sql VOLATILE;
