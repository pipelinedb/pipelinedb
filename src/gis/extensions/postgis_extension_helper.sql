-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
----
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.net
--
-- Copyright (C) 2011 Regina Obe <lr@pcorp.us>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- Author: Regina Obe <lr@pcorp.us>
--  
-- This is a suite of SQL helper functions for use during a PostGIS extension install/upgrade
-- The functions get uninstalled after the extention install/upgrade process
---------------------------
-- postgis_extension_remove_objects: This function removes objects of a particular class from an extension
-- this is needed because there is no ALTER EXTENSION DROP FUNCTION/AGGREGATE command
-- and we can't CREATE OR REPALCe functions whose signatures have changed and we can drop them if they are part of an extention
-- So we use this to remove it from extension first before we drop
CREATE OR REPLACE FUNCTION postgis_extension_remove_objects(param_extension text, param_type text)
  RETURNS boolean AS
$$
DECLARE 
	var_sql text := '';
	var_r record;
	var_result boolean := false;
	var_class text := '';
	var_is_aggregate boolean := false;
	var_sql_list text := '';
BEGIN
		var_class := CASE WHEN lower(param_type) = 'function' OR lower(param_type) = 'aggregate' THEN 'pg_proc' ELSE '' END; 
		var_is_aggregate := CASE WHEN lower(param_type) = 'aggregate' THEN true ELSE false END;
		var_sql_list := 'SELECT ''ALTER EXTENSION '' || e.extname || '' DROP '' || $3 || '' '' || COALESCE(proc.proname || ''('' || oidvectortypes(proc.proargtypes) || '')'',typ.typname, cd.relname, op.oprname, 
				cs.typname || '' AS '' || ct.typname || '') '', opcname, opfname) || '';'' AS remove_command
		FROM pg_depend As d INNER JOIN pg_extension As e
			ON d.refobjid = e.oid INNER JOIN pg_class As c ON
				c.oid = d.classid
				LEFT JOIN pg_proc AS proc ON proc.oid = d.objid
				LEFT JOIN pg_type AS typ ON typ.oid = d.objid
				LEFT JOIN pg_class As cd ON cd.oid = d.objid
				LEFT JOIN pg_operator As op ON op.oid = d.objid
				LEFT JOIN pg_cast AS ca ON ca.oid = d.objid
				LEFT JOIN pg_type AS cs ON ca.castsource = cs.oid
				LEFT JOIN pg_type AS ct ON ca.casttarget = ct.oid
				LEFT JOIN pg_opclass As oc ON oc.oid = d.objid
				LEFT JOIN pg_opfamily As ofa ON ofa.oid = d.objid
		WHERE d.deptype = ''e'' and e.extname = $1 and c.relname = $2 AND COALESCE(proc.proisagg, false) = $4;';
		FOR var_r IN EXECUTE var_sql_list  USING param_extension, var_class, param_type, var_is_aggregate
        LOOP
            var_sql := var_sql || var_r.remove_command || ';';
        END LOOP;
        IF var_sql > '' THEN
            EXECUTE var_sql;
            var_result := true;
        END IF;
        RETURN var_result;
END;
$$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION postgis_extension_drop_if_exists(param_extension text, param_statement text)
  RETURNS boolean AS
$$
DECLARE 
	var_sql_ext text := 'ALTER EXTENSION ' || quote_ident(param_extension) || ' ' || replace(param_statement, 'IF EXISTS', '');
	var_result boolean := false;
BEGIN
	BEGIN
		EXECUTE var_sql_ext;
		var_result := true;
	EXCEPTION
		WHEN OTHERS THEN
			--this is to allow ignoring if the object does not exist in extension
			var_result := false;
	END;
	RETURN var_result;
END;
$$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION postgis_extension_AddToSearchPath(a_schema_name varchar)
RETURNS text
AS
$$
DECLARE
	var_result text;
	var_cur_search_path text;
BEGIN
	SELECT reset_val INTO var_cur_search_path FROM pg_settings WHERE name = 'search_path';
	IF var_cur_search_path LIKE '%' || quote_ident(a_schema_name) || '%' THEN
		var_result := a_schema_name || ' already in database search_path';
	ELSE
		var_cur_search_path := var_cur_search_path || ', '
                        || quote_ident(a_schema_name); 
		EXECUTE 'ALTER DATABASE ' || quote_ident(current_database())
                              || ' SET search_path = ' || var_cur_search_path;
		var_result := a_schema_name || ' has been added to end of database search_path ';
	END IF;

	EXECUTE 'SET search_path = ' || var_cur_search_path;
  
  RETURN var_result;
END
$$
LANGUAGE 'plpgsql' VOLATILE STRICT;
