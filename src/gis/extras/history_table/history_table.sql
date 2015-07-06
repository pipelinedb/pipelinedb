-- PUBLIC FUNCTIONS --

create or replace function postgis_install_history() returns void as
$$
--this function creates a table that will hold some interesting values for managing history tables
--later functions will be added
BEGIN

	IF exists(select 1 FROM information_schema.tables WHERE table_name = 'historic_information') = true THEN
		raise notice 'The table historic_information already exists. Could not create it.';
	ELSE
	execute 'create table historic_information(table_id serial not null,table_name varchar(100) not null,primary_field varchar(100) not null, geometry_field varchar(100) not null, constraint history_tables_pk primary key(table_id,table_name));';
	END IF;

END
$$
language 'plpgsql';

--end build_history_table


--im open to suggestions for the names of the functions.
--just realized that one is build_history_table and the other create_...
CREATE OR REPLACE FUNCTION postgis_enable_history(p_schema text,p_table text,p_geometry_field text) returns boolean as
$$
DECLARE

v_current_table text;
v_history_table text;

v_geometry_type text; --checks for the type of p_geometry_field
v_dimensions integer; --checks for the ndims in p_geometry_field
v_srid integer;       --checks for the srid in p_geometry_field
v_gid text;           --checks the name of the pk column in p_table

--SQL statement that will create the historic table
v_table_sql text;

--SQL statement that will perform an update on geometry_columns
v_update_geometry_sql text; 

--SQL statement that will perform an update on historic_tables
v_update_history_sql text;

BEGIN

	--determines the name of current table
	v_current_table:= p_schema || '.' || p_table;
	--determines the name of historic table
	v_history_table:= p_schema || '.' || p_table || '_history';

	--sql to determine the values of geometry type, srid and ndims
	v_geometry_type:= (SELECT "type" FROM public.geometry_columns WHERE f_table_schema = p_schema AND f_table_name = p_table AND f_geometry_column = p_geometry_field);
	v_dimensions:= (SELECT coord_dimension FROM public.geometry_columns WHERE f_table_schema = p_schema AND f_table_name = p_table AND f_geometry_column = p_geometry_field);
	v_srid:= (SELECT srid FROM public.geometry_columns WHERE f_table_schema = p_schema AND f_table_name = p_table AND f_geometry_column = p_geometry_field);
	v_gid:= (SELECT column_name FROM information_schema.key_column_usage WHERE table_schema = p_schema AND table_name = p_table);
	--end sql
	
	--generate sql for creating the historic table
	v_table_sql:= 'CREATE TABLE ' || v_history_table || 
	'(' ||
	'history_id serial not null,' ||
	'date_added timestamp not null default now(),' ||
	'date_deleted timestamp default null,' ||
	'last_operation varchar(30) not null,' ||
	'active_user varchar(90) not null default CURRENT_USER,' ||
	'current_version text not null,' ||
	'like ' || v_current_table || ',' ||
	'CONSTRAINT ' || p_table || '_history_pk primary key(history_id));';
	--end sql

	--update geometry columns
	v_update_geometry_sql:='INSERT INTO public.geometry_columns(f_table_catalog,f_table_schema,f_table_name,f_geometry_column,coord_dimension,srid,type) values (' ||
	quote_literal('') || ',' ||
	quote_literal(p_schema) || ',' ||
	quote_literal(p_table || '_history') || ',' ||
	quote_literal(p_geometry_field) || ',' ||
	v_dimensions::text || ',' ||
	v_srid::text || ',' ||
	quote_literal(v_geometry_type) || ');';
	--end update geometry_columns
	
	--insert into historic_tables
	v_update_history_sql:='INSERT INTO public.historic_information(table_id,table_name,primary_field,geometry_field) VALUES (' ||
	'DEFAULT,' ||
	quote_literal(v_history_table) || ',' || 
	quote_literal(v_gid) || ',' ||
	quote_literal(p_geometry_field) || ');';
	--end update historic tables

	execute v_table_sql;
	execute v_update_geometry_sql;
	execute v_update_history_sql;

	execute _postgis_add_insert_rule(p_schema,p_table,v_gid);
	execute _postgis_add_delete_rule(p_schema,p_table,v_gid);
	execute _postgis_add_update_rule(p_schema,p_table,v_gid);
	execute _postgis_create_history_indexes(p_schema,p_table,p_geometry_field);

	return true;
	
END
$$
language 'plpgsql';

--end create_history_table

-- PRIVATE FUNCTIONS --

--add_insert_rule
CREATE OR REPLACE FUNCTION _postgis_add_insert_rule(p_schema text,p_table text,p_gid_field text) returns void as
$$
DECLARE

v_sql text;

BEGIN

	v_sql:= 'CREATE OR REPLACE RULE ' || p_table || '_history_insert as ON INSERT TO ' || p_schema || '.' || p_table ||
	' DO (' ||
	'INSERT INTO ' || p_schema || '.' || p_table || '_history VALUES(' ||
	'DEFAULT,' || --history_id nextval()
	'DEFAULT,' || --date_added now()
	'NULL,' || --date_deleted 
	quote_literal('INSERT') || ',' || --operation
	'DEFAULT,' ||
	'NEW.' || p_gid_field || ',' ||
	'NEW.*));';

	execute v_sql;

END
$$
language 'plpgsql';	
--end add_insert_rule

--add_update_rule
CREATE OR REPLACE FUNCTION _postgis_add_update_rule(p_schema text,p_table text,p_gid_field text) returns void as
$$
DECLARE

v_sql text;

BEGIN

	v_sql:= 'CREATE OR REPLACE RULE ' || p_table || '_history_update as ON UPDATE TO ' || p_schema || '.' || p_table ||
	' DO (' ||
	'UPDATE ' || p_schema || '.' || p_table || '_history SET ' ||
	'date_deleted = now(),' ||
	'active_user = CURRENT_USER,' ||
	'current_version = ' || 'NEW.' || p_gid_field || ',' ||
	'last_operation = ' || quote_literal('UPDATE') ||
	'WHERE ' || p_gid_field || ' = OLD.' || p_gid_field || ';' || -- end of the update statement
	'INSERT INTO ' || p_schema || '.' || p_table || '_history VALUES (' ||
	'DEFAULT,' || --history_id nextval()
	'DEFAULT,' || --date_added now()
	'NULL,' || --date_deleted 
	quote_literal('INSERT') || ',' || --operation
	'DEFAULT,' ||
	'NEW.' || p_gid_field || ',' ||
	'NEW.*););';

	execute v_sql;

END
$$
language 'plpgsql';
--end add_update_rule

--add_delete_rule
CREATE OR REPLACE FUNCTION _postgis_add_delete_rule(p_schema text,p_table text,p_gid_field text) returns void as
$$
DECLARE

v_sql text;

BEGIN

	v_sql:= 'CREATE OR REPLACE RULE ' || p_table || '_history_delete as ON DELETE TO ' || p_schema || '.' || p_table ||
	' DO (' ||
	'UPDATE ' || p_schema || '.' || p_table || '_history SET ' ||
	'date_deleted = now(),' ||
	'active_user = CURRENT_USER,' ||
	'current_version = ' || quote_literal('-9999') || ',' ||
	'last_operation = ' || quote_literal('DELETED') ||
	'WHERE ' || p_gid_field || ' = OLD.' || p_gid_field || ');';

	execute v_sql;

END
$$
language 'plpgsql';
--end ad__delete_rule

--create indexes function
CREATE OR REPLACE FUNCTION _postgis_create_history_indexes(p_schema text, p_table text, p_geometry_field text) returns void as
$$
DECLARE

v_geomindex_sql text;
v_dateindex_sql text;
v_userindex_sql text;
v_operindex_sql text;

BEGIN
	v_geomindex_sql:= 'CREATE INDEX ' || 'idx_' || p_table || '_geometry_history' ||
	' ON ' || p_schema || '.' || p_table || '_history USING GIST(' || p_geometry_field || ');';

	v_dateindex_sql:= 'CREATE INDEX ' || 'idx_' || p_table || '_date_history' ||
	' ON ' || p_schema || '.' || p_table || '_history (date_added,date_deleted);';

	v_userindex_sql:= 'CREATE INDEX ' || 'idx_' || p_table || '_user_history' ||
	' ON ' || p_schema || '.' || p_table || '_history(active_user);';

	v_operindex_sql:= 'CREATE INDEX ' || 'idx_' || p_table || '_oper_history' ||
	' ON ' || p_schema || '.' || p_table || '_history (last_operation);';

	execute v_geomindex_sql;
	execute v_dateindex_sql;
	execute v_userindex_sql;
	execute v_operindex_sql;

END
$$
language 'plpgsql'
--end create indexes

/*TODO LIST:

CREATE A FUNCTION THAT WILL DROP A CERTAIN HISTORIC TABLE AND REMOVE ITS ITENS FROM GEOMERTY_COLUMNS AND HISTORIC_INFORMATION
CREATE A FUNCTION TO POPULATE ALL THE EXISTING RECORDS TO THE HISTORIC TABLE, AS A INSERT
*/