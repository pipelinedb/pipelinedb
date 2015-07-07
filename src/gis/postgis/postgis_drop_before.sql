-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--
-- PostGIS - Spatial Types for PostgreSQL
-- http://postgis.net
--
-- Copyright (C) 2011-2012 Sandro Santilli <strk@keybit.net>
-- Copyright (C) 2010-2013 Regina Obe <lr@pcorp.us>
-- Copyright (C) 2009      Paul Ramsey <pramsey@cleverelephant.ca>
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- This file contains drop commands for obsoleted items that need
-- to be dropped _before_ upgrade of old functions.
-- Changes to this file affect postgis_upgrade*.sql script.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

DROP FUNCTION IF EXISTS AddGeometryColumn(varchar,varchar,varchar,varchar,integer,varchar,integer,boolean);
DROP FUNCTION IF EXISTS ST_MakeEnvelope(float8, float8, float8, float8);
--changed name of prec arg to be consistent with ST_AsGML/KML
DROP FUNCTION IF EXISTS ST_AsX3D(geometry, integer, integer); 
--changed name of arg: http://trac.osgeo.org/postgis/ticket/1606
DROP FUNCTION IF EXISTS UpdateGeometrySRID(varchar,varchar,varchar,varchar,integer);

--deprecated and removed in 2.1 
-- Hack to fix 2.0 naming 
-- We can't just drop it since its bound to opclass
-- See ticket 2279 for why we need to do this
-- We can get rid of this DO code when 3.0 comes along
DO  language 'plpgsql' $$ 
BEGIN
	-- fix geometry ops --
	IF EXISTS(SELECT oprname from pg_operator where oprname = '&&' AND oprrest::text = 'geometry_gist_sel_2d') THEN
	--it is bound to old name, drop new, rename old to new, install will fix body of code
		DROP FUNCTION IF EXISTS gserialized_gist_sel_2d(internal, oid, internal, int4) ;
		ALTER FUNCTION geometry_gist_sel_2d(internal, oid, internal, int4) RENAME TO gserialized_gist_sel_2d;
	END IF;
	IF EXISTS(SELECT oprname from pg_operator where oprname = '&&' AND oprjoin::text = 'geometry_gist_joinsel_2d') THEN
	--it is bound to old name, drop new, rename old to new,  install will fix body of code
		DROP FUNCTION IF EXISTS gserialized_gist_joinsel_2d(internal, oid, internal, smallint) ;
		ALTER FUNCTION geometry_gist_joinsel_2d(internal, oid, internal, smallint) RENAME TO gserialized_gist_joinsel_2d;
	END IF;
	-- fix geography ops --
	IF EXISTS(SELECT oprname from pg_operator where oprname = '&&' AND oprrest::text = 'geography_gist_selectivity') THEN
	--it is bound to old name, drop new, rename old to new, install will fix body of code
		DROP FUNCTION IF EXISTS gserialized_gist_sel_nd(internal, oid, internal, int4) ;
		ALTER FUNCTION geography_gist_selectivity(internal, oid, internal, int4) RENAME TO gserialized_gist_sel_nd;
	END IF;
	
	IF EXISTS(SELECT oprname from pg_operator where oprname = '&&' AND oprjoin::text = 'geography_gist_join_selectivity') THEN
	--it is bound to old name, drop new, rename old to new, install will fix body of code
		DROP FUNCTION IF EXISTS gserialized_gist_joinsel_nd(internal, oid, internal, smallint) ;
		ALTER FUNCTION geography_gist_join_selectivity(internal, oid, internal, smallint) RENAME TO gserialized_gist_joinsel_nd;
	END IF;
END;
$$ ;

-- Going from multiple functions to default args
-- Need to drop old multiple variants to not get in trouble.
DROP FUNCTION IF EXISTS ST_AsTWKB(geometry,int4);
DROP FUNCTION IF EXISTS ST_AsTWKB(geometry,int4,int8);
DROP FUNCTION IF EXISTS ST_AsTWKB(geometry,int4,int8,boolean);

DROP VIEW IF EXISTS geometry_columns; -- removed cast 2.2.0 so need to recreate
