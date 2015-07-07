-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
--
-- PostGIS Raster - Raster Type for PostGIS
-- http://trac.osgeo.org/postgis/wiki/WKTRaster
--
-- Copyright (C) 2011 Regina Obe <lr@pcorp.us>
-- Copyright (C) 2011-2012 Regents of the University of California
--   <bkpark@ucdavis.edu>
--
-- This program is free software; you can redistribute it and/or
-- modify it under the terms of the GNU General Public License
-- as published by the Free Software Foundation; either version 2
-- of the License, or (at your option) any later version.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.
--
-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software Foundation,
-- Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- WARNING: Any change in this file must be evaluated for compatibility.
--
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
--
-- This file will be used to drop obseleted objects.
-- It will be only used for upgrades.
-- It will be loaded _after_ the objects upgrade statements, so
-- that objects previously dependent on these objects have a chance
-- to get upgraded to remove the dependency.
--
-- Remember: only put _obsoleted_ signatures in this file.
--
-- TODO: tag each item with the version in which it was dropped
--
--

-- drop obsoleted aggregates
DROP AGGREGATE IF EXISTS ST_Union(raster, text, text, text, double precision, text, text, text, double precision, text, text, text, double precision);
DROP AGGREGATE IF EXISTS ST_Union(raster, text, text, text);
DROP AGGREGATE IF EXISTS ST_Union(raster, text, text, text, double precision, text, text, text, double precision);
DROP AGGREGATE IF EXISTS ST_Union(raster, text, text);
DROP AGGREGATE IF EXISTS ST_Union(raster, text, text, text, double precision);
DROP AGGREGATE IF EXISTS ST_Union(raster, record[]);


DROP FUNCTION IF EXISTS ST_Intersects(raster,boolean,geometry);
DROP FUNCTION IF EXISTS ST_Intersects(geometry,raster,boolean);
DROP FUNCTION IF EXISTS ST_Intersects(raster,geometry);
DROP FUNCTION IF EXISTS ST_Intersects(geometry,raster);
DROP FUNCTION IF EXISTS ST_Intersects(raster, integer, boolean  , geometry);
DROP FUNCTION IF EXISTS ST_Intersects(geometry , raster, integer , boolean);
DROP FUNCTION IF EXISTS ST_Intersection(raster,raster, integer, integer);
DROP FUNCTION IF EXISTS ST_Intersection(geometry,raster);

-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionfinal1(raster);
-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionstate(raster, raster, int4);
-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionstate(raster, raster);
-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionstate(raster, raster, text);
-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionstate(raster, raster, int4, text);
-- Removed in 2.1.0
DROP FUNCTION IF EXISTS _st_mapalgebra4unionstate(raster, raster, text, text, text, float8, text, text, text, float8);
-- Removed in 2.2.0
DROP FUNCTION IF EXISTS _st_mapalgebra(rastbandarg[],regprocedure,text,integer,integer,text,raster,text[]);
