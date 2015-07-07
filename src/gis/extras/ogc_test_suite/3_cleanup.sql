-- FILE: sqltcle.sql 10/01/98
--
-- *** ADAPTATION ALERT ***
-- OGC will not examine this script for adaptations.
-- Please add any other cleanup to this script.
-- 
DELETE FROM spatial_ref_sys;
DELETE FROM geometry_columns;
DROP TABLE lakes;
DROP TABLE road_segments;
DROP TABLE divided_routes;
DROP TABLE forests;
DROP TABLE bridges;
DROP TABLE streams;
DROP TABLE buildings;
DROP TABLE ponds;
DROP TABLE named_places;
DROP TABLE map_neatlines;
--DROP TABLE route_segments;
--DROP TABLE routes;
--DROP TABLE map_components;
--DROP TABLE maps;
