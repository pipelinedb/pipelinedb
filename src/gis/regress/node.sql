-- Node two crossing lines
select 't1', st_asewkt(st_node(
'SRID=10;MULTILINESTRING((0 0, 10 0),(5 -5, 5 5))'
));

-- Node two overlapping lines
select 't2', st_asewkt(st_node(
'SRID=10;MULTILINESTRING((0 0, 10 0, 20 0),(25 0, 15 0, 8 0))'
));

-- Node a self-intersecting line
-- NOTE: requires GEOS 3.3.2 which is still unreleased at time of writing
--       see http://trac.osgeo.org/geos/ticket/482
--select st_node('SRID=10;LINESTRING(0 0, 10 10, 0 10, 10 0)');
