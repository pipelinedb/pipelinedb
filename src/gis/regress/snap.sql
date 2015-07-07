-- SRIDs are checked!
select 't1', st_asewkt(st_snap(
  'SRID=10;LINESTRING(0 0, 10 0)', 'SRID=5;LINESTRING(0 0, 100 0)', 0
));

-- SRIDs are retained
select 't2', st_asewkt(st_snap(
  'SRID=10;LINESTRING(0 0, 10 0)', 'SRID=10;LINESTRING(0 0, 9 0)', 1
));

-- Segment snapping
select 't3', st_asewkt(st_snap(
  'LINESTRING(0 0, 10 0)', 'LINESTRING(5 -1, 5 -10)', 2
));

-- Vertex snapping
select 't4', st_asewkt(st_snap(
  'LINESTRING(0 0, 10 0)', 'POINT(11 0)', 2
));

-- http://sourceforge.net/mailarchive/forum.php?thread_name=4CE841B8.4090702@cgf.upv.es&forum_name=jts-topo-suite-user
select 't5', st_asewkt(st_snap(
  'LINESTRING (70 250, 190 340)',
  'POLYGON ((230 340, 300 340, 300 240, 230 240, 230 340))',
  50
));
