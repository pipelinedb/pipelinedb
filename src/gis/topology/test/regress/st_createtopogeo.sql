\set VERBOSITY terse
set client_min_messages to ERROR;

TRUNCATE spatial_ref_sys;
INSERT INTO spatial_ref_sys ( auth_name, auth_srid, srid, proj4text ) VALUES ( 'EPSG', 4326, 4326, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs' );

-- Invalid topologies
select topology.st_createtopogeo('', 'GEOMETRYCOLLECTION(POINT(0 0))');
select topology.st_createtopogeo('t', 'GEOMETRYCOLLECTION(POINT(0 0))');
select topology.st_createtopogeo(null, 'GEOMETRYCOLLECTION(POINT(0 0))');

CREATE function print_isolated_nodes(lbl text)
 RETURNS table(olbl text, msg text)
AS $$
DECLARE
 sql text;
BEGIN
  sql := 'SELECT ' || quote_literal(lbl) || '::text, count(node_id)
    || '' isolated nodes in face '' || containing_face
    FROM t.node WHERE containing_face IS NOT NULL GROUP by containing_face
    ORDER BY count(node_id), containing_face';
  RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE 'plpgsql';

CREATE function print_elements_count(lbl text)
 RETURNS table(olbl text, nodes text, edges text, faces text)
AS $$
DECLARE
 sql text;
BEGIN
  sql := 'select ' || quote_literal(lbl) || '::text, 
       ( select count(node_id) || '' nodes'' from t.node ) as nodes,
       ( select count(edge_id) || '' edges'' from t.edge ) as edges,
       ( select count(face_id) || '' faces'' from t.face
                                    where face_id <> 0 ) as faces';
  RETURN QUERY EXECUTE sql;
END;
$$ LANGUAGE 'plpgsql';


-- Invalid geometries
select null from ( select topology.CreateTopology('t', 4326) > 0 ) as ct;
select topology.st_createtopogeo('t', null); -- Invalid geometry
select 'invalid_srid', topology.st_createtopogeo('t', 'POINT(0 0)'); 
select null from ( select topology.DropTopology('t') ) as dt;

-- Single point
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T1', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'POINT(0 0)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T1');
select null from ( select topology.DropTopology('t') ) as dt;

-- Single line
select null from ( select topology.CreateTopology('t', 4326) > 0 ) as ct;
select 'T2', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'SRID=4326;LINESTRING(0 0, 8 -40)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T2');
select null from ( select topology.DropTopology('t') ) as dt;

-- Single polygon with no holes
select null from ( select topology.CreateTopology('t', 4326) > 0 ) as ct;
select 'T3', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'SRID=4326;POLYGON((0 0, 8 -40, 70 34, 0 0))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T3');
select null from ( select topology.DropTopology('t') ) as dt;

-- Single polygon with an hole
select null from ( select topology.CreateTopology('t', 4326) > 0 ) as ct;
select 'T4', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'SRID=4326;POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),(5 5, 8 9, 4 2, 5 5))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T4');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi point with duplicated points
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T5', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTIPOINT(0 0, 5 5, 0 0, 10 -2, 5 5, 0 0)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T5');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi line with duplicated lines
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T6', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTILINESTRING((0 0, 10 0),(10 0, 0 0))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T6');
select * from print_isolated_nodes('T6');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi line with crossing lines
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T7', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTILINESTRING((0 0, 10 0),(5 -5, 6 5))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T7');
select * from print_isolated_nodes('T7');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi polygon with duplicated polygons
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T8', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTIPOLYGON(
  ((0 0,10 0,10 10,0 10,0 0)),
  ((0 0,0 10,10 10,10 0,0 0))
)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T8');
select * from print_isolated_nodes('T8');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi polygon with overlapping polygons
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T9', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTIPOLYGON(
  ((0 0,10 0,10 10,0 10,0 0)),
  ((5 5,5 15,15 15,15 5,5 5))
)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T9');
select * from print_isolated_nodes('T9');
select null from ( select topology.DropTopology('t') ) as dt;

-- Multi polygon with touching polygons
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T10', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'MULTIPOLYGON(
  ((0 0,5 10,10 0,0 0)),
  ((0 20,5 10,10 20,0 20))
)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T10');
select * from print_isolated_nodes('T10');
select null from ( select topology.DropTopology('t') ) as dt;

-- Collection of line and point within it
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T11', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(LINESTRING(0 0, 10 0),POINT(5 0))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T11');
select * from print_isolated_nodes('T11');
select null from ( select topology.DropTopology('t') ) as dt;

-- Collection of line and points on line's endpoint
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T12', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(LINESTRING(0 0, 10 0),POINT(0 0),POINT(10 0))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T12');
select * from print_isolated_nodes('T12');
select null from ( select topology.DropTopology('t') ) as dt;

-- Collection of line, points and polygons with various crossing and
-- overlaps
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T13', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(
  MULTIPOLYGON(
    ((0 0,10 0,10 10,0 10,0 0)),
    ((5 5,5 15,15 15,15 5,5 5), (10 10, 12 10, 10 12, 10 10))
  ),
  LINESTRING(0 0, 20 0),
  MULTIPOINT(0 0,10 0,5 0),
  MULTILINESTRING((0 0, 10 0),(10 0, 15 5)),
  POINT(5 0),
  POINT(10.5 10.5),
  POINT(100 500)
)'
::geometry as g ) as i ) as j;
select * from print_elements_count('T13');
select * from print_isolated_nodes('T13');
select null from ( select topology.DropTopology('t') ) as dt;

-- Collection of all geometries which can be derivated by the
-- well-known city_data topology
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T14', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(LINESTRING(8 30,16 30,16 38,3 38,3 30,8 30),POINT(4 31),LINESTRING(4 31,7 31,7 34,4 34,4 31),POINT(8 30),POINT(9 6),LINESTRING(9 6,9 14),LINESTRING(9 6,21 6),POLYGON((9 14,21 14,21 6,9 6,9 14)),POINT(9 14),LINESTRING(9 14,9 22),LINESTRING(9 14,21 14),POLYGON((9 22,21 22,21 14,9 14,9 22)),POINT(9 22),LINESTRING(9 22,21 22),POINT(9 35),LINESTRING(9 35,13 35),POINT(13 35),POLYGON((25 30,17 30,17 40,31 40,31 30,25 30)),POINT(20 37),POINT(21 6),LINESTRING(21 6,21 14),LINESTRING(21 6,35 6),POLYGON((21 14,35 14,35 6,21 6,21 14)),POINT(21 14),LINESTRING(21 14,21 22),LINESTRING(35 14,21 14),POLYGON((21 22,35 22,35 14,21 14,21 22)),POINT(21 22),LINESTRING(21 22,35 22),POINT(25 30),LINESTRING(25 30,25 35),POINT(25 35),POINT(35 6),LINESTRING(35 6,35 14),LINESTRING(35 6,47 6),POLYGON((35 14,47 14,47 6,35 6,35 14)),POINT(35 14),LINESTRING(35 14,35 22),LINESTRING(35 14,47 14),POLYGON((35 22,47 22,47 14,35 14,35 22)),POINT(35 22),LINESTRING(35 22,47 22),LINESTRING(36 38,38 35,41 34,42 33,45 32,47 28,50 28,52 32,57 33),POINT(36 38),LINESTRING(41 40,45 40,47 42,62 41,61 38,59 39,57 36,57 33),POINT(41 40),POINT(47 6),LINESTRING(47 6,47 14),POINT(47 14),LINESTRING(47 14,47 22),POINT(47 22),POINT(57 33))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T14');
select * from print_isolated_nodes('T14');
select null from ( select topology.DropTopology('t') ) as dt;

-- See ticket #1261
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T15', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(LINESTRING(-5 -2,0 0),
                LINESTRING(0 0,10 10),LINESTRING(0 0,5 2,10 10),
                LINESTRING(10 10,12 10))
'::geometry as g ) as i ) as j;
select * from print_elements_count('T15');
select * from print_isolated_nodes('T15');
select null from ( select topology.DropTopology('t') ) as dt;

-- Three mergeable lines 
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T16', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT
'GEOMETRYCOLLECTION(LINESTRING(0 0, 10 0),LINESTRING(0 3, 20 4),LINESTRING(10 0, 20 4))'
::geometry as g ) as i ) as j;
select * from print_elements_count('T16');
select * from print_isolated_nodes('T16');
select null from ( select topology.DropTopology('t') ) as dt;

-- Very close-by nodes created by intersection
-- See ticket #1284
select null from ( select topology.CreateTopology('t') > 0 ) as ct;
select 'T17', st_asewkt(g) FROM (
SELECT g, topology.st_createtopogeo('t', g) FROM ( SELECT '
MULTILINESTRING(
(
 832709.937 816560.25,
 832705.813 816470.25,
 832661.937 816561.875
),
(
 832705.812 816470.25,
 832709.937 816560.25
),
(
 832661.938 816561.875,
 832705.813 816470.25
))
'::geometry as g ) as i ) as j;
select * from print_elements_count('T17');
select * from print_isolated_nodes('T17');
select null from ( select topology.DropTopology('t') ) as dt;

-- clean up
DELETE FROM spatial_ref_sys where srid = 4326;
DROP FUNCTION print_isolated_nodes(text);
DROP FUNCTION print_elements_count(text);
