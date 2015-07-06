\set VERBOSITY terse
set client_min_messages to ERROR;

INSERT INTO spatial_ref_sys ( auth_name, auth_srid, srid, proj4text ) VALUES ( 'EPSG', 4326, 4326, '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs' );


-- Import city_data
\i load_topology-4326.sql

-- Utility functions for the test {

CREATE TEMP TABLE orig_node_summary(node_id integer, containing_face integer);
CREATE OR REPLACE FUNCTION save_nodes()
RETURNS VOID
AS $$
  TRUNCATE TABLE orig_node_summary;
  INSERT INTO orig_node_summary
  SELECT node_id,
    containing_face
    FROM city_data.node;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION check_nodes(lbl text)
RETURNS TABLE (l text, o text, node_id int,
    containing_face int)
AS $$
DECLARE
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'node_id,
      containing_face 
  		FROM city_data.node';
  sql2 := 'node_id, containing_face
  		FROM orig_node_summary';

  q := '(' ||
          'SELECT ' || quote_literal(lbl) || ',''+'' as op,' || sql1 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''+'',' || sql2 ||
          ') UNION ( ' ||
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql2 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql1 ||
       ') ORDER BY node_id, op';

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ LANGUAGE 'plpgsql';

CREATE TEMP TABLE orig_edge_summary (edge_id integer, next_left_edge integer, next_right_edge integer, left_face integer, right_face integer);
CREATE OR REPLACE FUNCTION save_edges()
RETURNS VOID
AS $$
  TRUNCATE orig_edge_summary;
  INSERT INTO orig_edge_summary 
  SELECT edge_id,
    next_left_edge, next_right_edge, left_face, right_face
    FROM city_data.edge_data;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION check_edges(lbl text)
RETURNS TABLE (l text, o text, edge_id int,
    next_left_edge int, next_right_edge int,
    left_face int, right_face int)
AS $$
DECLARE
  rec RECORD;
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'edge_id,
      next_left_edge, next_right_edge, left_face, right_face
  		FROM city_data.edge_data';
  sql2 := 'edge_id,
  		next_left_edge, next_right_edge, left_face, right_face
  		FROM orig_edge_summary';

  q := '(' ||
          'SELECT ' || quote_literal(lbl) || ',''+'' as op,' || sql1 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''+'',' || sql2 ||
          ') UNION ( ' ||
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql2 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql1 ||
       ') order by edge_id, op';

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ LANGUAGE 'plpgsql';

CREATE TEMP TABLE orig_face_summary(face_id integer, mbr geometry);
CREATE OR REPLACE FUNCTION save_faces()
RETURNS VOID
AS $$
  TRUNCATE orig_face_summary;
  INSERT INTO orig_face_summary 
  SELECT face_id, mbr
    FROM city_data.face;
$$ LANGUAGE 'sql';

CREATE OR REPLACE FUNCTION check_faces(lbl text)
RETURNS TABLE (l text, o text, face_id int, mbr text)
AS $$
DECLARE
  sql1 text;
  sql2 text;
  q text;
BEGIN
  sql1 := 'face_id, ST_AsEWKT(mbr) FROM city_data.face';
  sql2 := 'face_id, ST_AsEWKT(mbr) FROM orig_face_summary';

  q := '(' ||
          'SELECT ' || quote_literal(lbl) || ',''+'' as op,' || sql1 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''+'',' || sql2 ||
          ') UNION ( ' ||
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql2 ||
          ' EXCEPT ' || 
          'SELECT ' || quote_literal(lbl) || ',''-'',' || sql1 ||
       ') ORDER BY face_id, op';

  RAISE DEBUG '%', q;

  RETURN QUERY EXECUTE q;

END
$$ language 'plpgsql';

-- }

-- Save current state
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Bogus calls -- {
SELECT topology.ST_RemEdgeModFace('city_data', null);
SELECT topology.ST_RemEdgeModFace(null, 1);
SELECT topology.ST_RemEdgeModFace('', 1);
SELECT topology.ST_RemEdgeModFace('city_data', 0); -- non-existent
SELECT topology.ST_RemEdgeModFace('city_data', 143); -- non-existent
SELECT * FROM check_nodes('bogus');
SELECT * FROM check_edges('bogus');
SELECT * FROM check_faces('bogus');
-- }


-- Remove isolated edge
SELECT 'RM(25)', topology.ST_RemEdgeModFace('city_data', 25);
SELECT * FROM check_nodes('RM(25)/nodes');
SELECT * FROM check_edges('RM(25)/edges');
SELECT * FROM check_faces('RM(25)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Remove edge not forming a ring
SELECT 'RM(4)', topology.ST_RemEdgeModFace('city_data', 4);
SELECT * FROM check_nodes('RM(4)/nodes');
SELECT * FROM check_edges('RM(4)/edges');
SELECT * FROM check_faces('RM(4)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Heal faces 1 and 9 -- should drop them and create a new face
-- New face has the same mbr as old one
SELECT 'RM(26)', topology.ST_RemEdgeModFace('city_data', 26);
SELECT * FROM check_nodes('RM(26)/nodes');
SELECT * FROM check_edges('RM(26)/edges');
SELECT * FROM check_faces('RM(26)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Heal faces 3 and 6 -- should drop them and create a new face
-- New face has a mbr being the union of the dropped faces
SELECT 'RM(9)', topology.ST_RemEdgeModFace('city_data', 9);
SELECT * FROM check_nodes('RM(9)/nodes');
SELECT * FROM check_edges('RM(9)/edges');
SELECT * FROM check_faces('RM(9)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Heal faces 4 and 11 -- should drop them and create a new face
-- New face has a mbr being the union of the dropped faces
SELECT 'RM(19)', topology.ST_RemEdgeModFace('city_data', 19);
SELECT * FROM check_nodes('RM(19)/nodes');
SELECT * FROM check_edges('RM(19)/edges');
SELECT * FROM check_faces('RM(19)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Heal faces 7 and 12 -- should drop them and create a new face
-- New face has a mbr equal to previous face 12.
-- This healing leaves edge 20 dangling
SELECT 'RM(10)', topology.ST_RemEdgeModFace('city_data', 10);
SELECT * FROM check_nodes('RM(10)/nodes');
SELECT * FROM check_edges('RM(10)/edges');
SELECT * FROM check_faces('RM(10)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Drop dangling edge, no faces change
SELECT 'RM(20)', topology.ST_RemEdgeModFace('city_data', 20);
SELECT * FROM check_nodes('RM(20)/nodes');
SELECT * FROM check_edges('RM(20)/edges');
SELECT * FROM check_faces('RM(20)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing face 
SELECT 'RM(15)', topology.ST_RemEdgeModFace('city_data', 15);
SELECT * FROM check_nodes('RM(15)/nodes');
SELECT * FROM check_edges('RM(15)/edges');
SELECT * FROM check_faces('RM(15)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();


-- Universe flooding existing single-edge (closed) face 
-- with dangling edge starting from the closing node and
-- going inside.
-- Closed edge is in CW order.
SELECT 'RM(2)', topology.ST_RemEdgeModFace('city_data', 2);
SELECT * FROM check_nodes('RM(2)/nodes');
SELECT * FROM check_edges('RM(2)/edges');
SELECT * FROM check_faces('RM(2)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face 
-- with dangling edge coming from inside and ending to the closing node
-- Closed edge is in CW order.
-- Requires reconstructing the outer ring
SELECT 'NE(27)', topology.ST_AddEdgeNewFaces('city_data', 3, 3, 'SRID=4326;LINESTRING(25 35, 30 27, 20 27, 25 35)');
SELECT * FROM check_nodes('NE(27)/nodes');
SELECT * FROM check_edges('NE(27)/edges');
SELECT * FROM check_faces('NE(27)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(27)', topology.ST_RemEdgeModFace('city_data', 27);
SELECT * FROM check_nodes('RM(27)/nodes');
SELECT * FROM check_edges('RM(27)/edges');
SELECT * FROM check_faces('RM(27)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face 
-- with dangling edge coming from inside and ending to the closing node
-- Closed edge is in CCW order.
-- Requires reconstructing the outer ring
SELECT 'NE(28)', topology.ST_AddEdgeNewFaces('city_data', 3, 3, 'SRID=4326;LINESTRING(25 35, 20 27, 30 27, 25 35)');
SELECT * FROM check_nodes('NE(28)/nodes');
SELECT * FROM check_edges('NE(28)/edges');
SELECT * FROM check_faces('NE(28)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(28)', topology.ST_RemEdgeModFace('city_data', 28);
SELECT * FROM check_nodes('RM(28)/nodes');
SELECT * FROM check_edges('RM(28)/edges');
SELECT * FROM check_faces('RM(28)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face 
-- with dangling edge starting from closing node and going inside.
-- Closed edge is in CCW order.
-- Requires reconstructing the outer ring
SELECT 'NE(29)', topology.ST_AddEdgeNewFaces('city_data', 2, 2, 'SRID=4326;LINESTRING(25 30, 28 37, 22 37, 25 30)');
SELECT * FROM check_nodes('NE(29)/nodes');
SELECT * FROM check_edges('NE(29)/edges');
SELECT * FROM check_faces('NE(29)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(29)', topology.ST_RemEdgeModFace('city_data', 29);
SELECT * FROM check_nodes('RM(29)/nodes');
SELECT * FROM check_edges('RM(29)/edges');
SELECT * FROM check_faces('RM(29)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face
-- with dangling edges both inside and outside
-- Closed edge in CW order.
-- Requires adding an edge and reconstructing the outer ring
SELECT 'NE(30)', topology.ST_AddEdgeNewFaces('city_data', 4, 3, 'SRID=4326;LINESTRING(20 37, 25 35)');
SELECT 'NE(31)', topology.ST_AddEdgeNewFaces('city_data', 3, 3, 'SRID=4326;LINESTRING(25 35, 18 35, 18 40, 25 35)');
SELECT * FROM check_nodes('NE(30,31)/nodes');
SELECT * FROM check_edges('NE(30,31)/edges');
SELECT * FROM check_faces('NE(30,31)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(31)', topology.ST_RemEdgeModFace('city_data', 31);
SELECT * FROM check_nodes('RM(31)/nodes');
SELECT * FROM check_edges('RM(31)/edges');
SELECT * FROM check_faces('RM(31)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face
-- with dangling edges both inside 
-- Closed edge in CW order.
-- Requires reconstructing the outer ring
SELECT 'NE(32)', topology.ST_AddEdgeNewFaces('city_data', 3, 3, 'SRID=4326;LINESTRING(25 35, 18 35, 18 40, 28 40, 28 27, 18 27, 25 35)');
SELECT * FROM check_nodes('NE(32)/nodes');
SELECT * FROM check_edges('NE(32)/edges');
SELECT * FROM check_faces('NE(32)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(32)', topology.ST_RemEdgeModFace('city_data', 32);
SELECT * FROM check_nodes('RM(32)/nodes');
SELECT * FROM check_edges('RM(32)/edges');
SELECT * FROM check_faces('RM(32)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face
-- with dangling edges both inside 
-- Closed edge in CCW order.
-- Requires reconstructing the outer ring
SELECT 'NE(33)', topology.ST_AddEdgeNewFaces('city_data', 3, 3,
  'SRID=4326;LINESTRING(25 35,18 27,28 27,28 40,18 40,18 35,25 35)');
SELECT * FROM check_nodes('NE(33)/nodes');
SELECT * FROM check_edges('NE(33)/edges');
SELECT * FROM check_faces('NE(33)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(33)', topology.ST_RemEdgeModFace('city_data', 33);
SELECT * FROM check_nodes('RM(33)/nodes');
SELECT * FROM check_edges('RM(33)/edges');
SELECT * FROM check_faces('RM(33)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face 
-- with dangling edge starting from closing node and going outside.
-- Closed edge is in CW order.
-- Requires reconstructing the outer ring
SELECT 'NE(34)', topology.ST_AddEdgeNewFaces('city_data', 2, 2,
 'SRID=4326;LINESTRING(25 30, 28 27, 22 27, 25 30)');
SELECT * FROM check_nodes('NE(34)/nodes');
SELECT * FROM check_edges('NE(34)/edges');
SELECT * FROM check_faces('NE(34)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(34)', topology.ST_RemEdgeModFace('city_data', 34);
SELECT * FROM check_nodes('RM(34)/nodes');
SELECT * FROM check_edges('RM(34)/edges');
SELECT * FROM check_faces('RM(34)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

-- Universe flooding existing single-edge (closed) face 
-- with dangling edge starting from closing node and going outside.
-- Closed edge is in CCW order.
-- Requires reconstructing the outer ring
SELECT 'NE(35)', topology.ST_AddEdgeNewFaces('city_data', 2, 2,
 'SRID=4326;LINESTRING(25 30,22 27,28 27,25 30)'
);
SELECT * FROM check_nodes('NE(35)/nodes');
SELECT * FROM check_edges('NE(35)/edges');
SELECT * FROM check_faces('NE(35)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();
-- Here's the removal
SELECT 'RM(35)', topology.ST_RemEdgeModFace('city_data', 35);
SELECT * FROM check_nodes('RM(35)/nodes');
SELECT * FROM check_edges('RM(35)/edges');
SELECT * FROM check_faces('RM(35)/faces');
SELECT save_edges(); SELECT save_faces(); SELECT save_nodes();

SELECT topology.DropTopology('city_data');


-------------------------------------------------------------------------
-- Now test in presence of features 
-------------------------------------------------------------------------
-- {

-- Import city_data
\i load_topology.sql
\i load_features.sql
\i cache_geometries.sql

-- A city_street is defined by edge 3, can't drop
SELECT '*RM(3)', topology.ST_RemEdgeModFace('city_data', 3);
-- A city_street is defined by edge 4 and 5, can't drop any of the two
SELECT '*RM(4)', topology.ST_RemEdgeModFace('city_data', 4);
SELECT '*RM(5)', topology.ST_RemEdgeModFace('city_data', 5);

-- Two land_parcels (P2 and P3) are defined by either face
-- 5 but not face 4 or by face 4 but not face 5, so we can't heal
-- the faces by dropping edge 17
SELECT '*RM(17)', topology.ST_RemEdgeModFace('city_data', 17);

-- Dropping edge 11 is fine as it heals faces 5 and 8, which
-- only serve definition of land_parcel P3 which contains both
SELECT 'RM(11)', 'relations_before:', count(*) FROM city_data.relation;
SELECT 'RM(11)', topology.ST_RemEdgeModFace('city_data', 11);
SELECT 'RM(11)', 'relations_after:', count(*) FROM city_data.relation;

-- Land parcel P3 is now defined by face 8, so we can't drop
-- any edge which would destroy that face.
SELECT '*RM(8)', topology.ST_RemEdgeModFace('city_data', 8); -- face_right=8
SELECT '*RM(15)', topology.ST_RemEdgeModFace('city_data', 15); -- face_left=8

-- Check that no land_parcel objects had topology changed
SELECT 'RM(11)', feature_name, 
 ST_Equals( ST_Multi(feature::geometry), ST_Multi(the_geom) ) as unchanged
 FROM features.land_parcels;

SELECT topology.DropTopology('city_data');
DROP SCHEMA features CASCADE;

-- }
-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

-- clean up
DROP FUNCTION save_edges();
DROP FUNCTION check_edges(text);
DROP FUNCTION save_faces();
DROP FUNCTION check_faces(text);
DROP FUNCTION save_nodes();
DROP FUNCTION check_nodes(text);
DELETE FROM spatial_ref_sys where srid = 4326;


