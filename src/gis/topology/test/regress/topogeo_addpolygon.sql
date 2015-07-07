\set VERBOSITY terse
set client_min_messages to ERROR;

\i load_topology.sql

-- Save max node,edge and face ids
select 'node'::text as what, max(node_id) INTO city_data.limits FROM city_data.node;
INSERT INTO city_data.limits select 'edge'::text as what, max(edge_id) FROM city_data.edge;
INSERT INTO city_data.limits select 'face'::text as what, max(face_id) FROM city_data.face;
SELECT 'max',* from city_data.limits;

-- Check changes since last saving, save more
-- {
CREATE OR REPLACE FUNCTION check_changes()
RETURNS TABLE (o text)
AS $$
DECLARE
  rec RECORD;
  sql text;
BEGIN
  -- Check effect on nodes
  sql := 'SELECT n.node_id, ''N|'' || n.node_id || ''|'' ||
        COALESCE(n.containing_face::text,'''') || ''|'' ||
        ST_AsText(ST_SnapToGrid(n.geom, 0.2))::text as xx
  	FROM city_data.node n WHERE n.node_id > (
    		SELECT max FROM city_data.limits WHERE what = ''node''::text )
  		ORDER BY n.node_id';

  FOR rec IN EXECUTE sql LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;
  
  -- Check effect on edges 
  sql := '
  WITH node_limits AS ( SELECT max FROM city_data.limits WHERE what = ''node''::text ),
       edge_limits AS ( SELECT max FROM city_data.limits WHERE what = ''edge''::text )
  SELECT ''E|'' || e.edge_id || ''|sn'' || e.start_node || ''|en'' || e.end_node :: text as xx
   FROM city_data.edge e, node_limits nl, edge_limits el
   WHERE e.start_node > nl.max
      OR e.end_node > nl.max
      OR e.edge_id > el.max
  ORDER BY e.edge_id;
  ';

  FOR rec IN EXECUTE sql LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  -- Check effect on faces
  sql := '
  WITH face_limits AS ( SELECT max FROM city_data.limits WHERE what = ''face''::text )
  SELECT ''F|'' || f.face_id ::text as xx
   FROM city_data.face f, face_limits fl
   WHERE f.face_id > fl.max
  ORDER BY f.face_id;
  ';

  FOR rec IN EXECUTE sql LOOP
    o := rec.xx;
    RETURN NEXT;
  END LOOP;

  UPDATE city_data.limits SET max = (SELECT max(n.node_id) FROM city_data.node n) WHERE what = 'node';
  UPDATE city_data.limits SET max = (SELECT max(e.edge_id) FROM city_data.edge e) WHERE what = 'edge';
  UPDATE city_data.limits SET max = (SELECT max(f.face_id) FROM city_data.face f) WHERE what = 'face';

END;
$$ LANGUAGE 'plpgsql';
-- }


-- Invalid calls
SELECT 'invalid', TopoGeo_addPolygon('city_data', 'MULTILINESTRING((36 26, 38 30))');
SELECT 'invalid', TopoGeo_addPolygon('city_data', 'POINT(36 26)');
SELECT 'invalid', TopoGeo_addPolygon('invalid', 'POLYGON((36 26, 40 24, 40 30, 36 26))');

-- Isolated face in universal face
SELECT 'iso_uni', TopoGeo_addPolygon('city_data', 'POLYGON((36 26, 38 30, 43 26, 36 26))');
SELECT check_changes();

-- Isolated face in universal face with hole
SELECT 'iso_uni_hole', TopoGeo_addPolygon('city_data', 'POLYGON((9 28, 16 29, 16 23, 10 23, 9 28),(15 25, 13 27, 11 24, 15 25))');
SELECT check_changes();

-- Existing single face
SELECT 'ex', TopoGeo_addPolygon('city_data', 'POLYGON((21 22,35 22,35 14,21 14,21 22))');
SELECT check_changes();

-- Union of existing faces
SELECT 'ex_union', TopoGeo_addPolygon('city_data', 'POLYGON((9 14,21 14,35 14,35 6,21 6,9 6,9 14))') ORDER BY 2;
SELECT check_changes();

-- Half an existing face
SELECT 'half', TopoGeo_addPolygon('city_data', 'POLYGON((21 14, 35 22, 35 14, 21 14))');
SELECT check_changes();

-- Split two existing faces
SELECT 'split', TopoGeo_addPolygon('city_data', 'POLYGON((21 14, 21 22, 35 14, 21 14))') ORDER BY 2;
SELECT check_changes();

-- Union of existing face, with hole
SELECT 'ex_hole', TopoGeo_addPolygon('city_data', 'POLYGON((9 22,47 22,47 6,9 6,9 22),(21 14,28 18,35 14,21 14))') ORDER BY 2;
SELECT check_changes();

-- Union of existing face, with hole and a tolerance
SELECT 'ex_hole_snap', TopoGeo_addPolygon('city_data', 'POLYGON((9 22,35 22.5, 47 22,47 6,9 6,9 22),(21 14,28 17.5,35 14,21 14))', 1) ORDER BY 2;
SELECT check_changes();

DROP FUNCTION check_changes();
SELECT DropTopology('city_data');

