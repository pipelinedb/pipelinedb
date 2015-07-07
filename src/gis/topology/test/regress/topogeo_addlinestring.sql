\set VERBOSITY terse
set client_min_messages to ERROR;

\i load_topology.sql

-- Save max node id
select 'node'::text as what, max(node_id) INTO city_data.limits FROM city_data.node;
INSERT INTO city_data.limits select 'edge'::text as what, max(edge_id) FROM city_data.edge;
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
  
  -- Check effect on edges (there should be one split)
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

  UPDATE city_data.limits SET max = (SELECT max(n.node_id) FROM city_data.node n) WHERE what = 'node';
  UPDATE city_data.limits SET max = (SELECT max(e.edge_id) FROM city_data.edge e) WHERE what = 'edge';

END;
$$ LANGUAGE 'plpgsql';
-- }


-- Invalid calls
SELECT 'invalid', TopoGeo_addLineString('city_data', 'MULTILINESTRING((36 26, 38 30))');
SELECT 'invalid', TopoGeo_addLineString('city_data', 'POINT(36 26)');
SELECT 'invalid', TopoGeo_addLineString('invalid', 'LINESTRING(36 26, 0 0)');

-- Isolated edge in universal face
SELECT 'iso_uni', TopoGeo_addLineString('city_data', 'LINESTRING(36 26, 38 30)');
SELECT check_changes();

-- Isolated edge in face 5
SELECT 'iso_f5', TopoGeo_addLineString('city_data', 'LINESTRING(37 20, 43 19, 41 16)');
SELECT check_changes();

-- Existing isolated edge
SELECT 'iso_ex', TopoGeo_addLineString('city_data', 'LINESTRING(36 26, 38 30)');
SELECT check_changes();

-- Existing isolated edge within tolerance
SELECT 'iso_ex_tol', TopoGeo_addLineString('city_data', 'LINESTRING(36 27, 38 31)', 2);
SELECT check_changes();

-- Existing non-isolated edge
SELECT 'noniso_ex', TopoGeo_addLineString('city_data', 'LINESTRING(35 6, 35 14)');
SELECT check_changes();

-- Existing non-isolated edge within tolerance 
SELECT 'noniso_ex_tol', TopoGeo_addLineString('city_data', 'LINESTRING(35 7, 35 13)', 2);
SELECT check_changes();

-- Fully contained
SELECT 'contained', TopoGeo_addLineString('city_data', 'LINESTRING(35 8, 35 12)');
SELECT check_changes();

-- Overlapping 
SELECT 'overlap', TopoGeo_addLineString('city_data', 'LINESTRING(45 22, 49 22)') ORDER BY 2;
SELECT check_changes();

-- Crossing
SELECT 'cross', TopoGeo_addLineString('city_data', 'LINESTRING(49 18, 44 17)') ORDER BY 2;
SELECT check_changes();

-- Snapping (and splitting a face)
SELECT 'snap', TopoGeo_addLineString('city_data', 'LINESTRING(18 22.2, 22.5 22.2, 21.2 20.5)', 1) ORDER BY 2;
SELECT check_changes();
SELECT 'snap_again', TopoGeo_addLineString('city_data', 'LINESTRING(18 22.2, 22.5 22.2, 21.2 20.5)', 1) ORDER BY 2;
SELECT check_changes();

-- A mix of crossing and overlapping, splitting another face
SELECT 'crossover', TopoGeo_addLineString('city_data', 'LINESTRING(9 18, 9 20, 21 10, 21 7)') ORDER BY 2;
SELECT check_changes();
SELECT 'crossover_again', TopoGeo_addLineString('city_data', 'LINESTRING(9 18, 9 20, 21 10, 21 7)') ORDER BY 2;
SELECT check_changes();

-- Fully containing
SELECT 'contains', TopoGeo_addLineString('city_data', 'LINESTRING(14 34, 13 35, 10 35, 9 35, 7 36)') ORDER BY 2;
SELECT check_changes();

-- Crossing a node
SELECT 'nodecross', TopoGeo_addLineString('city_data', 'LINESTRING(18 37, 22 37)') ORDER BY 2;
SELECT check_changes();

-- Existing isolated edge with 2 segments
SELECT 'iso_ex_2segs', TopoGeo_addLineString('city_data', 'LINESTRING(37 20, 43 19, 41 16)');
SELECT check_changes();

-- See http://trac.osgeo.org/postgis/attachment/ticket/1613

SELECT '#1613.1', TopoGeo_addLineString('city_data', 'LINESTRING(556267.562954 144887.066638, 556267 144887.4)') ORDER BY 2;
SELECT check_changes();
SELECT '#1613.2', TopoGeo_addLineString('city_data', 'LINESTRING(556250 144887, 556267 144887.07, 556310.04 144887)') ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- See http://trac.osgeo.org/postgis/ticket/1631

-- clean all up first
DELETE FROM city_data.edge_data; 
DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1631.1', TopoGeo_addLineString('city_data',
  'LINESTRING(556267.56295432 144887.06663814,556267.566 144888)'
) ORDER BY 2;
SELECT check_changes();
SELECT '#1631.2', TopoGeo_addLineString('city_data',
  'LINESTRING(556254.67 144886.62, 556267.66 144887.07)'
) ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- See http://trac.osgeo.org/postgis/ticket/1641

-- clean all up first
DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1641.1', TopoGeo_addLineString('city_data',
  'LINESTRING(-0.223586 0.474301, 0.142550 0.406124)' 
) ORDER BY 2;
SELECT check_changes();
-- Use a tolerance
SELECT '#1641.2', TopoGeo_addLineString('city_data',
  'LINESTRING(0.095989 0.113619, -0.064646 0.470149)'
  , 1e-16
) ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- Now w/out explicit tolerance (will use local min)
-- clean all up first
DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1641.3', TopoGeo_addLineString('city_data',
  'LINESTRING(-0.223586 0.474301, 0.142550 0.406124)' 
) ORDER BY 2;
SELECT check_changes();
SELECT '#1641.4', TopoGeo_addLineString('city_data',
  'LINESTRING(0.095989 0.113619, -0.064646 0.470149)'
) ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- See http://trac.osgeo.org/postgis/ticket/1650

DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1650.1' UNION ALL
SELECT '#1650.2' || TopoGeo_addLineString('city_data',
  'LINESTRING(0 0, 0 1)'
, 2)::text;
SELECT check_changes();

SELECT '#1650.3', TopoGeo_addLineString('city_data',
  'LINESTRING(-1 0, 10 0)'
, 2) ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- Test snapping of line over a node
-- See http://trac.osgeo.org/postgis/ticket/1654 

DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1654.1', 'N', ST_AddIsoNode('city_data', 0, 'POINT(0 0)');
SELECT check_changes();
SELECT '#1654.2', TopoGeo_addLineString('city_data',
  'LINESTRING(-10 1, 10 1)'
, 2) ORDER BY 2;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- Test snapping of new edge endpoints 
-- See http://trac.osgeo.org/postgis/ticket/1706 

DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1706.1', 'E', TopoGeo_AddLineString('city_data',
 'LINESTRING(20 10, 10 10, 9 12, 10 20)');
SELECT check_changes();

SELECT '#1706.2', 'E*', TopoGeo_addLineString('city_data',
 'LINESTRING(10 0, 10 10, 15 10, 20 10)'
, 4) ORDER BY 3;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- Test noding after snap 
-- See http://trac.osgeo.org/postgis/ticket/1714 

DELETE FROM city_data.edge_data; DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

SELECT '#1714.1', 'N', AddNode('city_data', 'POINT(10 0)', false, true);
SELECT check_changes();

SELECT '#1714.2', 'E*', TopoGeo_addLineString('city_data',
 'LINESTRING(10 0, 0 20, 0 0, 10 0)'
, 12) ORDER BY 3;
SELECT check_changes();

-- Consistency check
SELECT * FROM ValidateTopology('city_data');

-- Cleanups
DROP FUNCTION check_changes();
SELECT DropTopology('city_data');
