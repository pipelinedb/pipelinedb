
\set VERBOSITY terse
set client_min_messages to ERROR;

-- Import city_data
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
  SELECT ''E|'' || e.edge_id || ''|sn'' || e.start_node || ''|en'' || e.end_node 
         || ''|nl'' || e.next_left_edge
         || ''|nr'' || e.next_right_edge
         || ''|lf'' || e.left_face
         || ''|rf'' || e.right_face
         :: text as xx
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
SELECT 'invalid', ST_NewEdgesSplit('city_data', 999, 'POINT(36 26, 38 30)');
SELECT 'invalid', ST_NewEdgesSplit('city_data', 10, 'POINT(28 15)');
SELECT 'invalid', ST_NewEdgesSplit('', 10, 'POINT(28 14)');
SELECT 'invalid', ST_NewEdgesSplit(NULL, 10, 'POINT(28 14)');
SELECT 'invalid', ST_NewEdgesSplit('city_data', NULL, 'POINT(28 14)');
SELECT 'invalid', ST_NewEdgesSplit('city_data', 10, NULL);
SELECT 'invalid', ST_NewEdgesSplit('fake', 10, 'POINT(28 14)');

-- Non-isolated edge 
SELECT 'noniso', ST_NewEdgesSplit('city_data', 10, 'POINT(28 14)');
SELECT check_changes();

-- Isolated edge
SELECT 'iso', ST_NewEdgesSplit('city_data', 25, 'POINT(11 35)');
SELECT check_changes();

-- Dangling on end point 
SELECT 'dangling_end', ST_NewEdgesSplit('city_data', 3, 'POINT(25 32)');
SELECT check_changes();

-- Dangling on start point 
SELECT 'dangling_start', ST_NewEdgesSplit('city_data', 4, 'POINT(45 32)');
SELECT check_changes();

-- Splitting closed edge
SELECT 'closed', ST_NewEdgesSplit('city_data', 1, 'POINT(3 38)');
SELECT check_changes();

-- Robustness of edge splitting (#1711)

-- clean all up first
DELETE FROM city_data.edge_data; 
DELETE FROM city_data.node; 
DELETE FROM city_data.face where face_id > 0; 

CREATE TEMP TABLE t AS
SELECT
'01020000000400000000000000000034400000000000002440000000000000244000000000000024400000000000002240000000000000284000000000000024400000000000003440'
::geometry as line, 
'010100000000000000000022400000000000002840'
::geometry as point,
null::int as edge_id,
null::int as node_id
;

UPDATE t SET edge_id = AddEdge('city_data', line);
UPDATE t SET node_id = ST_NewEdgesSplit('city_data', t.edge_id, t.point);
SELECT 'robust.1', 'E'||edge_id, 'N'||node_id FROM t;
SELECT check_changes();
SELECT 'robust.2',
 ST_Equals(t.point, ST_EndPoint(e1.geom)),
 ST_Equals(t.point, ST_StartPoint(e2.geom))
FROM t, city_data.edge e1, city_data.edge e2, city_data.node n
WHERE n.node_id = t.node_id
 AND e1.end_node = n.node_id
 AND e2.start_node = n.node_id;

DROP TABLE t;


DROP FUNCTION check_changes();
SELECT DropTopology('city_data');
