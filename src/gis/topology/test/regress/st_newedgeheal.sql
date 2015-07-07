\set VERBOSITY terse
set client_min_messages to ERROR;

-- Import city_data
\i load_topology.sql

SELECT topology.ST_NewEdgeHeal('city_data', 1, null);
SELECT topology.ST_NewEdgeHeal('city_data', null, 1);
SELECT topology.ST_NewEdgeHeal(null, 1, 2);
SELECT topology.ST_NewEdgeHeal('', 1, 2);

-- Not connected edges
SELECT topology.ST_NewEdgeHeal('city_data', 25, 3);

-- Other connected edges
SELECT topology.ST_NewEdgeHeal('city_data', 9, 10);

-- Closed edge
SELECT topology.ST_NewEdgeHeal('city_data', 2, 3);
SELECT topology.ST_NewEdgeHeal('city_data', 3, 2);

-- Heal to self 
SELECT topology.ST_NewEdgeHeal('city_data', 25, 25);

-- Good ones {

-- check state before 
SELECT 'E'||edge_id,
  ST_AsText(ST_StartPoint(geom)), ST_AsText(ST_EndPoint(geom)),
  next_left_edge, next_right_edge, start_node, end_node
  FROM city_data.edge_data ORDER BY edge_id;
SELECT 'N'||node_id FROM city_data.node;

-- No other edges involved, SQL/MM caseno 2, drops node 6
SELECT 'MH(4,5)', topology.ST_NewEdgeHeal('city_data', 4, 5);

-- Face and other edges involved, SQL/MM caseno 1, drops node 16
SELECT 'MH(21,6)', topology.ST_NewEdgeHeal('city_data', 21, 6);
-- Face and other edges involved, SQL/MM caseno 2, drops node 19
SELECT 'MH(8,15)', topology.ST_NewEdgeHeal('city_data', 8, 15);
-- Face and other edges involved, SQL/MM caseno 3, drops node 8
SELECT 'MH(12,22)', topology.ST_NewEdgeHeal('city_data', 12, 22);
-- Face and other edges involved, SQL/MM caseno 4, drops node 11
SELECT 'MH(16,14)', topology.ST_NewEdgeHeal('city_data', 16, 14);

-- check state after
SELECT 'E'||edge_id,
  ST_AsText(ST_StartPoint(geom)), ST_AsText(ST_EndPoint(geom)),
  next_left_edge, next_right_edge, start_node, end_node
  FROM city_data.edge_data ORDER BY edge_id;
SELECT 'N'||node_id FROM city_data.node;

-- }

-- clean up
SELECT topology.DropTopology('city_data');

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

-- Now test in presence of features 

SELECT topology.CreateTopology('t') > 1;
CREATE TABLE t.f(id varchar);
SELECT topology.AddTopoGeometryColumn('t', 't', 'f','g', 'LINE');

SELECT 'E'||topology.AddEdge('t', 'LINESTRING(2 2, 2  8)');        -- 1
SELECT 'E'||topology.AddEdge('t', 'LINESTRING(2  8,  8  8)');      -- 2

INSERT INTO t.f VALUES ('F+E1',
  topology.CreateTopoGeom('t', 2, 1, '{{1,2}}')); 

-- This should be forbidden, as F+E1 above could not be
-- defined w/out one of the edges
SELECT topology.ST_NewEdgeHeal('t', 1, 2);
SELECT topology.ST_NewEdgeHeal('t', 2, 1);

-- This is for ticket #941
SELECT topology.ST_NewEdgeHeal('t', 1, 200);
SELECT topology.ST_NewEdgeHeal('t', 100, 2);

-- Now see how signed edges are updated

SELECT 'E'||topology.AddEdge('t', 'LINESTRING(0 0, 5 0)');         -- 3
SELECT 'E'||topology.AddEdge('t', 'LINESTRING(10 0, 5 0)');        -- 4

INSERT INTO t.f VALUES ('F+E3-E4',
  topology.CreateTopoGeom('t', 2, 1, '{{3,2},{-4,2}}')); 
INSERT INTO t.f VALUES ('F-E3+E4',
  topology.CreateTopoGeom('t', 2, 1, '{{-3,2},{4,2}}')); 

SELECT r.topogeo_id, r.element_id
  FROM t.relation r, t.f f WHERE 
  r.layer_id = layer_id(f.g) AND r.topogeo_id = id(f.g)
  AND r.topogeo_id in (2,3)
  ORDER BY r.layer_id, r.topogeo_id;

-- This is fine, but will have to tweak definition of
-- 'F+E3-E4' and 'F-E3+E4'
SELECT 'MH(3,4)', topology.ST_NewEdgeHeal('t', 3, 4);

-- This is for ticket #942
SELECT topology.ST_NewEdgeHeal('t', 1, 5);

SELECT r.topogeo_id, r.element_id
  FROM t.relation r, t.f f WHERE 
  r.layer_id = layer_id(f.g) AND r.topogeo_id = id(f.g)
  AND r.topogeo_id in (2,3)
  ORDER BY r.layer_id, r.topogeo_id;

SELECT topology.DropTopology('t');

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

SELECT '#1955', topology.CreateTopology('t') > 1;

SELECT '#1955.1', 'E'||topology.AddEdge('t', 'LINESTRING(0 0, 10 0, 10 10)');        -- 1
SELECT '#1955.1', 'E'||topology.AddEdge('t', 'LINESTRING(0 0, 0 10, 10 10)'); ;      -- 2

SELECT '#1955.1', count(node_id), 'start nodes' as label FROM t.node GROUP BY label; 

-- Deletes second node. Not very predictable which one is removed
SELECT '#1955.1', 'H:1,2', 'E' || topology.ST_NewEdgeHeal('t', 1, 2), 'created';

SELECT '#1955.1', count(node_id), 'nodes left' as label FROM t.node GROUP BY label; 

SELECT '#1955.2', 'E'||topology.AddEdge('t', 'LINESTRING(50 0, 60 0, 60 10)');        -- 4
SELECT '#1955.2', 'E'||topology.AddEdge('t', 'LINESTRING(50 0, 50 10, 60 10)'); ;     -- 5
SELECT '#1955.2', 'E'||topology.AddEdge('t', 'LINESTRING(60 10, 70 10)'); ;           -- 6

SELECT '#1955.2', count(node_id), 'start nodes' as label FROM t.node GROUP BY label; 

-- Only the start node can be deleted (50 0) because the other is shared by
-- another edge 
SELECT '#1955.2', 'H:4,5', 'E' || topology.ST_NewEdgeHeal('t', 4, 5), 'created';

SELECT '#1955.2', count(node_id), 'nodes left' as label FROM t.node GROUP BY label; 

SELECT '#1955.3', 'E'||topology.AddEdge('t', 'LINESTRING(80 0, 90 0, 90 10)');        -- 8
SELECT '#1955.3', 'E'||topology.AddEdge('t', 'LINESTRING(80 0, 80 10, 90 10)'); ;     -- 9
SELECT '#1955.3', 'E'||topology.AddEdge('t', 'LINESTRING(70 10, 80 0)'); ;            -- 10

SELECT '#1955.3', count(node_id), 'start nodes' as label FROM t.node GROUP BY label; 

-- Only the end node can be deleted (90 10) because the other is shared by
-- another edge 
SELECT '#1955.3', 'H:8,9', 'E' || topology.ST_NewEdgeHeal('t', 8, 9), 'created';

SELECT '#1955.3', count(node_id), 'nodes left' as label FROM t.node GROUP BY label; 

SELECT '#1955', topology.DropTopology('t');

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------

-- Another case of merging edges sharing both endpoints
-- See http://trac.osgeo.org/postgis/ticket/1998

SELECT '#1998.+', CreateTopology('t1998') > 1;
SELECT '#1998.N1', ST_AddIsoNode('t1998', 0, 'POINT(1 1)');
SELECT '#1998.N2', ST_AddIsoNode('t1998', 0, 'POINT(0 0)');
SELECT '#1998.E1', ST_AddEdgeModFace('t1998', 1, 1, 'LINESTRING(1 1,1 2,2 2,2 1,1 1)');
SELECT '#1998.E2', ST_AddEdgeModFace('t1998', 2, 1, 'LINESTRING(0 0,0 1,1 1)');
SELECT '#1998.E3', ST_AddEdgeModFace('t1998', 1, 2, 'LINESTRING(1 1,1 0,0 0)');
SELECT '#1998.X0' as lbl, count(*) FROM ValidateTopology('t1998') GROUP BY lbl;
SELECT '#1998.NE', ST_NewEdgeHeal('t1998', 2, 3);
SELECT '#1998.NE4', ST_AsText(geom) FROM t1998.edge WHERE edge_id = 4;
SELECT '#1998.X1' as lbl, count(*) FROM ValidateTopology('t1998') GROUP BY lbl;
SELECT '#1998.-', topology.DropTopology('t1998');

-------------------------------------------------------------------------
-------------------------------------------------------------------------
-------------------------------------------------------------------------


-- TODO: test registered but unexistent topology
-- TODO: test registered but corrupted topology
--       (missing node, edge, relation...)
