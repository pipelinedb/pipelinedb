set client_min_messages to ERROR;

-- Test with zero tolerance

SELECT topology.CreateTopology('tt') > 0;

-- Register a face in absence of edges (exception expected)
SELECT 'f*',  topology.addFace('tt', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');

-- Create 4 edges

SELECT 'e1',  topology.addEdge('tt', 'LINESTRING(0 0, 10 0)');
SELECT 'e2',  topology.addEdge('tt', 'LINESTRING(10 0, 10 10)');
SELECT 'e3',  topology.addEdge('tt', 'LINESTRING(0 10, 10 10)');
SELECT 'e4',  topology.addEdge('tt', 'LINESTRING(0 0, 0 10)');

-- Add one edge only incident on a vertex
SELECT 'e5',  topology.addEdge('tt', 'LINESTRING(0 0, 0 -10)');

-- Add 3 more edges closing a square to the right,
-- all edges with same direction

SELECT 'e6',  topology.addEdge('tt', 'LINESTRING(10 10, 20 10)');
SELECT 'e7',  topology.addEdge('tt', 'LINESTRING(20 10, 20 0)');
SELECT 'e8',  topology.addEdge('tt', 'LINESTRING(20 0, 10 0)');


-- Register a face with no holes
SELECT 'f1',  topology.addFace('tt', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');

-- Register the _same_ face  again
SELECT 'f1*',  topology.addFace('tt', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');

-- Register a face with no holes matching all edges in the same direction
SELECT 'f2',  topology.addFace('tt', 'POLYGON((10 10, 20 10, 20 0, 10 0, 10 10))');

-- Check added faces
SELECT face_id, Box2d(mbr) from tt.face ORDER by face_id;

-- Check linking
SELECT edge_id, left_face, right_face from tt.edge ORDER by edge_id;

-- Force re-registration of an existing face
SELECT 'f1-force',  topology.addFace('tt', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))', true);

-- re-check added faces and linking
SELECT face_id, Box2d(mbr) from tt.face ORDER by face_id;
SELECT edge_id, left_face, right_face from tt.edge ORDER by edge_id;

SELECT topology.DropTopology('tt');

-- Test topology with MixedCase
SELECT topology.CreateTopology('Ul') > 0;
SELECT 'MiX-e1',  topology.addEdge('Ul', 'LINESTRING(0 0, 10 0)');
SELECT 'MiX-e2',  topology.addEdge('Ul', 'LINESTRING(10 0, 10 10)');
SELECT 'MiX-e3',  topology.addEdge('Ul', 'LINESTRING(0 10, 10 10)');
SELECT 'MiX-e4',  topology.addEdge('Ul', 'LINESTRING(0 0, 0 10)');
SELECT 'MiX-f1',  topology.addFace('Ul', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');
SELECT topology.DropTopology('Ul');

-- Test polygons with holes
SELECT topology.CreateTopology('t2') > 0;

-- Edges forming two squares
SELECT 't2.e1',  topology.addEdge('t2', 'LINESTRING(0 0, 10 0)');
SELECT 't2.e2',  topology.addEdge('t2', 'LINESTRING(10 0, 10 10)');
SELECT 't2.e3',  topology.addEdge('t2', 'LINESTRING(0 10, 10 10)');
SELECT 't2.e4',  topology.addEdge('t2', 'LINESTRING(0 0, 0 10)');
SELECT 't2.e5',  topology.addEdge('t2', 'LINESTRING(10 10, 20 10)');
SELECT 't2.e6',  topology.addEdge('t2', 'LINESTRING(20 10, 20 0)');
SELECT 't2.e7',  topology.addEdge('t2', 'LINESTRING(20 0, 10 0)');

-- Clockwise hole within the square on the left
SELECT 't2.e8',  topology.addEdge('t2', 'LINESTRING(1 1, 1 2, 2 2, 2 1, 1 1)');
-- Counter-clockwise hole within the square on the left
SELECT 't2.e9',  topology.addEdge('t2', 'LINESTRING(3 1,4 1,4 2,3 2,3 1)');

-- Multi-edge hole within the square on the right
SELECT 't2.e10',  topology.addEdge('t2', 'LINESTRING(12 2, 14 2, 14 4)');
SELECT 't2.e11',  topology.addEdge('t2', 'LINESTRING(12 2, 12 4, 14 4)');

-- Register left face with two holes
SELECT 't2.f1',  topology.addFace('t2',
'POLYGON((10 5, 10 10,0 10, 0 0,10 0,10 5),
         (1 1,2 1,2 2,1 2,1 1),
         (3 1,3 2,4 2,4 1,3 1))'
);

-- Register right face with one hole
SELECT 't2.f2',  topology.addFace('t2',
'POLYGON((20 0,10 0,10 10,20 10,20 0),
         (12 2,14 2,14 4,12 4, 12 2))'
);

-- Register left hole in left square
SELECT 't2.f3',  topology.addFace('t2',
'POLYGON((1 1,2 1,2 2,1 2,1 1))'
);

-- Register right hole in left square
SELECT 't2.f4',  topology.addFace('t2',
'POLYGON((3 1,4 1,4 2,3 2,3 1))'
);

-- Register hole in right face 
SELECT 't2.f5',  topology.addFace('t2',
'POLYGON((12 2,12 4,14 4,14 2,12 2))'
);

-- Attempt to register a not-fully-defined face 
SELECT topology.addFace('t2',
'POLYGON((12 2,12 5,14 5,14 2,12 2))'
);

-- Check added faces
SELECT face_id, Box2d(mbr) from t2.face ORDER by face_id;

-- Check linking
SELECT edge_id, left_face, right_face from t2.edge ORDER by edge_id;

SELECT topology.DropTopology('t2');

--
-- Test edge touching face ring on both endpoints but not covered
-- (E1 with F1)
--
--         
--   N2 +-------.
--      |\  F1  |
--   E1 | \     | E3
--      |F2\    |
--      |  /    |
--      | /E2   |
--      |/      |
--   N1 +-------'
-- 
SELECT topology.CreateTopology('t3') > 0;

SELECT 't3.e1',  topology.addEdge('t3', 'LINESTRING(0 0, 0 10)');
SELECT 't3.e2',  topology.addEdge('t3', 'LINESTRING(0 10, 5 5, 0 0)');
SELECT 't3.e3',  topology.addEdge('t3', 'LINESTRING(0 10, 10 10, 10 0, 0 0)');

-- Register F1
SELECT 't3.f1',  topology.addFace('t3',
'POLYGON((5 5, 0 10, 10 10, 10 0, 0 0, 5 5))');

-- Register F2
SELECT 't3.f2',  topology.addFace('t3', 'POLYGON((0 0, 5 5, 0 10, 0 0))');

-- Check added faces
SELECT face_id, Box2d(mbr) from t3.face ORDER by face_id;

-- Check linking
SELECT edge_id, left_face, right_face from t3.edge ORDER by edge_id;

SELECT topology.DropTopology('t3');

--
-- Test proper updating of left/right face for contained edges
-- and nodes
--
SELECT topology.CreateTopology('t4') > 0;

SELECT 'N' || topology.addNode('t4', 'POINT(2 6)');
UPDATE t4.node set containing_face = 0 WHERE node_id = 1;

SELECT 'E' || topology.addEdge('t4', 'LINESTRING(0 0,10 0)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(10 0,10 10)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(10 10,0 10)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(0 0,0 10)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(0 0,5 5)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(5 5,6 5)');
SELECT 'E' || topology.addEdge('t4', 'LINESTRING(0 10,8 8,10 0)');

select 'F' || topology.addface('t4','POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');

-- Check edges and nodes
SELECT 'E'||edge_id, left_face, right_face from t4.edge ORDER by edge_id;
SELECT 'N'||node_id, containing_face from t4.node ORDER by node_id;

SELECT topology.DropTopology('t4');

--
-- Test narrow face. See ticket #1302.
-- {
--
SELECT '#1302', topology.CreateTopology('tt') > 0;

SELECT '#1302', 'E' || topology.addEdge('tt', '01020000000300000000917E9BA468294100917E9B8AEA2841C976BE1FA4682941C976BE9F8AEA2841B39ABE1FA46829415ACCC29F8AEA2841');
SELECT '#1302', 'E' || topology.addEdge('tt', '010200000003000000B39ABE1FA46829415ACCC29F8AEA284137894120A4682941C976BE9F8AEA284100917E9BA468294100917E9B8AEA2841');
SELECT '#1302', 'F' || topology.addFace('tt', '0103000000010000000500000000917E9BA468294100917E9B8AEA2841C976BE1FA4682941C976BE9F8AEA2841B39ABE1FA46829415ACCC29F8AEA284137894120A4682941C976BE9F8AEA284100917E9BA468294100917E9B8AEA2841');

SELECT '#1302', 'E' || edge_id, 'L' || left_face, 'R' || right_face FROM tt.edge_data ORDER BY edge_id;

SELECT '#1302', topology.DropTopology('tt');
-- }

--
-- Test face ring with endpoint matching edge endpoint
-- and tricky numbers (see #1383)
-- {
--
SELECT '#1383', CreateTopology('tt') > 0;

SELECT '#1383', 'E' || addEdge('tt', 'LINESTRING(-0.1 -10, -0.2 0)');
SELECT '#1383', 'E' || addEdge('tt', 'LINESTRING(-0.2 0, -1e-8 0)');
SELECT '#1383', 'E' || addEdge('tt', 'LINESTRING(-1e-8 0, 1 0, -0.1 -10)');

SELECT '#1383', 'F' || addFace('tt', 'POLYGON((-1e-8 0, 1 0, -0.1 -10, -0.2 0, -0.2 0, -1e-8 0))');

SELECT '#1383', 'E' || edge_id, 'L' || left_face, 'R' || right_face FROM tt.edge_data ORDER BY edge_id;

SELECT '#1383', DropTopology('tt');
-- }
