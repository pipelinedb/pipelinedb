set client_min_messages to ERROR;

-- Test with zero tolerance

SELECT topology.CreateTopology('tt3d', -1, 0, true) > 0;

-- Create 4 edges

SELECT 'e1',  topology.addEdge('tt3d', 'LINESTRING(0 0 10, 10 0 20)');
SELECT 'e2',  topology.addEdge('tt3d', 'LINESTRING(10 0 20, 10 10 30)');
SELECT 'e3',  topology.addEdge('tt3d', 'LINESTRING(0 10 20, 10 10 30)');
SELECT 'e4',  topology.addEdge('tt3d', 'LINESTRING(0 0 10, 0 10 20)');

-- Register a face with no holes
SELECT 'f1',  topology.addFace('tt3d', 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))');

-- Check added faces
SELECT face_id, st_asewkt(mbr) from tt3d.face ORDER by face_id;

-- Check linking
SELECT edge_id, left_face, right_face from tt3d.edge ORDER by edge_id;

SELECT topology.DropTopology('tt3d');

