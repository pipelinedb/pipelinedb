set client_min_messages to ERROR;

SELECT topology.CreateTopology('tt') > 0;

SELECT 'e1',  topology.addEdge('tt', 'LINESTRING(0 0, 10 0)');
SELECT 'e2',  topology.addEdge('tt', 'LINESTRING(10 0, 10 10)');
SELECT 'e3',  topology.addEdge('tt', 'LINESTRING(0 10, 10 10)');
SELECT 'e4',  topology.addEdge('tt', 'LINESTRING(0 0, 0 10)');
SELECT 'e5',  topology.addEdge('tt', 'LINESTRING(0 0, 0 -10)');
SELECT 'e6',  topology.addEdge('tt', 'LINESTRING(10 10, 20 10)');
SELECT 'e7',  topology.addEdge('tt', 'LINESTRING(20 10, 20 0)');
SELECT 'e8',  topology.addEdge('tt', 'LINESTRING(20 0, 10 0)');
SELECT 'e9',  topology.addEdge('tt', 'LINESTRING(10 0, 0 -10)');
SELECT 'e10',  topology.addEdge('tt', 'LINESTRING(2 2, 5 2, 2 5)');
SELECT 'e11',  topology.addEdge('tt', 'LINESTRING(2 2, 2 5)');

-- Call, check linking
SELECT topology.polygonize('tt');
SELECT face_id, Box2d(mbr) from tt.face ORDER by face_id;
SELECT edge_id, left_face, right_face from tt.edge ORDER by edge_id;

-- Call again and recheck linking (shouldn't change anything)
SELECT topology.polygonize('tt');
SELECT face_id, Box2d(mbr) from tt.face ORDER by face_id;
SELECT edge_id, left_face, right_face from tt.edge ORDER by edge_id;

SELECT topology.DropTopology('tt');

