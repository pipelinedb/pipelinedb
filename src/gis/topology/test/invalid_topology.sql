BEGIN;

-- Create the topology. 
-- NOTE:
--  Returns topology id... which depend on how many
--  topologies where created in the regress database
--  so we just check it's a number greater than 0
SELECT topology.CreateTopology('invalid_topology') > 0;

-- Insert faces
INSERT INTO invalid_topology.face(face_id) VALUES(1); -- F1
INSERT INTO invalid_topology.face(face_id) VALUES(2); -- F2
INSERT INTO invalid_topology.face(face_id) VALUES(3); -- F3
INSERT INTO invalid_topology.face(face_id) VALUES(4); -- F4
INSERT INTO invalid_topology.face(face_id) VALUES(5); -- F5
INSERT INTO invalid_topology.face(face_id) VALUES(6); -- F6
INSERT INTO invalid_topology.face(face_id) VALUES(7); -- F7
INSERT INTO invalid_topology.face(face_id) VALUES(8); -- F8
INSERT INTO invalid_topology.face(face_id) VALUES(9); -- F9
-- Next face has no edges
INSERT INTO invalid_topology.face(face_id) VALUES(10); -- F10
-- Next face is within F2
INSERT INTO invalid_topology.face(face_id) VALUES(11); -- F11
-- Next face overlaps F2
INSERT INTO invalid_topology.face(face_id) VALUES(12); -- F12
-- Next face is here only to serve as a placeholder for broken edges
INSERT INTO invalid_topology.face(face_id) VALUES(13); -- F13

-- Insert nodes
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(1, 'POINT(8 30)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(2, 'POINT(25 30)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(3, 'POINT(25 35)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(4, 'POINT(20 37)', 2);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(5, 'POINT(36 38)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(6, 'POINT(57 33)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(7, 'POINT(41 40)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(8, 'POINT(9 6)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(9, 'POINT(21 6)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(10, 'POINT(35 6)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(11, 'POINT(47 6)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(12, 'POINT(47 14)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(13, 'POINT(35 14)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(14, 'POINT(21 14)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(15, 'POINT(9 14)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(16, 'POINT(9 22)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(17, 'POINT(21 22)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(18, 'POINT(35 22)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(19, 'POINT(47 22)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(20, 'POINT(4 31)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(21, 'POINT(9 35)', NULL);
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(22, 'POINT(13 35)', NULL);

-- Next node(node_id,geom,containing_face) is coincident with N1
INSERT INTO invalid_topology.node(node_id,geom,containing_face)
	VALUES(23, 'POINT(8 30)', NULL);

-- Insert edges
INSERT INTO invalid_topology.edge VALUES(1, 1, 1, 1, -1, 1, 0,
  'LINESTRING(8 30, 16 30, 16 38, 3 38, 3 30, 8 30)');
INSERT INTO invalid_topology.edge VALUES(2, 2, 2, -3, -2, 2, 0,
  'LINESTRING(25 30, 31 30, 31 40, 17 40, 17 30, 25 30)');
INSERT INTO invalid_topology.edge VALUES(3, 2, 3, -3, 2, 2, 2,
  'LINESTRING(25 30, 25 35)');
INSERT INTO invalid_topology.edge VALUES(4, 5, 6, -5, 4, 0, 0,
    'LINESTRING(36 38, 38 35, 41 34, 42 33, 45 32, 47 28, 50 28, 52 32, 57 33)');
INSERT INTO invalid_topology.edge VALUES(5, 7, 6, -4, 5, 0, 0,
    'LINESTRING(41 40, 45 40, 47 42, 62 41, 61 38, 59 39, 57 36, 57 33)');
INSERT INTO invalid_topology.edge VALUES(6, 16, 17, 7, -21, 0, 3,
    'LINESTRING(9 22, 21 22)');
INSERT INTO invalid_topology.edge VALUES(7, 17, 18, 8, -19, 0, 4,
    'LINESTRING(21 22, 35 22)');
INSERT INTO invalid_topology.edge VALUES(8, 18, 19, -15, -17, 0, 5,
    'LINESTRING(35 22, 47 22)');
INSERT INTO invalid_topology.edge VALUES(9, 15, 14, 19, -22, 3, 6,
    'LINESTRING(9 14, 21 14)');
INSERT INTO invalid_topology.edge VALUES(10, 13, 14, -20, 17, 7, 4,
    'LINESTRING(35 14, 21 14)');
INSERT INTO invalid_topology.edge VALUES(11, 13, 12, 15, -18, 5, 8,
    'LINESTRING(35 14, 47 14)');
INSERT INTO invalid_topology.edge VALUES(12, 8, 9, 20, 22, 6, 0,
    'LINESTRING(9 6, 21 6)');
INSERT INTO invalid_topology.edge VALUES(13, 9, 10, 18, -12, 7, 0,
    'LINESTRING(21 6, 35 6)');
INSERT INTO invalid_topology.edge VALUES(14, 10, 11, 16, -13, 8, 0,
    'LINESTRING(35 6, 47 6)');
INSERT INTO invalid_topology.edge VALUES(15, 12, 19, -8, -16, 5, 0,
    'LINESTRING(47 14, 47 22)');
INSERT INTO invalid_topology.edge VALUES(16, 11, 12, -11, -14, 8, 0,
    'LINESTRING(47 6, 47 14)');
INSERT INTO invalid_topology.edge VALUES(17, 13, 18, -7, 11, 4, 5,
    'LINESTRING(35 14, 35 22)');
INSERT INTO invalid_topology.edge VALUES(18, 10, 13, 10, 14, 7, 8,
    'LINESTRING(35 6, 35 14)');
INSERT INTO invalid_topology.edge VALUES(19, 14, 17, -6, -10, 3, 4,
    'LINESTRING(21 14, 21 22)');
INSERT INTO invalid_topology.edge VALUES(20, 9, 14, -9, 13, 6, 7,
    'LINESTRING(21 6, 21 14)');
INSERT INTO invalid_topology.edge VALUES(21, 15, 16, 6, 9, 0, 3,
    'LINESTRING(9 14, 9 22)');
INSERT INTO invalid_topology.edge VALUES(22, 8, 15, 21, 12, 0, 6,
    'LINESTRING(9 6, 9 14)');
INSERT INTO invalid_topology.edge VALUES(25, 21, 22, -25, 25, 1, 1,
  'LINESTRING(9 35, 13 35)');
INSERT INTO invalid_topology.edge VALUES(26, 20, 20, 26, -26, 9, 1,
  'LINESTRING(4 31, 7 31, 7 34, 4 34, 4 31)');
-- Next edge crosses node N14
INSERT INTO invalid_topology.edge VALUES(27, 15, 13, 27, -27, 6, 3,
  'LINESTRING(9 14, 21 14, 35 14)');
-- Next edge is not simple 
INSERT INTO invalid_topology.edge VALUES(28, 3, 3, 28, 28, 2, 2,
  'LINESTRING(25 35, 28 30, 27 27, 27 32, 25 35)');
-- Next edge crosses E2
INSERT INTO invalid_topology.edge VALUES(29, 17, 3, 3, 3, 0, 0,
  'LINESTRING(21 22, 25 35)');
-- Next edge's start and end points do not match Nodes geometries
INSERT INTO invalid_topology.edge VALUES(30, 4, 3, 30, 30, 4, 4,
  'LINESTRING(21 37, 20 37, 20 38)');

-- Next edge bounds face F11 as within face F2
INSERT INTO invalid_topology.edge VALUES(31, 3, 3, 31, -31, 11, 11,
  'LINESTRING(25 35, 25 36, 26 36, 26 35, 25 35)');

-- Next edge bounds face F12 as overlapping face F2
INSERT INTO invalid_topology.edge VALUES(32, 4, 4, 31, -31, 12, 12,
  'LINESTRING(20 37, 20 42, 21 42, 21 37, 20 37)');

-- Next edge is not valid 
INSERT INTO invalid_topology.edge VALUES(33, 3, 3, 28, 28, 13, 13,
  '01020000000100000000000000000039400000000000804140');

-- Validate topology
SELECT * from topology.validatetopology('invalid_topology');

END;
