set client_min_messages to ERROR;

\i load_topology.sql


-- good one
SELECT 'T1', topology.ST_ChangeEdgeGeom('city_data', 25,
 'LINESTRING(9 35, 11 33, 13 35)');

-- start/end points mismatch
SELECT topology.ST_ChangeEdgeGeom('city_data', 25,
 'LINESTRING(10 35, 13 35)');
SELECT topology.ST_ChangeEdgeGeom('city_data', 25,
 'LINESTRING(9 35, 13 36)');

-- Node crossing 
SELECT topology.ST_ChangeEdgeGeom('city_data', 3,
  'LINESTRING(25 30, 20 36, 20 38, 25 35)');

-- Non-simple edge
SELECT topology.ST_ChangeEdgeGeom('city_data', 1,
  'LINESTRING(8 30, 9 30, 8 30)');

-- Dimensionally collapsed edge (#1774)
SELECT topology.ST_ChangeEdgeGeom('city_data', 1,
  'LINESTRING(8 30, 8 30, 8 30)');

-- Non-existent edge (#979)
SELECT topology.ST_ChangeEdgeGeom('city_data', 666,
  'LINESTRING(25 30, 20 36, 20 38, 25 35)');

-- Test edge crossing
SELECT topology.ST_ChangeEdgeGeom('city_data', 25,
 'LINESTRING(9 35, 11 40, 13 35)');

-- Test change in presence of edges sharing node (#1428)
SELECT 'T2', topology.ST_ChangeEdgeGeom('city_data', 5,
 'LINESTRING(41 40, 57 33)');

-- Change to edge crossing old self 
SELECT 'T3', topology.ST_ChangeEdgeGeom('city_data', 5,
 'LINESTRING(41 40, 49 40, 49 34, 57 33)');

-- Change a closed edge (counterclockwise)
SELECT 'T4', topology.ST_ChangeEdgeGeom('city_data', 26,
 'LINESTRING(4 31, 7 31, 4 33, 4 31)');
-- Check face update
SELECT 'T4F', ST_Equals(f.mbr, ST_Envelope(e.geom))
 FROM city_data.face f, city_data.edge e
 WHERE e.edge_id = 26 AND f.face_id = e.left_face;

-- Collisions on edge motion path is forbidden:
-- get to include a whole isolated edge
SELECT topology.ST_ChangeEdgeGeom('city_data', 26,
 'LINESTRING(4 31, 7 31, 15 34, 12 37.5, 4 34, 4 31)');

-- This movement doesn't collide:
SELECT 'T5', topology.ST_ChangeEdgeGeom('city_data', 3,
 'LINESTRING(25 30, 18 35, 18 39, 23 39, 23 36, 20 38, 19 37, 20 35, 25 35)');

-- This movement doesn't collide either:
SELECT 'T6', topology.ST_ChangeEdgeGeom('city_data', 3,
 'LINESTRING(25 30, 22 38, 25 35)');

-- This movement gets to include an isolated node:
SELECT topology.ST_ChangeEdgeGeom('city_data', 3,
 'LINESTRING(25 30, 18 35, 18 39, 23 39, 23 36, 20 35, 25 35)');

-- This movement is legit (counterclockwise closed edge)
SELECT 'T7', topology.ST_ChangeEdgeGeom('city_data', 2,
 'LINESTRING(25 30, 28 39, 16 39, 25 30)');
-- Check face update
SELECT 'T7F', ST_Equals(f.mbr, ST_Envelope(e.geom))
 FROM city_data.face f, city_data.edge e
 WHERE e.edge_id = 2 AND f.face_id = e.left_face;

-- This movement gets to exclude an isolated node:
SELECT topology.ST_ChangeEdgeGeom('city_data', 2,
 'LINESTRING(25 30, 28 39, 20 39, 25 30)');

-- This movement should be fine
SELECT 'T7.1', topology.ST_ChangeEdgeGeom('city_data', 2,
'LINESTRING(25 30, 28 39, 17 39, 25 30)');
-- Check face update
SELECT 'T7F.1', 
  ST_Equals(f.mbr, ST_Envelope(ST_GetFaceGeometry('city_data', f.face_id)))
  FROM city_data.face f, city_data.edge e
  WHERE e.edge_id = 2 AND f.face_id = e.left_face;

-- Test changing winding direction of closed edge
SELECT topology.ST_ChangeEdgeGeom('city_data', 26,
 ST_Reverse('LINESTRING(4 31, 7 31, 4 34, 4 31)'));

-- Maintain winding of closed edge (counterclockwise)
SELECT 'T8', topology.ST_ChangeEdgeGeom('city_data', 26,
 'LINESTRING(4 31, 4 30.4, 5 30.4, 4 31)');
-- Check face update
SELECT 'T8F', 
  ST_Equals(f.mbr, ST_Envelope(ST_GetFaceGeometry('city_data', f.face_id)))
  FROM city_data.face f, city_data.edge e
  WHERE e.edge_id = 26 AND f.face_id = e.left_face;

-- test changing winding of non-closed edge ring
SELECT topology.ST_ChangeEdgeGeom('city_data', 13,
 'LINESTRING(21 6, 21 2, 6 2, 6 25, 50 25, 50 2, 35 2, 35 6)');

-- test moving closed edge into another face
SELECT 'T9', ST_AddEdgeModFace('city_data', 20, 20,
 'LINESTRING(4 31, 7 31, 4 34, 4 31)');
SELECT ST_ChangeEdgeGeom('city_data', 26, -- should fail!
 'LINESTRING(4 31,5 31.5,4.6 32,4 31)');

-- test moving non-closed edge into another face
SELECT 'T10', ST_AddEdgeModFace('city_data', 17, 18,
 'LINESTRING(21 22, 28 27, 35 22)');
SELECT ST_ChangeEdgeGeom('city_data', 28, -- should fail!
 'LINESTRING(21 22, 28 18, 35 22)');

-- test enlarging a face MBR by moving an edge
SELECT 'T11', ST_ChangeEdgeGeom('city_data', 16, 
 'LINESTRING(47 6, 51 10, 47 14)');
-- Check face update
SELECT 'T11F',
  ST_Equals(f.mbr, ST_Envelope(ST_GetFaceGeometry('city_data', f.face_id)))
  FROM city_data.face f, city_data.edge e
  WHERE e.edge_id = 16 AND f.face_id = e.left_face;

-- See http://trac.osgeo.org/postgis/ticket/1775
SELECT 'T12.1', ST_AddIsoNode('city_data', 8, 'POINT(49 10)');
SELECT 'T12', ST_ChangeEdgeGeom('city_data', 16, 'LINESTRING(47 6, 47 14)');

-- See http://trac.osgeo.org/postgis/ticket/2176
SELECT 'T13.1', TopoGeo_AddLineString('city_data', '01020000001D000000E42CEC69873FF2BF9E98F56228E347400EDB16653648F2BF4985B18520E34740E92B4833164DF2BF3A1E335019E34740A94D9CDCEF50F2BF33F9669B1BE347407DAEB6627F59F2BF2CF180B229E34740758E01D9EB5DF2BFD0D556EC2FE34740533F6F2A5261F2BFD717096D39E34740F4893C49BA66F2BFC8073D9B55E34740B8239C16BC68F2BF33A7CB6262E34740AA2B9FE57970F2BF4165FCFB8CE347406DC5FEB27B72F2BFBA4E232D95E34740978BF84ECC7AF2BF24EEB1F4A1E34740E527D53E1D8FF2BF8F8D40BCAEE3474036CD3B4ED191F2BF649291B3B0E34740841266DAFE95F2BF1DE6CB0BB0E34740E3361AC05BA0F2BFB2632310AFE347405C5A0D897BACF2BF72F90FE9B7E3474031D3F6AFACB4F2BF4F232D95B7E347402B137EA99FB7F2BFD656EC2FBBE347402D431CEBE2B6F2BF551344DD07E4474011E4A08499B6F2BF15E3FC4D28E447406519E25817B7F2BF63EE5A423EE447409DD7D825AAB7F2BFE3FC4D2844E447405969520ABABDF2BF2384471B47E44740A31EA2D11DC4F2BFB1F9B83654E447400473F4F8BDCDF2BFEA5BE67459E447405070B1A206D3F2BFF19D98F562E4474062670A9DD7D8F2BF0E4FAF9465E447407FF6234564D8F2BFF1BA7EC16EE44740' );
SELECT 'T13.2', ST_ChangeEdgeGeom('city_data', 29, '010200000008000000E42CEC69873FF2BF9E98F56228E34740E92B4833164DF2BF3B1E335019E34740768E01D9EB5DF2BFD0D556EC2FE347406EC5FEB27B72F2BFBA4E232D95E34740988BF84ECC7AF2BF25EEB1F4A1E347402C137EA99FB7F2BFD656EC2FBBE347409DD7D825AAB7F2BFE4FC4D2844E447407FF6234564D8F2BFF1BA7EC16EE44740' );
-- Now add an obstacle and try to change back (should fail)
SELECT 'T13.3', TopoGeo_AddPoint('city_data', 'POINT(-1.1697 47.7825)'::geometry);
SELECT 'T13.4', ST_ChangeEdgeGeom('city_data', 29, '01020000001D000000E42CEC69873FF2BF9E98F56228E347400EDB16653648F2BF4985B18520E34740E92B4833164DF2BF3A1E335019E34740A94D9CDCEF50F2BF33F9669B1BE347407DAEB6627F59F2BF2CF180B229E34740758E01D9EB5DF2BFD0D556EC2FE34740533F6F2A5261F2BFD717096D39E34740F4893C49BA66F2BFC8073D9B55E34740B8239C16BC68F2BF33A7CB6262E34740AA2B9FE57970F2BF4165FCFB8CE347406DC5FEB27B72F2BFBA4E232D95E34740978BF84ECC7AF2BF24EEB1F4A1E34740E527D53E1D8FF2BF8F8D40BCAEE3474036CD3B4ED191F2BF649291B3B0E34740841266DAFE95F2BF1DE6CB0BB0E34740E3361AC05BA0F2BFB2632310AFE347405C5A0D897BACF2BF72F90FE9B7E3474031D3F6AFACB4F2BF4F232D95B7E347402B137EA99FB7F2BFD656EC2FBBE347402D431CEBE2B6F2BF551344DD07E4474011E4A08499B6F2BF15E3FC4D28E447406519E25817B7F2BF63EE5A423EE447409DD7D825AAB7F2BFE3FC4D2844E447405969520ABABDF2BF2384471B47E44740A31EA2D11DC4F2BFB1F9B83654E447400473F4F8BDCDF2BFEA5BE67459E447405070B1A206D3F2BFF19D98F562E4474062670A9DD7D8F2BF0E4FAF9465E447407FF6234564D8F2BFF1BA7EC16EE44740' );

-- TODO: test changing some clockwise closed edges..

SELECT topology.DropTopology('city_data');

