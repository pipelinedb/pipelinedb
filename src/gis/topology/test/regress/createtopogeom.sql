\set VERBOSITY terse
set client_min_messages to ERROR;

SELECT topology.CreateTopology('MiX') > 0;

-- Fails due to missing layer 1
SELECT topology.CreateTopoGeom(
    'MiX', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{3,3},{6,3}}'); -- face_id:3 face_id:6

CREATE TABLE "MiX".poi (id int);
SELECT 'l1', topology.AddTopoGeometryColumn('MiX', 'MiX', 'poi', 'feat', 'POINT');

-- A Layer of type 1 (POINT) cannot contain a TopoGeometry of type 2 (LINE)
SELECT topology.CreateTopoGeom( 'MiX', 2, 1, '{{12,2}}'); 
-- A Layer of type 1 (POINT) cannot contain a TopoGeometry of type 3 (POLY)
SELECT topology.CreateTopoGeom( 'MiX', 3, 1, '{{13,3}}'); 
-- A Layer of type 1 (POINT) cannot contain a TopoGeometry of type 4 (COLL.)
SELECT topology.CreateTopoGeom( 'MiX', 4, 1, '{{12,2}}'); 

-- Node 78 does not exist in topology MiX (trigger on "relation" table)
SELECT topology.CreateTopoGeom( 'MiX', 1, 1, '{{78,1}}'); 

SELECT 'n1',  topology.addNode('MiX', 'POINT(0 0)');

-- Success !
SELECT layer_id(tg), id(tg), type(tg) FROM (
 SELECT topology.CreateTopoGeom( 'MiX', 1, 1, '{{1,1}}') as tg
) foo; 

-- Invalid TopoGeometry type (out of range)
SELECT CreateTopoGeom( 'MiX', 5, 1, '{{1,1}}'); 
SELECT CreateTopoGeom( 'MiX', 0, 1, '{{1,1}}'); 

CREATE TABLE "MiX".f_lineal (id int);
SELECT 'l2', AddTopoGeometryColumn('MiX', 'MiX', 'f_lineal', 'feat', 'LINE');

SELECT 'n2', addNode('MiX', 'POINT(10 0)');
SELECT 'e1', addEdge('MiX', 'LINESTRING(0 0, 10 0)');

SELECT CreateTopoGeom( 'MiX', 2, 2, '{{1,1}}'); -- wrong prim. type
SELECT 'L1', ST_AsText(CreateTopoGeom( 'MiX', 2, 2, '{{1,2}}')); -- fine

CREATE TABLE "MiX".f_areal (id int);
SELECT 'l3', AddTopoGeometryColumn('MiX', 'MiX', 'f_areal', 'feat', 'POLYGON');

SELECT 'e2', addEdge('MiX', 'LINESTRING(10 0, 5 5, 0 0)');
SELECT 'f1', addFace('MiX', 'POLYGON((0 0, 10 0, 5 5, 0 0))');

SELECT 'A1', CreateTopoGeom( 'MiX', 2, 3, '{{1,3}}'); -- wrong tg type
SELECT 'A1', CreateTopoGeom( 'MiX', 3, 3, '{{1,2}}'); -- wrong prim. type
SELECT 'A1', ST_AsText(CreateTopoGeom( 'MiX', 3, 3, '{{1,3}}')); -- fine

CREATE TABLE "MiX".f_mix (id int);
SELECT 'l4', AddTopoGeometryColumn('MiX', 'MiX', 'f_mix', 'feat', 'COLLECTION');
SELECT 'MP', ST_AsText(CreateTopoGeom( 'MiX', 1, 4, '{{1,1}}')); -- fine point
SELECT 'ML', ST_AsText(CreateTopoGeom( 'MiX', 2, 4, '{{1,2}}')); -- fine line
SELECT 'MA', ST_AsText(CreateTopoGeom( 'MiX', 3, 4, '{{1,3}}')); -- fine area
SELECT 'MM', ST_AsText(CreateTopoGeom( 'MiX', 4, 4, '{{1,3},{1,2},{1,1}}')); -- fine mix

-- Test emptyness {
SELECT 'POINT EMPTY', ST_AsEWKT( CreateTopoGeom( 'MiX', 1, 4, '{{0,0}}' ) );
SELECT 'LINESTRING EMPTY', ST_AsEWKT( CreateTopoGeom( 'MiX', 2, 4, '{{0,0}}' ) );
SELECT 'POLYGON EMPTY', ST_AsEWKT( CreateTopoGeom( 'MiX', 3, 4, '{{0,0}}' ) );
SELECT 'GEOMETRYCOLLECTION EMPTY', ST_AsEWKT( CreateTopoGeom( 'MiX', 4, 4, '{{0,0}}' ) );
-- } Test emptyness 

SELECT DropTopology('MiX');
