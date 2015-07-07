\set VERBOSITY terse
set client_min_messages to ERROR;

\i load_topology.sql

-- Invalid calls
SELECT 'invalid', TopoGeo_addPoint('city_data', 'LINESTRING(36 26, 38 30)');
SELECT 'invalid', TopoGeo_addPoint('city_data', 'MULTIPOINT((36 26))');
SELECT 'invalid', TopoGeo_addPoint('invalid', 'POINT(36 26)');

-- Save max node id
select 'node'::text as what, max(node_id) INTO city_data.limits FROM city_data.node;

-- Isolated point in universal face
SELECT 'iso_uni', TopoGeo_addPoint('city_data', 'POINT(38 26)');

-- Isolated point in face 3
SELECT 'iso_f3', TopoGeo_addPoint('city_data', 'POINT(16 18)');

-- Existing isolated node
SELECT 'iso_ex', TopoGeo_addPoint('city_data', 'POINT(38 26)');

-- Existing isolated node within tolerance
SELECT 'iso_ex_tol', TopoGeo_addPoint('city_data', 'POINT(38 27)', 1.5);

-- Existing non-isolated node
SELECT 'noniso_ex', TopoGeo_addPoint('city_data', 'POINT(25 30)');

-- Existing non-isolated node within tolerance (closer to edge)
SELECT 'noniso_ex_tol', TopoGeo_addPoint('city_data', 'POINT(26 30.2)', 3);

-- Splitting edge 
SELECT 'split', TopoGeo_addPoint('city_data', 'POINT(26 30.2)', 1);

-- Check effect on nodes
SELECT 'N', n.node_id, n.containing_face, ST_AsText(n.geom)
FROM city_data.node n WHERE n.node_id > (
  SELECT max FROM city_data.limits WHERE what = 'node'::text )
ORDER BY n.node_id;

-- Check effect on edges (there should be one split)
WITH limits AS ( SELECT max FROM city_data.limits WHERE what = 'node'::text )
SELECT 'E', n.edge_id, n.start_node, n.end_node 
 FROM city_data.edge n, limits m
 WHERE n.start_node > m.max
    OR n.end_node > m.max
ORDER BY n.edge_id;

-- Test precision
SELECT 'prec1', 'N' || topogeo_addpoint('city_data', 'POINT(39 26)', 2);
SELECT 'prec2', 'N' || topogeo_addpoint('city_data', 'POINT(39 26)', 1);
SELECT 'prec3', 'N' || topogeo_addpoint('city_data', 'POINT(36 26)', 1);

SELECT DropTopology('city_data');

-- See http://trac.osgeo.org/postgis/ticket/2033
SELECT 'tt2033.start', CreateTopology('t',0,0,true) > 0;
SELECT 'tt2033', 'E' || topogeo_addlinestring('t', 'LINESTRING(0 0 0,0 1 0,0 2 1)');
SELECT 'tt2033', 'N' || topogeo_addpoint('t', 'POINT(0.2 1 1)', 0.5);
SELECT 'tt2033', 'NC', node_id, ST_AsText(geom) FROM t.node ORDER BY node_id;
SELECT 'tt2033.end' || DropTopology('t');

