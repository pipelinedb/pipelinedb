\set VERBOSITY terse
set client_min_messages to WARNING;

SELECT topology.CreateTopology('tt3d', -1, 0, true) > 0;

COPY tt3d.face(face_id) FROM STDIN;
1
\.

COPY tt3d.node(node_id, geom) FROM STDIN;
1	POINT(0 0 30)
2	POINT(10 10 20)
\.

COPY tt3d.edge_data(
	edge_id, start_node, end_node,
	abs_next_left_edge, abs_next_right_edge,
	next_left_edge, next_right_edge,
        left_face, right_face, geom) FROM STDIN;
1	1	2	2	2	2	2	0	1	LINESTRING(0 0 30, 0 10 25, 10 10 20)
2	2	1	1	1	1	1	0	1	LINESTRING(10 10 20, 10 0 18, 0 0 30)
\.

-- 2.5d face geometries
CREATE TABLE public.faces (id serial);
SELECT topology.AddTopoGeometryColumn('tt3d', 'public', 'faces', 'g',
	'POLYGON');
INSERT INTO public.faces (g) VALUES (
  topology.CreateTopoGeom(
    'tt3d', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{1,3}}') -- face_id:1
    );

-- 2.5d line geometries
CREATE TABLE lines (id serial);
SELECT topology.AddTopoGeometryColumn('tt3d', 'public', 'lines', 'g',
	'LINE');
INSERT INTO public.lines (g) VALUES (
  topology.CreateTopoGeom(
    'tt3d', -- Topology name
    2, -- Topology geometry type (lineal)
    2, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{1,2},{2,2}}') -- edge_id:1 edge_id:2
    );

SELECT 'f'||id, ST_AsEWKT(topology.geometry(g)) from faces;
SELECT 'l'||id, ST_AsEWKT(topology.geometry(g)) from public.lines;

SELECT topology.DropTopology('tt3d');
DROP TABLE lines;
DROP TABLE faces;
