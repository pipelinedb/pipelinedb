set client_min_messages to ERROR;

SELECT topology.CreateTopology('schema_topo') > 0;

select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(1 2, 1 5)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(1 5, 10 5)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 5, 10 2)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 2, 1 2)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 5, 10 12)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 12, 10 14)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 14, 10 15)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(10 15, 15 15)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(15 15, 15 2)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(15 2, 10 2)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(1 5, 1 12)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(1 12, 7 12)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(7 12, 8 12)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(8 12, 10 12)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(7 12, 7 15, 10 15)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(8 12, 8 14, 10 14)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(4 7, 4 10)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(4 10, 6 10)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(6 10, 6 7)'));
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(6 7, 4 7)'));

select topology.addFace('schema_topo', 'POLYGON((1 2, 1 5, 10 5, 10 2, 1 2 ))');
select topology.addFace('schema_topo', 'POLYGON((10 2, 10 5, 10 12, 10 14, 10 15, 15 15, 15 2, 10 2))');
select topology.addFace('schema_topo', 'POLYGON((7 12, 7 15, 10 15, 10 14, 8 14, 8 12, 7 12))');
select topology.addFace('schema_topo', 'POLYGON((1 5, 1 12, 7 12, 8 12, 10 12, 10 5, 1 5),(4 7, 4 10, 6 10, 6 7, 4 7))');


-- ask for a Point with tolerance zero
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(7 7)'), 0::float8)::int;
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(6 7)'), 0::float8)::int;
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(5 7)'), 0::float8)::int;

-- ask for a Point where there isn't a Face
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(9 13)'), 0::float8)::int;
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(5 8)'), 0::float8)::int;

-- Ask for a point outside from an face but with a tolerance sufficient to include one face
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(8.5 13)'), 0.5::float8)::int;
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(5 8)'), 1::float8)::int;

-- Failing cases (should all raise exceptions) -------

-- Ask for Point in a Node (2 or more faces)
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(1 5)'), 0::float8)::int;

-- Ask for a Point with a tollerance too high (2 or more faces)
select topology.GetFaceByPoint('schema_topo',ST_GeomFromText('POINT(9 13)'), 1::float8)::int;


SELECT topology.DropTopology('schema_topo');
