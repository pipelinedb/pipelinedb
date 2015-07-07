set client_min_messages to WARNING;

SELECT topology.CreateTopology('schema_topo') > 0;

select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(1 4, 4 7)')) = 1;
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(4 7, 6 9)')) = 2;
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(2 2, 4 4)')) = 3;
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(4 4, 5 5, 6 6)')) = 4;
select topology.AddEdge('schema_topo',ST_GeomFromText('LINESTRING(6 6, 6 9)')) = 5;

-- ask on a Point with tolerance zero
select topology.GetNodeByPoint('schema_topo',ST_GeomFromText('POINT(5 5)'), 0::float8);

-- ask on a Point on a node with tolerance
select topology.GetNodeByPoint('schema_topo',ST_GeomFromText('POINT(4 4)'), 0::float8);

-- Ask for a point outside from an edge with a tolerance sufficient to include one node
select topology.GetNodeByPoint('schema_topo',ST_GeomFromText('POINT(4.1 4)'), 0.3::float8);

-- Ask for a point where there isn't a node
select topology.GetNodeByPoint('schema_topo',ST_GeomFromText('POINT(5 5.5)'), 0::float8) = 0;


-- Failing cases (should all raise exceptions) -------

-- Ask for a Point with a tollerance too high
select topology.GetNodeByPoint('schema_topo',ST_GeomFromText('POINT(4 7)'), 5::float8);


SELECT topology.DropTopology('schema_topo');
