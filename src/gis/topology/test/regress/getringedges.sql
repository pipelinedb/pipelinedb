set client_min_messages to ERROR;

\i load_topology.sql

SELECT 'R'||edge_id, (topology.GetRingEdges('city_data', edge_id)).* 
	FROM city_data.edge;

SELECT 'R-'||edge_id, (topology.GetRingEdges('city_data', -edge_id)).* 
	FROM city_data.edge;

SELECT topology.DropTopology('city_data');
