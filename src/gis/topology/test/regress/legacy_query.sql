set client_min_messages to WARNING;

-- Tests TopoGeometry->Geometry cast and id(TopoGeometry)

\i load_topology.sql
\i load_features.sql
\i more_features.sql
\i hierarchy.sql
\i query_features.sql

-- clean up
SELECT topology.DropTopology('city_data');
DROP SCHEMA features CASCADE;
