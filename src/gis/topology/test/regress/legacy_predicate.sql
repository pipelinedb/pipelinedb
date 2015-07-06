set client_min_messages to WARNING;

\i load_topology.sql
\i load_features.sql
\i more_features.sql
\i hierarchy.sql
\i topo_predicates.sql

-- clean up
SELECT topology.DropTopology('city_data');
DROP SCHEMA features CASCADE;
