set client_min_messages to WARNING;

\i invalid_topology.sql

-- clean up
SELECT topology.DropTopology('invalid_topology');
