set client_min_messages to WARNING;

\i load_topology.sql
\i validate_topology.sql

-- clean up
SELECT topology.DropTopology('city_data');

-- Test for #1612
SELECT CreateTopology('tt') > 0;
SELECT 'Empty topology errors', count(*) FROM ValidateTopology('tt');
SELECT DropTopology('tt');
