set client_min_messages to WARNING;
\set VERBOSITY terse

-- Usual city_data input 

\i load_topology.sql


-- NULL exceptions
select ST_AddIsoNode(NULL, 0, 'POINT(1 4)');
select ST_AddIsoNode('tt', 0, NULL);
select ST_AddIsoNode('tt', NULL, NULL);
select ST_AddIsoNode(NULL, NULL, NULL);
-- Wrong topology name
select ST_AddIsoNode('wrong_name', 0, 'POINT(1 4)');
select ST_AddIsoNode('', 0, 'POINT(1 4)');
-- Negative idface';
select ST_AddIsoNode('city_data', -1, 'POINT(1 4)');
-- Wrong idface
select ST_AddIsoNode('city_data', 5, 'POINT(5 33)'); -- in face 9
select ST_AddIsoNode('city_data', 9, 'POINT(39 18)'); -- in face 5
--  Coincident nodes';
select ST_AddIsoNode('city_data', 0, 'POINT(21 22)');
select ST_AddIsoNode('city_data', NULL, 'POINT(21 22)');
select ST_AddIsoNode('city_data', 1, 'POINT(21 22)');

-- Smart creation ISO Node (without know idface)';

-- in face 5
select 1 as id, ST_AddIsoNode('city_data', NULL, 'POINT(39 18)') as n
  into nn;
insert into nn -- in universe
select '2', ST_AddIsoNode('city_data', NULL, 'POINT(50 18)');
insert into nn -- in face 9
select '3', ST_AddIsoNode('city_data', NULL, 'POINT(5 33)');

-- Explicit face

insert into nn -- in face 5
select '4', ST_AddIsoNode('city_data', 5, 'POINT(42 18)');
insert into nn -- in universe
select '5', ST_AddIsoNode('city_data', 0, 'POINT(50 17)');
insert into nn -- in face 9
select '6', ST_AddIsoNode('city_data', 9, 'POINT(5 32)');

SELECT 'T'||t.id, n.node_id, n.containing_face FROM nn t, city_data.node n
  WHERE t.n = n.node_id ORDER BY t.id;

-- TODO: test for bug #1503
--SELECT 'T5', st_addisonode('city_data', 22, 'POINT(28.5 32.5)');

DROP TABLE nn;
select topology.DropTopology('city_data');
