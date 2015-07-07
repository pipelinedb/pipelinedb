set client_min_messages to WARNING;
\set VERBOSITY terse

select createtopology('tt') > 0;
select addtopogeometrycolumn('tt','public','feature','tg','POINT'); -- fail
create table feature(id integer);
select addtopogeometrycolumn('tt','public','feature','tg','BOGUS'); -- fail
select addtopogeometrycolumn('tt','public','feature','tg','POINT', 0); -- fail

-- Expect first good call returning 1
select 'T1', addtopogeometrycolumn('tt','public','feature','tg','POINT');

-- Check that you can add a second topogeometry column to the same table
select 'T2', addtopogeometrycolumn('tt','public','feature','tg2','LINE');

-- Check polygonal
select 'T3', addtopogeometrycolumn('tt','public','feature','tg3','POLYGON');

-- Check collection
select 'T4', addtopogeometrycolumn('tt','public','feature','tg4','COLLECTION');

-- Check alternate names
select 'T5', addtopogeometrycolumn('tt','public','feature',
	'tg5','ST_MultiPoint');
select 'T6', addtopogeometrycolumn('tt','public','feature',
	'tg6','ST_MultiLineString');
select 'T7', addtopogeometrycolumn('tt','public','feature',
	'tg7','ST_MultiPolygon');
select 'T8', addtopogeometrycolumn('tt','public','feature',
	'tg8','GEOMETRYCOLLECTION');
select 'T9', addtopogeometrycolumn('tt','public','feature',
	'tg9','PUNtal');
select 'T10', addtopogeometrycolumn('tt','public','feature',
	'tg10','Lineal');
select 'T11', addtopogeometrycolumn('tt','public','feature',
	'tg11','Areal');
select 'T12', addtopogeometrycolumn('tt','public','feature',
	'tg12','GEOMETRY');

select l.layer_id, l.schema_name, l.table_name, l.feature_column,
 l.feature_type, l.level, l.child_id 
from topology.layer l, topology.topology t
where l.topology_id = t.id and t.name = 'tt'
order by l.layer_id;

drop table feature;
select droptopology('tt');
