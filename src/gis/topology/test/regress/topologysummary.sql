SELECT E'--missing--\n' || TopologySummary('test');
CREATE SCHEMA test;
SELECT E'--empty--\n' || TopologySummary('test');
CREATE TABLE test.node(id int);
SELECT E'--node--\n' || TopologySummary('test');
CREATE TABLE test.edge(id int);
SELECT E'--node+edge--\n' || TopologySummary('test');
CREATE TABLE test.face(id int);
SELECT E'--node+edge+face--\n' || TopologySummary('test');
CREATE TABLE test.relation(id int);
SELECT E'--node+edge+face+corrupted_relation--\n' || TopologySummary('test');
ALTER TABLE test.relation ADD layer_id int, ADD topogeo_id int;
SELECT E'--node+edge+face+relation--\n' || TopologySummary('test');
INSERT INTO test.relation (layer_id, topogeo_id) VALUES (1,1);
SELECT E'--node+edge+face+relation+topogeom--\n' || TopologySummary('test');
INSERT INTO topology.topology (id,name,srid,precision,hasz)
  VALUES(1,'test',10,20,'f');
SELECT E'--registered_topo--\n' || TopologySummary('test');
INSERT INTO topology.layer (topology_id,layer_id, schema_name, table_name, feature_column, feature_type, level, child_id)
  VALUES(1,1,'test','t','c', 1, 0, null);
SELECT E'--registered_missing_layer_table--\n' || TopologySummary('test');
CREATE TABLE test.t(i int);
SELECT E'--registered_missing_layer_column--\n' || TopologySummary('test');
ALTER TABLE test.t ADD c TopoGeometry;
SELECT E'--registered_layer_missing_topogeom--\n' || TopologySummary('test');
INSERT INTO test.t(c) VALUES ( (1,1,1,1) );
SELECT E'--registered_layer--\n' || TopologySummary('test');
-- TODO: test hierarchical
DROP TABLE test.t;
DROP TABLE test.node;
DROP TABLE test.edge;
DROP TABLE test.face;
DROP TABLE test.relation;
DROP SCHEMA test;
SELECT E'--registered+missing--\n' || TopologySummary('test');
DELETE FROM topology.layer WHERE topology_id = 1;
DELETE FROM topology.topology WHERE id = 1;
