set client_min_messages to ERROR;
-- Drop corrupted topology (with missing layer tables)
-- See http://trac.osgeo.org/postgis/ticket/3016
SELECT topology.CreateTopology('t1') > 0;
CREATE TABLE t1f (id int);
SELECT topology.AddTopoGeometryColumn('t1', 'public', 't1f', 'geom_t1', 'LINE') > 0;
DROP TABLE t1.relation;
SELECT topology.DropTopoGeometryColumn('public','t1f','geom_t1');
DROP TABLE t1f;
SELECT topology.DropTopology('t1');
