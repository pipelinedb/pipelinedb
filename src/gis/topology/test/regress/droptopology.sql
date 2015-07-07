set client_min_messages to WARNING;

SELECT topology.CreateTopology('t1') > 0;
SELECT topology.CreateTopology('t2') > 0;

CREATE TABLE t1f (id int);
SELECT topology.AddTopoGeometryColumn('t1', 'public', 't1f', 'geom_t1', 'LINE') > 0;

CREATE TABLE t2f (id int);
SELECT topology.AddTopoGeometryColumn('t2', 'public', 't2f', 'geom_t2', 'LINE') > 0;

SELECT topology.DropTopology('t1');
SELECT topology.DropTopology('t2');
DROP TABLE t2f;
DROP TABLE t1f;
