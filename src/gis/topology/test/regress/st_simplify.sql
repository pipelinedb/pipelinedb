set client_min_messages to WARNING;

SELECT CreateTopology('tt') > 0;
CREATE TABLE tt.areas(id serial, g geometry);
INSERT INTO tt.areas(g) VALUES ('POLYGON((0 0,1 1,1 3,0 4,-2 3,-1 1,0 0))'),
                               ('POLYGON((0 0,1 1,1 3,2 3,2 0,0 0))');
CREATE TEMP TABLE _test_layers AS SELECT 1 as id,
  AddTopoGeometryColumn('tt', 'tt', 'areas', 'tg', 'polygon') as layer_id;
SELECT 'L' || layer_id FROM _test_layers WHERE id = 1;
UPDATE tt.areas SET tg = toTopoGeom(g, 'tt', 1);

-- ensures this point won't be removed
SELECT 'N' || TopoGeo_addPoint('tt', 'POINT(1 3)');

SELECT 'S1',
  -- Point 1 3 is removed when simplifying the simple (unconstrained) geometry
  ST_Equals(ST_Simplify( g, 1), 'POLYGON((0 0,1 3,-2 3,0 0))'),
  ST_Equals(ST_Simplify(tg, 1), 'POLYGON((0 0,1 3,-2 3,0 0))')
FROM tt.areas WHERE id = 1;
SELECT 'S2',
  ST_Equals(ST_Simplify( g, 1), 'POLYGON((0 0,1 3,2 0,0 0))'),
  ST_Equals(ST_Simplify(tg, 1), 'POLYGON((0 0,1 3,2 0,0 0))')
FROM tt.areas WHERE id = 2;

-- Test hierarchical -- see http://trac.osgeo.org/postgis/ticket/2547

CREATE TABLE tt.bigareas(id serial, g geometry);
INSERT INTO _test_layers SELECT 2,
  AddTopoGeometryColumn('tt', 'tt', 'bigareas', 'tg', 'polygon', layer_id)
  FROM _test_layers WHERE id = 1;
SELECT 'L' || layer_id FROM _test_layers WHERE id = 2;
INSERT INTO tt.bigareas (tg) SELECT
  topology.CreateTopoGeom( 'tt', 3,
    (select layer_id from _test_layers where id = 2),
    TopoElementArray_agg(ARRAY[r.topogeo_id, r.layer_id]))
  FROM tt.relation r, _test_layers l1
  WHERE r.layer_id = l1.layer_id AND l1.id = 1
  GROUP BY r.topogeo_id ORDER BY r.topogeo_id;
UPDATE tt.bigareas a SET g = st_multi(o.g) FROM tt.areas o WHERE a.id = o.id;

SELECT 'HS1',
  -- Point 1 3 is removed when simplifying the simple (unconstrained) geometry
  ST_Equals(ST_Simplify( g, 1), 'POLYGON((0 0,1 3,-2 3,0 0))'),
  ST_Equals(ST_Simplify(tg, 1), 'POLYGON((0 0,1 3,-2 3,0 0))')
FROM tt.bigareas WHERE id = 1;
SELECT 'HS2',
  ST_Equals(ST_Simplify( g, 1), 'POLYGON((0 0,1 3,2 0,0 0))'),
  ST_Equals(ST_Simplify(tg, 1), 'POLYGON((0 0,1 3,2 0,0 0))')
FROM tt.bigareas WHERE id = 2;

SELECT DropTopology('tt') IS NULL;
