set client_min_messages to WARNING;

\i load_topology.sql
\i load_features.sql
\i hierarchy.sql

-- This edges perturbate the topology so that walking around the boundaries
-- of P1 and P2 may require walking on some of them
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(9 14, 15 10)');
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(21 14, 15 18)');
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(21 14, 28 10)');
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(35 14, 28 18)');

--- Lineal non-hierarchical 
SELECT 'L1-vanilla', feature_name, topology.AsTopoJSON(feature, NULL)
 FROM features.city_streets
 WHERE feature_name IN ('R3', 'R4', 'R1', 'R2' )
 ORDER BY feature_name;

--- Lineal hierarchical 
SELECT 'L2-vanilla', feature_name, topology.AsTopoJSON(feature, NULL)
 FROM features.big_streets
 WHERE feature_name IN ('R4', 'R1R2' )
 ORDER BY feature_name;

--- Areal non-hierarchical
SELECT 'A1-vanilla', feature_name, topology.AsTopoJSON(feature, NULL)
 FROM features.land_parcels
 WHERE feature_name IN ('P1', 'P2', 'P3', 'P4', 'P5' )
 ORDER BY feature_name;

--- Areal hierarchical
SELECT 'A2-vanilla', feature_name, topology.AsTopoJSON(feature, NULL)
 FROM features.big_parcels
 WHERE feature_name IN ('P1P2', 'P3P4')
 ORDER BY feature_name;

-- Now again with edge mapping {
CREATE TEMP TABLE edgemap (arc_id serial, edge_id int unique);

--- Lineal non-hierarchical 
SELECT 'L1-edgemap', feature_name, topology.AsTopoJSON(feature, 'edgemap')
 FROM features.city_streets
 WHERE feature_name IN ('R3', 'R4', 'R1', 'R2' )
 ORDER BY feature_name;

--- Lineal hierarchical 
TRUNCATE edgemap; SELECT NULLIF(setval('edgemap_arc_id_seq', 1, false), 1);
SELECT 'L2-edgemap', feature_name, topology.AsTopoJSON(feature, 'edgemap')
 FROM features.big_streets
 WHERE feature_name IN ('R4', 'R1R2' )
 ORDER BY feature_name;

--- Areal non-hierarchical
TRUNCATE edgemap; SELECT NULLIF(setval('edgemap_arc_id_seq', 1, false), 1);
SELECT 'A1-edgemap', feature_name, topology.AsTopoJSON(feature, 'edgemap')
 FROM features.land_parcels
 WHERE feature_name IN ('P1', 'P2', 'P3', 'P4', 'P5' )
 ORDER BY feature_name;

--- Areal hierarchical
TRUNCATE edgemap; SELECT NULLIF(setval('edgemap_arc_id_seq', 1, false), 1);
SELECT 'A2-edgemap', feature_name, topology.AsTopoJSON(feature, 'edgemap')
 FROM features.big_parcels
 WHERE feature_name IN ('P1P2', 'P3P4')
 ORDER BY feature_name;

DROP TABLE edgemap;
-- End edge mapping }

-- This edge splits an hole in two faces
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(4 31, 7 34)');

-- This edge wraps a couple of faces, to test holes at 2 level distance from parent
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(0 25, 33 25, 33 44, 0 44, 0 25)');

-- Now add a new polygon
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(3 47, 33 47, 33 52, 3 52, 3 47)');
SELECT 'E' || TopoGeo_addLinestring('city_data', 'LINESTRING(10 48, 16 48, 16 50, 10 50, 10 48)');

-- And this defines a new feature including both face 1and the new 
-- wrapping face 11 plus the new (holed) face 12
INSERT INTO features.land_parcels VALUES ('P6', 
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{1,3},{11,3},{12,3}}'));

SELECT 'A3-vanilla', feature_name, topology.AsTopoJSON(feature, null)
 FROM features.land_parcels
 WHERE feature_name IN ('P6')
 ORDER BY feature_name;

SELECT topology.DropTopology('city_data');
DROP SCHEMA features CASCADE;
