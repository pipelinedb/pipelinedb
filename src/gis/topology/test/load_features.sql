--
-- From examples in chapter 1.12.1 of
-- "Spatial Topology and Network Data Models" (Oracle manual)
--
-- Modified to use postgis-based topology model.
-- Loads the whole topology represented in Figure 1-1 of the
-- manual, creates TopoGeometry objects and associations.
--

--ORA--------------------------------
--ORA---- Main steps for using the topology data model with a topology
--ORA---- built from edge, node, and face data
--ORA--------------------------------
--ORA---- ...
--ORA---- 3. Create feature tables.
--ORA---- 4. Associate feature tables with the topology.
--ORA---- 5. Initialize topology
--ORA---- 6. Load feature tables using the SDO_TOPO_GEOMETRY constructor.


BEGIN;

-- 3. Create feature tables  

CREATE SCHEMA features;

CREATE TABLE features.land_parcels ( -- Land parcels (selected faces)
  feature_name VARCHAR PRIMARY KEY) with oids;
CREATE TABLE features.city_streets ( -- City streets (selected edges)
  feature_name VARCHAR PRIMARY KEY) with oids;
CREATE TABLE features.traffic_signs ( -- Traffic signs (selected nodes)
  feature_name VARCHAR PRIMARY KEY) with oids;



-- 4. Associate feature tables with the topology.
--    Add the three topology geometry layers to the CITY_DATA topology.
--    Any order is OK.
SELECT topology.AddTopoGeometryColumn('city_data', 'features', 'land_parcels', 'feature', 'POLYGON');
SELECT topology.AddTopoGeometryColumn('city_data', 'features', 'traffic_signs','feature', 'POINT');
SELECT topology.AddTopoGeometryColumn('city_data', 'features', 'city_streets','feature', 'LINE');

--  As a result, Spatial generates a unique TG_LAYER_ID for each layer in
--  the topology metadata (USER/ALL_SDO_TOPO_METADATA).

--NOTYET---- 5. Initialize topology metadata.
--NOTYET--EXECUTE topology.INITIALIZE_METADATA('CITY_DATA');


-- 6. Load feature tables using the CreateTopoGeom constructor.
-- Each topology feature can consist of one or more objects (face, edge, node)
-- of an appropriate type. For example, a land parcel can consist of one face,
-- or two or more faces, as specified in the SDO_TOPO_OBJECT_ARRAY.
-- There are typically fewer features than there are faces, nodes, and edges.
-- In this example, the only features are these:
-- Area features (land parcels): P1, P2, P3, P4, P5
-- Point features (traffic signs): S1, S2, S3, S4
-- Linear features (roads/streets): R1, R2, R3, R4

-- 6A. Load LAND_PARCELS table.
-- P1
INSERT INTO features.land_parcels VALUES ('P1', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{3,3},{6,3}}') -- face_id:3 face_id:6
    );

-- P2
INSERT INTO features.land_parcels VALUES ('P2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from ALL_SDO_TOPO_METADATA)
    '{{4,3},{7,3}}'));
-- P3
INSERT INTO features.land_parcels VALUES ('P3', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{5,3},{8,3}}'));
-- P4
INSERT INTO features.land_parcels VALUES ('P4', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{2,3}}'));
-- P5 (Includes F1, but not F9.)
INSERT INTO features.land_parcels VALUES ('P5', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    1, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{1,3}}'));

-- 6B. Load TRAFFIC_SIGNS table.
-- S1
INSERT INTO features.traffic_signs VALUES ('S1', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    1, -- Topology geometry type (point)
    2, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{14,1}}'));
-- S2
INSERT INTO features.traffic_signs VALUES ('S2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    1, -- Topology geometry type (point)
    2, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{13,1}}'));
-- S3
INSERT INTO features.traffic_signs VALUES ('S3', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    1, -- Topology geometry type (point)
    2, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{6,1}}'));
-- S4
INSERT INTO features.traffic_signs VALUES ('S4', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    1, -- Topology geometry type (point)
    2, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{4,1}}'));

-- 6C. Load CITY_STREETS table.
-- (Note: "R" in feature names is for "Road", because "S" is used for signs.)
-- R1
INSERT INTO features.city_streets VALUES ('R1', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (line string)
    3, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{9,2},{-10,2}}')); -- E9, E10
-- R2
INSERT INTO features.city_streets VALUES ('R2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (line string)
    3, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{4,2},{-5,2}}')); -- E4, E5
-- R3
INSERT INTO features.city_streets VALUES ('R3', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (line string)
    3, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{25,2}}'));
-- R4
INSERT INTO features.city_streets VALUES ('R4', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (line string)
    3, -- TG_LAYER_ID for this topology (from topology.layer)
    '{{3,2}}'));

END;

