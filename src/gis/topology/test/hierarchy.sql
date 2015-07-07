--
-- Define some hierarchical layers
--

--
-- Parcels
--

CREATE TABLE features.big_parcels (
	feature_name varchar primary key
) WITH OIDS;

SELECT topology.AddTopoGeometryColumn('city_data', 'features',
	'big_parcels', 'feature', 'POLYGON',
	1 -- the land_parcles
);

SELECT AddGeometryColumn('features','big_parcels','the_geom',-1,'MULTIPOLYGON',2);

INSERT INTO features.big_parcels VALUES ('P1P2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_parcels'),
    '{{1,1},{2,1}}')); -- P1 and P2

INSERT INTO features.big_parcels VALUES ('P3P4', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_parcels'),
    '{{3,1},{4,1}}')); -- P3 and P4

INSERT INTO features.big_parcels VALUES ('F3F6', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    3, -- Topology geometry type (polygon/multipolygon)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_parcels'),
    (SELECT topoelementarray_agg(ARRAY[id(feature), 1])
     FROM features.land_parcels
     WHERE feature_name in ('F3','F6')) 
  ));

--
-- Streets
--

CREATE TABLE features.big_streets (
	feature_name varchar primary key
) WITH OIDS;

SELECT topology.AddTopoGeometryColumn('city_data', 'features',
	'big_streets', 'feature', 'LINE',
	3 -- the city_streets layer id
);

INSERT INTO features.big_streets VALUES ('R1R2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (lineal)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_streets'),
    (SELECT topoelementarray_agg(ARRAY[id(feature), 3])
     FROM features.city_streets
     WHERE feature_name in ('R1','R2')) -- R1 and R2
  ));

INSERT INTO features.big_streets VALUES ('R4', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    2, -- Topology geometry type (lineal)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_streets'),
    (SELECT topoelementarray_agg(ARRAY[id(feature), 3])
     FROM features.city_streets
     WHERE feature_name in ('R4')) 
  )); 

--
-- Signs
--

CREATE TABLE features.big_signs (
	feature_name varchar primary key
) WITH OIDS;

SELECT topology.AddTopoGeometryColumn('city_data', 'features',
	'big_signs', 'feature', 'POINT',
	2 -- the traffic_signs
);

SELECT AddGeometryColumn('features','big_signs','the_geom',-1,'MULTIPOINT',2);

INSERT INTO features.big_signs VALUES ('S1S2', -- Feature name
  topology.CreateTopoGeom(
    'city_data', -- Topology name
    1, -- Topology geometry type (point/multipoint)
    (SELECT layer_id FROM topology.layer WHERE table_name = 'big_signs'),
    '{{1,2},{2,2}}')); -- S1 and S2

