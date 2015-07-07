-- This script adds a the_geom column to the feature tables
-- created by load_topology.sql and stores there the SFS Geometry
-- derived by the TopoGeometry column

-- Add geometry columns, for caching Geometries from TopoGeometries

SELECT AddGeometryColumn('features','land_parcels','the_geom',-1,'MULTIPOLYGON',2);
SELECT AddGeometryColumn('features','city_streets','the_geom',-1,'MULTILINESTRING',2);
SELECT AddGeometryColumn('features','traffic_signs','the_geom',-1,'MULTIPOINT',2);



--ALTER TABLE features.city_streets ADD the_geom geometry;
UPDATE features.city_streets set the_geom =
  st_multi(topology.Geometry(feature)); 

--ALTER TABLE features.traffic_signs ADD the_geom geometry;
UPDATE features.traffic_signs set the_geom =
  st_multi(topology.Geometry(feature)); 

--ALTER TABLE features.land_parcels ADD the_geom geometry;
UPDATE features.land_parcels set the_geom =
  st_multi(topology.Geometry(feature)); 

